using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.RateLimiting;
using UniversalData.Hosting;

namespace UniversalData.Management;

public static class ManagementSecurity
{
    public const string Scheme = "ud-session";
    internal static void Configure(WebApplicationBuilder builder,HostPolicy policy,IdentityDirectory identities,
        ClusterHostState? cluster)
    {
        var protection = builder.Services.AddDataProtection().SetApplicationName("UniversalData/" + ProductInfo.Protocol);
        if (cluster is null)
        {
            string keys = Path.Combine(policy.DataDirectory,"authentication","keys"); HostFiles.NoLinks(keys); Directory.CreateDirectory(keys);
            if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(keys,UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            protection.PersistKeysToFileSystem(new DirectoryInfo(keys));
        }
        else
            builder.Services.Configure<KeyManagementOptions>(options => options.XmlRepository = cluster);
        builder.Services.AddAuthentication(Scheme).AddCookie(Scheme,options => {
            options.Cookie.Name = "ud-session-v" + ProductInfo.Protocol; options.Cookie.HttpOnly = true;
            options.Cookie.SameSite = SameSiteMode.Strict; options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
            options.ExpireTimeSpan = TimeSpan.FromHours(1); options.SlidingExpiration = false;
            options.Events = new CookieAuthenticationEvents {
                OnRedirectToLogin = context => { context.Response.StatusCode = 401; return Task.CompletedTask; },
                OnRedirectToAccessDenied = context => { context.Response.StatusCode = 403; return Task.CompletedTask; },
                OnValidatePrincipal = async context => {
                    var subject = context.Principal?.FindFirstValue(ClaimTypes.NameIdentifier); var stamp = context.Principal?.FindFirstValue("stamp");
                    var identity = subject is null || stamp is null ? null : identities.Find(subject,stamp);
                    bool valid = identity is not null && identity.Tenant == context.Principal!.FindFirstValue("tenant");
                    if (valid) {
                        using var catalog = policy.Bind(HostIdentity.Operator(identity!.Tenant)).OpenCatalog();
                        var id = context.Principal!.FindFirstValue("session") ?? ""; var revision = catalog.LatestRevision(CatalogKind.Audit,"session/" + id);
                        using var session = JsonDocument.Parse(revision == 0 ? "{}" : catalog.Get(CatalogKind.Audit,"session/" + id,revision)!);
                        valid = session.RootElement.TryGetProperty("active",out var active) && active.GetBoolean() &&
                            session.RootElement.GetProperty("subject").GetString() == identity.Subject && session.RootElement.GetProperty("expiresMs").GetInt64() > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    }
                    if (!valid) { context.RejectPrincipal(); await context.HttpContext.SignOutAsync(Scheme); }
                }
            };
        });
        builder.Services.AddAuthorization();
        builder.Services.AddRateLimiter(options => options.AddFixedWindowLimiter("login",limit => {
            limit.PermitLimit = 10; limit.Window = TimeSpan.FromMinutes(1); limit.QueueLimit = 0;
        }));
    }
    public static HostIdentity Identity(this HttpContext context,IdentityDirectory identities)
    {
        var identity = identities.Find(context.User.FindFirstValue(ClaimTypes.NameIdentifier) ?? "",context.User.FindFirstValue("stamp"));
        return identity ?? throw new UniversalDataException((int)StatusCode.Auth,"Session is no longer valid");
    }
    public static void Install(WebApplication app,HostPolicy policy,IdentityDirectory identities)
    {
        app.UseAuthentication(); app.UseAuthorization(); app.UseRateLimiter();
        app.Use(async (context,next) => {
            bool authenticated = context.User.Identity?.IsAuthenticated == true;
            var actor = authenticated ? context.Identity(identities) : null;
            bool mutation = HttpMethods.IsPost(context.Request.Method) || HttpMethods.IsPut(context.Request.Method) || HttpMethods.IsPatch(context.Request.Method) || HttpMethods.IsDelete(context.Request.Method);
            if (context.Request.Path.StartsWithSegments("/api") && authenticated && mutation) {
                var expected = context.User.FindFirstValue("csrf") ?? ""; var actual = context.Request.Headers["X-UD-CSRF"].ToString();
                if (expected.Length == 0 || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(expected),Encoding.UTF8.GetBytes(actual))) {
                    context.Response.StatusCode = 403; await context.Response.WriteAsJsonAsync(new { error = "Invalid request verification token" }); return;
                }
            }
            try { await next(); }
            finally {
                if (actor is { } identity && context.Request.Path.StartsWithSegments("/api") && mutation) {
                    using var catalog = policy.Bind(HostIdentity.Operator(identity.Tenant)).OpenCatalog();
                    string id = "operation/" + Guid.NewGuid().ToString("N");
                    catalog.Put(CatalogKind.Audit,id,1,JsonSerializer.Serialize(new { version = ProductInfo.Protocol,id,subject = identity.Subject,tenant = identity.Tenant,
                        method = context.Request.Method,route = (context.GetEndpoint() as RouteEndpoint)?.RoutePattern.RawText,status = context.Response.StatusCode,
                        timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },HostPolicy.JsonOptions));
                }
            }
        });
        app.MapPost("/api/auth/login",async (LoginRequest request,HttpContext context) => {
            var identity = identities.Authenticate(request.Subject,request.Password);
            if (identity is null) return Results.Unauthorized();
            _ = policy.Bind(identity);
            string session = Guid.NewGuid().ToString("N"),csrf = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            long expires = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds();
            using(var catalog = policy.Bind(HostIdentity.Operator(identity.Tenant)).OpenCatalog()) {
                catalog.Put(CatalogKind.Audit,"session/" + session,1,JsonSerializer.Serialize(new { version = ProductInfo.Protocol,id = "session/" + session,
                    subject = identity.Subject,active = true,expiresMs = expires,timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },HostPolicy.JsonOptions));
            }
            var claims = new[] { new Claim(ClaimTypes.NameIdentifier,identity.Subject),new Claim("tenant",identity.Tenant),new Claim("stamp",identity.Stamp),new Claim("session",session),new Claim("csrf",csrf) };
            await context.SignInAsync(Scheme,new ClaimsPrincipal(new ClaimsIdentity(claims,Scheme)));
            return Results.Ok(new { subject = identity.Subject,tenant = identity.Tenant,grants = identity.Grants,csrf });
        }).AllowAnonymous().RequireRateLimiting("login");
        app.MapGet("/api/auth/session",(HttpContext context) => {
            var identity = context.Identity(identities);
            return Results.Ok(new { subject = identity.Subject,tenant = identity.Tenant,grants = identity.Grants,csrf = context.User.FindFirstValue("csrf") });
        }).RequireAuthorization();
        app.MapPost("/api/auth/logout",async (HttpContext context) => {
            var identity = context.Identity(identities); var session = context.User.FindFirstValue("session")!;
            using var catalog = policy.Bind(HostIdentity.Operator(identity.Tenant)).OpenCatalog();
            catalog.Put(CatalogKind.Audit,"session/" + session,checked(catalog.LatestRevision(CatalogKind.Audit,"session/" + session) + 1),
                JsonSerializer.Serialize(new { version = ProductInfo.Protocol,id = "session/" + session,subject = identity.Subject,active = false,expiresMs = 0,
                    timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },HostPolicy.JsonOptions));
            await context.SignOutAsync(Scheme); return Results.NoContent();
        }).RequireAuthorization();
        app.MapPost("/api/auth/password",async (PasswordRequest request,HttpContext context) => {
            identities.ChangePassword(context.Identity(identities),request.CurrentPassword,request.Password);
            await context.SignOutAsync(Scheme); return Results.NoContent();
        }).RequireAuthorization();
    }
    private sealed record LoginRequest(string Subject,string Password);
    private sealed record PasswordRequest(string CurrentPassword,string Password);
}
