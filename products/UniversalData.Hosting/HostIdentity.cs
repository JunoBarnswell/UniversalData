using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace UniversalData.Hosting;

public sealed record ResourceGrant(string Permission, string Resource);
public sealed record HostIdentity(string Subject, string Tenant, string Stamp, ResourceGrant[] Grants)
{
    public bool Allows(string permission, string resource) => Grants.Any(grant =>
        (grant.Permission == "*" || grant.Permission == permission) &&
        (grant.Resource == "*" || grant.Resource == resource ||
         grant.Resource.EndsWith("/*", StringComparison.Ordinal) && resource.StartsWith(grant.Resource[..^1], StringComparison.Ordinal)));
    public void Require(string permission, string resource)
    {
        if (!Allows(permission, resource)) throw new UniversalDataException((int)StatusCode.Auth, "Operation is outside the authenticated scope");
    }
    public static bool ValidId(string value) => value.Length is > 0 and <= 128 && Regex.IsMatch(value, "\\A[A-Za-z0-9_.-]+\\z") && value is not ("." or "..");
    public static HostIdentity Operator(string tenant) => new("host-operator", tenant, "", [new("*", "*")]);
}
public sealed record IdentityUser(string Id, string Tenant, string PasswordHash, string Stamp, string[] Roles, bool Disabled = false);
public sealed record IdentityDocument(int Version, IdentityUser[] Users, Dictionary<string, ResourceGrant[]> Roles);

/// <summary>Reloaded host-owned identities. Password changes invalidate every existing session and queued authority.</summary>
public sealed class IdentityDirectory(string path)
{
    private readonly object gate = new();
    private IdentityDocument Read()
    {
        HostFiles.NoLinks(path);
        var document = JsonSerializer.Deserialize<IdentityDocument>(File.ReadAllText(path), HostPolicy.JsonOptions)
            ?? throw new InvalidDataException("Identity directory is empty");
        if (document.Version != 1 || document.Users.Any(user => !HostIdentity.ValidId(user.Id) || !HostIdentity.ValidId(user.Tenant) || user.Stamp.Length < 32) ||
            document.Users.Select(user => user.Id).Distinct(StringComparer.Ordinal).Count() != document.Users.Length)
            throw new InvalidDataException("Identity directory is invalid");
        return document;
    }
    public HostIdentity? Find(string subject, string? stamp = null)
    {
        lock (gate) {
            var document = Read(); var user = document.Users.SingleOrDefault(value => value.Id == subject && !value.Disabled);
            if (user is null || stamp is not null && user.Stamp != stamp) return null;
            return new(user.Id, user.Tenant, user.Stamp, user.Roles.SelectMany(role => document.Roles.GetValueOrDefault(role) ?? []).Distinct().ToArray());
        }
    }
    public HostIdentity? Authenticate(string subject, string password)
    {
        lock (gate) {
            var document = Read(); var user = document.Users.SingleOrDefault(value => value.Id == subject && !value.Disabled);
            // Equal-cost verification also for an unknown user; no credential or compiler text is logged.
            bool valid = Verify(password, user?.PasswordHash ?? DummyHash);
            return valid && user is not null ? Find(user.Id, user.Stamp) : null;
        }
    }
    private static readonly string DummyHash = HashPassword(Convert.ToHexString(RandomNumberGenerator.GetBytes(32)));
    public static string HashPassword(string password)
    {
        if (password.Length is < 12 or > 1024) throw new ArgumentException("Password must contain 12..1024 characters");
        var salt = RandomNumberGenerator.GetBytes(32);
        var hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, 600000, HashAlgorithmName.SHA256, 32);
        return "pbkdf2-sha256$600000$" + Convert.ToBase64String(salt) + "$" + Convert.ToBase64String(hash);
    }
    private static bool Verify(string password, string encoded)
    {
        try {
            var parts = encoded.Split('$');
            if (password.Length > 1024 || parts.Length != 4 || parts[0] != "pbkdf2-sha256" || !int.TryParse(parts[1], out int iterations) || iterations is < 600000 or > 2000000) return false;
            var salt = Convert.FromBase64String(parts[2]); var expected = Convert.FromBase64String(parts[3]);
            if (salt.Length != 32 || expected.Length != 32) return false;
            var actual = Rfc2898DeriveBytes.Pbkdf2(password, salt, iterations, HashAlgorithmName.SHA256, 32);
            try { return CryptographicOperations.FixedTimeEquals(actual, expected); }
            finally { CryptographicOperations.ZeroMemory(actual); }
        } catch (FormatException) { return false; }
    }
    public void ChangePassword(HostIdentity identity, string currentPassword, string password)
    {
        lock (gate) {
            if (Authenticate(identity.Subject, currentPassword)?.Stamp != identity.Stamp)
                throw new UniversalDataException((int)StatusCode.Auth, "Current credential is invalid");
            var document = Read(); var hash = HashPassword(password);
            document = document with { Users = document.Users.Select(user => user.Id == identity.Subject
                ? user with { PasswordHash = hash, Stamp = Convert.ToHexString(RandomNumberGenerator.GetBytes(32)) } : user).ToArray() };
            HostFiles.WritePrivate(path, JsonSerializer.SerializeToUtf8Bytes(document, HostPolicy.JsonOptions), overwrite: true);
        }
    }
}

public static class HostFiles
{
    public static void NoLinks(string path)
    {
        if (!Path.IsPathFullyQualified(path)) throw new ArgumentException("Host paths must be absolute");
        for (FileSystemInfo? part = new FileInfo(Path.GetFullPath(path)); part is not null; part = part is FileInfo file ? file.Directory : ((DirectoryInfo)part).Parent)
            if (part.LinkTarget is not null) throw new UniversalDataException((int)StatusCode.Auth, "Host path contains a symbolic link");
    }
    public static void WritePrivate(string path, ReadOnlySpan<byte> bytes, bool overwrite = false)
    {
        NoLinks(path); var directory = Path.GetDirectoryName(path)!;
        // A private file does not give us ownership of an existing host directory.
        // In particular, identity initialization must never chmod /etc or $HOME.
        if (!Directory.Exists(directory)) {
            if (OperatingSystem.IsWindows()) Directory.CreateDirectory(directory);
            else Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
        string temporary = Path.Combine(directory, ".write-" + Guid.NewGuid().ToString("N"));
        try {
            var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None };
            if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
            using (var file = new FileStream(temporary, options)) { file.Write(bytes); file.Flush(true); }
            File.Move(temporary, path, overwrite);
        } finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }
}
