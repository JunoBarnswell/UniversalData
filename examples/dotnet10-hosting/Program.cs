using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using UniversalData;
using UniversalData.Dotnet10Hosting;

if (GenericHostCommand.IsRequested(args))
    return await GenericHostCommand.RunAsync(args);

var builder = WebApplication.CreateBuilder(args);
if (Environment.GetEnvironmentVariable("UD_UNIVERSALDATA_CONFIG") is { Length: > 0 } configuredPath)
    builder.Configuration["UniversalData:ConfigPath"] = configuredPath;

builder.Services.AddUniversalData(
    builder.Configuration.GetRequiredSection("UniversalData"),
    builder.Environment.ContentRootPath);
builder.Services.AddSingleton<OrdersService>();

var app = builder.Build();

app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));

app.MapGet("/orders", async (int? shopId, int? offset, int? pageSize, string? source,
    OrdersService orders, CancellationToken cancellationToken) =>
{
    try
    {
        var rows = await orders.ListAsync(new OrderFilter
        {
            ShopId = shopId ?? 1,
            Offset = offset ?? 0,
            PageSize = pageSize ?? 20
        }, source, cancellationToken);
        return Results.Ok(rows);
    }
    catch (ArgumentException error)
    {
        return Results.BadRequest(error.Message);
    }
});

app.MapPost("/orders", async (OrderWriteRequest request, string? source,
    OrdersService orders, CancellationToken cancellationToken) =>
{
    try
    {
        return Results.Ok(await orders.InsertAndReadAsync(request, source, cancellationToken));
    }
    catch (ArgumentException error)
    {
        return Results.BadRequest(error.Message);
    }
    catch (UniversalDataException error)
    {
        return Results.Problem(error.Message, statusCode: StatusCodes.Status502BadGateway);
    }
});

await app.RunAsync();
return 0;
