using Microsoft.Extensions.Configuration;
using UniversalData;

namespace UniversalData.Dotnet10Hosting;

/// <summary>
/// The single application service used by both the ASP.NET Core endpoints and the Generic Host
/// command. It deliberately keeps source selection at the call boundary so the same query can
/// observe the primary or either streaming standby.
/// </summary>
public sealed class OrdersService
{
    private readonly IDataClient data;
    private readonly string defaultSource;
    private readonly IReadOnlyDictionary<string, DataJobQuery<OrderFilter, OrderListItem>> pages;

    public OrdersService(IDataClient data, IConfiguration configuration)
    {
        this.data = data;
        defaultSource = configuration["Orders:DefaultSource"] ?? "orders-primary";
        pages = data.Sources.List().ToDictionary(source => source.Name,
            source => DefinePage(source.Name), StringComparer.Ordinal);
    }

    public async Task<IReadOnlyList<OrderListItem>> ListAsync(OrderFilter filter,
        string? source = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ValidateFilter(filter);
        var query = pages[Source(source)];
        return await data.RunAsync(query, filter, cancellationToken).ToListAsync(cancellationToken);
    }

    private static DataJobQuery<OrderFilter, OrderListItem> DefinePage(string source) =>
        DataJob.Define<OrderFilter>("orders.list").QueryTable<OrderRecord>(source)
            .Where((row, input) => row.ShopId == input.ShopId)
            .OrderByDescending(row => row.Sequence)
            .ThenBy(row => row.Id)
            .Skip(input => input.Offset)
            .Take(input => input.PageSize)
            .Select(row => new OrderListItem
            {
                Id = row.Id,
                ShopId = row.ShopId,
                Sequence = row.Sequence,
                TotalCents = row.TotalCents
            })
            .Return();

    public async Task<OrderListItem> InsertAndReadAsync(OrderWriteRequest request,
        string? source = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateWrite(request);
        string namedSource = Source(source);
        var insert = DataJob.Define<OrderWriteRequest>("orders.insert");
        var parameters = insert.BindParameters(row => row.Id, row => row.ShopId, row => row.Sequence, row => row.TotalCents);
        insert.ExecuteSql(namedSource, "INSERT INTO orders (id, shop_id, sequence, total_cents) VALUES ($1, $2, $3, $4)",
            parameters);
        await data.RunAsync(insert, request, cancellationToken);

        // A successful write is not reported until the same named source can read the business
        // key back. The primary source is therefore read-after-write in the normal topology.
        return await ReadOneAsync(request.Id, namedSource, cancellationToken);
    }

    private async Task<OrderListItem> ReadOneAsync(int id, string source,
        CancellationToken cancellationToken)
    {
        var query = DataJob.Define<OrderLookup>("orders.read-one")
            .QueryTable<OrderRecord>(source)
            .Where((row, input) => row.Id == input.Id)
            .OrderBy(row => row.Id)
            .Take(input => input.Limit)
            .Select(row => new OrderListItem
            {
                Id = row.Id,
                ShopId = row.ShopId,
                Sequence = row.Sequence,
                TotalCents = row.TotalCents
            })
            .Return();
        var rows = await data.RunAsync(query, new OrderLookup { Id = id }, cancellationToken)
            .ToListAsync(cancellationToken);
        return rows.Count == 1
            ? rows[0]
            : throw new InvalidOperationException($"Order {id} was not visible after the write.");
    }

    private string Source(string? requested)
    {
        string value = string.IsNullOrWhiteSpace(requested) ? defaultSource : requested.Trim();
        if (!pages.ContainsKey(value))
            throw new ArgumentException("The source must be one of the configured order databases.", nameof(requested));
        return value;
    }

    private static void ValidateFilter(OrderFilter filter)
    {
        if (filter.ShopId <= 0) throw new ArgumentOutOfRangeException(nameof(filter.ShopId));
        if (filter.Offset < 0) throw new ArgumentOutOfRangeException(nameof(filter.Offset));
        if (filter.PageSize is < 0 or > 65536) throw new ArgumentOutOfRangeException(nameof(filter.PageSize));
    }

    private static void ValidateWrite(OrderWriteRequest request)
    {
        if (request.Id <= 0) throw new ArgumentOutOfRangeException(nameof(request.Id));
        if (request.ShopId <= 0) throw new ArgumentOutOfRangeException(nameof(request.ShopId));
        if (request.Sequence <= 0) throw new ArgumentOutOfRangeException(nameof(request.Sequence));
        if (request.Total < 0 || request.Total > 9999999999.99m)
            throw new ArgumentOutOfRangeException(nameof(request.Total));
        if (decimal.Round(request.Total, 2) != request.Total)
            throw new ArgumentException("Total must have at most two decimal places.", nameof(request.Total));
    }
}
