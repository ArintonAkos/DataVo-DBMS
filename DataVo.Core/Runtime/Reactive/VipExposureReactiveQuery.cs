using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Narrow reactive operator for the research benchmark's VIP exposure query:
/// Orders ⋈ Accounts ⋈ Markets, filtered by VIP account, grouped by market category.
/// Emits borrowed deltas via <see cref="IBorrowedReactiveQuery"/>; the legacy owned
/// <see cref="Apply"/> path materializes from the same borrowed build.
/// </summary>
internal sealed class VipExposureReactiveQuery : IBorrowedReactiveQuery
{
    private readonly record struct OrderRow(int Id, int AccountId, int MarketId, decimal Stake);
    private readonly record struct AccountRow(int Id, bool IsVip);
    private readonly record struct MarketRow(int Id, string Category);

    private readonly Dictionary<int, OrderRow> _orders = [];
    private readonly Dictionary<int, AccountRow> _accounts = [];
    private readonly Dictionary<int, MarketRow> _markets = [];
    private readonly Dictionary<string, decimal> _exposureByCategory = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _emittedCategories = new(StringComparer.OrdinalIgnoreCase);

    // Reused per-Apply scratch so the dispatch hot path does not allocate.
    private readonly HashSet<string> _touched = new(StringComparer.OrdinalIgnoreCase);
    private readonly ReactiveRowSchema _outputSchema = new("Category", "TotalExposure");
    private readonly QueryChangeBuilder _legacyBuilder;
    private readonly CellValue[] _rowScratch = new CellValue[2];

    public VipExposureReactiveQuery()
    {
        _legacyBuilder = new QueryChangeBuilder(_outputSchema);
    }

    public IReadOnlyCollection<string> Tables => ["Accounts", "Markets", "Orders"];

    public ReactiveRowSchema OutputSchema => _outputSchema;

    public static bool IsSupported(SelectStatement select)
    {
        return select.FromTable is not null
            && select.FromTable.Name.Equals("Orders", StringComparison.OrdinalIgnoreCase)
            && select.Joins.Count == 2
            && select.GroupByExpression?.Columns.Count == 1
            && select.GroupByExpression.Columns[0].Name.EndsWith("Category", StringComparison.OrdinalIgnoreCase)
            && select.Columns.Count == 2
            && select.Joins.Any(join => join.TableName.Name.Equals("Accounts", StringComparison.OrdinalIgnoreCase))
            && select.Joins.Any(join => join.TableName.Name.Equals("Markets", StringComparison.OrdinalIgnoreCase))
            && select.Columns.Any(column => column.Expression is AggregateExpressionNode aggregate
                && aggregate.FunctionName.Equals("SUM", StringComparison.OrdinalIgnoreCase));
    }

    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((_, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (IsAccounts(table))
            {
                UpsertAccount(ToAccount(row));
            }
            else if (IsMarkets(table))
            {
                UpsertMarket(ToMarket(row));
            }
            else if (IsOrders(table))
            {
                UpsertOrder(ToOrder(row));
            }
        }

        foreach (string category in _exposureByCategory.Keys)
        {
            _emittedCategories.Add(category);
        }
    }

    /// <summary>Legacy owned path: build the borrowed delta, then materialize. Behavior-identical to
    /// the pre-migration <c>Apply</c> (same <see cref="QueryChange"/> shape and values).</summary>
    public QueryChange Apply(IReadOnlyList<RowChange> tableChanges)
    {
        _legacyBuilder.Reset();
        ApplyInto(tableChanges, _legacyBuilder);
        return _legacyBuilder.Build().Materialize();
    }

    public void ApplyInto(IReadOnlyList<RowChange> tableChanges, QueryChangeBuilder builder)
    {
        _touched.Clear();

        for (int i = 0; i < tableChanges.Count; i++)
        {
            RowChange change = tableChanges[i];
            if (IsOrders(change.Table))
            {
                ApplyOrderChange(change, _touched);
            }
            else if (IsAccounts(change.Table))
            {
                ApplyAccountChange(change, _touched);
            }
            else if (IsMarkets(change.Table))
            {
                ApplyMarketChange(change, _touched);
            }
        }

        ClassifyInto(_touched, builder);
    }

    private void ApplyOrderChange(RowChange change, HashSet<string> touched)
    {
        if (change.Before is not null)
        {
            OrderRow before = ToOrder(change.Before);
            if (_orders.Remove(before.Id))
            {
                AdjustExposure(before, -before.Stake, touched);
            }
        }

        if (change.After is not null)
        {
            OrderRow after = ToOrder(change.After);
            _orders[after.Id] = after;
            AdjustExposure(after, after.Stake, touched);
        }
    }

    private void ApplyAccountChange(RowChange change, HashSet<string> touched)
    {
        AccountRow? before = change.Before is null ? null : ToAccount(change.Before);
        AccountRow? after = change.After is null ? null : ToAccount(change.After);

        if (before is not null && _accounts.Remove(before.Value.Id) && before.Value.IsVip)
        {
            foreach (OrderRow order in _orders.Values.Where(order => order.AccountId == before.Value.Id))
            {
                AdjustExposure(order, -order.Stake, touched);
            }
        }

        if (after is not null)
        {
            _accounts[after.Value.Id] = after.Value;
            if (after.Value.IsVip)
            {
                foreach (OrderRow order in _orders.Values.Where(order => order.AccountId == after.Value.Id))
                {
                    AdjustExposure(order, order.Stake, touched);
                }
            }
        }
    }

    private void ApplyMarketChange(RowChange change, HashSet<string> touched)
    {
        MarketRow? before = change.Before is null ? null : ToMarket(change.Before);
        MarketRow? after = change.After is null ? null : ToMarket(change.After);

        if (before is not null && _markets.Remove(before.Value.Id))
        {
            foreach (OrderRow order in _orders.Values.Where(order => order.MarketId == before.Value.Id && IsVip(order.AccountId)))
            {
                AdjustCategory(before.Value.Category, -order.Stake, touched);
            }
        }

        if (after is not null)
        {
            _markets[after.Value.Id] = after.Value;
            foreach (OrderRow order in _orders.Values.Where(order => order.MarketId == after.Value.Id && IsVip(order.AccountId)))
            {
                AdjustCategory(after.Value.Category, order.Stake, touched);
            }
        }
    }

    private void UpsertOrder(OrderRow order)
    {
        _orders[order.Id] = order;
        AdjustExposure(order, order.Stake, touched: null);
    }

    private void UpsertAccount(AccountRow account)
    {
        _accounts[account.Id] = account;
    }

    private void UpsertMarket(MarketRow market)
    {
        _markets[market.Id] = market;
    }

    private void AdjustExposure(OrderRow order, decimal delta, HashSet<string>? touched)
    {
        if (!IsVip(order.AccountId) || !_markets.TryGetValue(order.MarketId, out MarketRow market))
        {
            return;
        }

        AdjustCategory(market.Category, delta, touched);
    }

    private void AdjustCategory(string category, decimal delta, HashSet<string>? touched)
    {
        _exposureByCategory.TryGetValue(category, out decimal current);
        decimal next = current + delta;
        if (next == 0m)
        {
            _exposureByCategory.Remove(category);
        }
        else
        {
            _exposureByCategory[category] = next;
        }

        touched?.Add(category);
    }

    private void ClassifyInto(HashSet<string> touched, QueryChangeBuilder builder)
    {
        foreach (string category in touched)
        {
            if (_exposureByCategory.TryGetValue(category, out decimal total))
            {
                _rowScratch[0] = CellValue.From(category);
                _rowScratch[1] = CellValue.From(total);
                if (_emittedCategories.Add(category))
                {
                    builder.AddAddedRow(_rowScratch);
                }
                else
                {
                    builder.AddUpdatedRow(_rowScratch);
                }
            }
            else if (_emittedCategories.Remove(category))
            {
                _rowScratch[0] = CellValue.From(category);
                _rowScratch[1] = CellValue.Null;
                builder.AddRemovedRow(_rowScratch);
            }
        }
    }

    private bool IsVip(int accountId) =>
        _accounts.TryGetValue(accountId, out AccountRow account) && account.IsVip;

    private static OrderRow ToOrder(IReadOnlyDictionary<string, object?> row) =>
        new(ToInt(row["Id"]), ToInt(row["AccountId"]), ToInt(row["MarketId"]), ToDecimal(row["Stake"]));

    private static AccountRow ToAccount(IReadOnlyDictionary<string, object?> row) =>
        new(ToInt(row["Id"]), ToBool(row["IsVip"]));

    private static MarketRow ToMarket(IReadOnlyDictionary<string, object?> row) =>
        new(ToInt(row["Id"]), Convert.ToString(row["Category"], System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty);

    private static int ToInt(object? value) => Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture);

    private static decimal ToDecimal(object? value) => Convert.ToDecimal(value, System.Globalization.CultureInfo.InvariantCulture);

    private static bool ToBool(object? value) => value switch
    {
        bool b => b,
        int i => i != 0,
        long l => l != 0,
        string s => bool.TryParse(s, out bool b) ? b : s == "1",
        _ => Convert.ToBoolean(value, System.Globalization.CultureInfo.InvariantCulture)
    };

    private static bool IsAccounts(string table) => table.Equals("Accounts", StringComparison.OrdinalIgnoreCase);

    private static bool IsMarkets(string table) => table.Equals("Markets", StringComparison.OrdinalIgnoreCase);

    private static bool IsOrders(string table) => table.Equals("Orders", StringComparison.OrdinalIgnoreCase);
}
