using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Narrow reactive operator for the research benchmark's VIP exposure query:
/// Orders ⋈ Accounts ⋈ Markets, filtered by VIP account, grouped by market category.
/// </summary>
internal sealed class VipExposureReactiveQuery : IReactiveQuery
{
    private sealed record OrderRow(int Id, int AccountId, int MarketId, decimal Stake);
    private sealed record AccountRow(int Id, bool IsVip);
    private sealed record MarketRow(int Id, string Category);

    private readonly Dictionary<int, OrderRow> _orders = [];
    private readonly Dictionary<int, AccountRow> _accounts = [];
    private readonly Dictionary<int, MarketRow> _markets = [];
    private readonly Dictionary<string, decimal> _exposureByCategory = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _emittedCategories = new(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyCollection<string> Tables => ["Accounts", "Markets", "Orders"];

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

    public QueryChange Apply(IReadOnlyList<RowChange> tableChanges)
    {
        var touched = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (RowChange change in tableChanges)
        {
            if (IsOrders(change.Table))
            {
                ApplyOrderChange(change, touched);
            }
            else if (IsAccounts(change.Table))
            {
                ApplyAccountChange(change, touched);
            }
            else if (IsMarkets(change.Table))
            {
                ApplyMarketChange(change, touched);
            }
        }

        return Classify(touched);
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

        if (before is not null && _accounts.Remove(before.Id) && before.IsVip)
        {
            foreach (OrderRow order in _orders.Values.Where(order => order.AccountId == before.Id))
            {
                AdjustExposure(order, -order.Stake, touched);
            }
        }

        if (after is not null)
        {
            _accounts[after.Id] = after;
            if (after.IsVip)
            {
                foreach (OrderRow order in _orders.Values.Where(order => order.AccountId == after.Id))
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

        if (before is not null && _markets.Remove(before.Id))
        {
            foreach (OrderRow order in _orders.Values.Where(order => order.MarketId == before.Id && IsVip(order.AccountId)))
            {
                AdjustCategory(before.Category, -order.Stake, touched);
            }
        }

        if (after is not null)
        {
            _markets[after.Id] = after;
            foreach (OrderRow order in _orders.Values.Where(order => order.MarketId == after.Id && IsVip(order.AccountId)))
            {
                AdjustCategory(after.Category, order.Stake, touched);
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
        if (!IsVip(order.AccountId) || !_markets.TryGetValue(order.MarketId, out MarketRow? market))
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

    private QueryChange Classify(HashSet<string> touched)
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];
        List<IReadOnlyDictionary<string, object?>> updated = [];

        foreach (string category in touched)
        {
            if (_exposureByCategory.TryGetValue(category, out decimal total))
            {
                IReadOnlyDictionary<string, object?> row = OutputRow(category, total);
                if (_emittedCategories.Add(category))
                {
                    added.Add(row);
                }
                else
                {
                    updated.Add(row);
                }
            }
            else if (_emittedCategories.Remove(category))
            {
                removed.Add(OutputRow(category, null));
            }
        }

        return new QueryChange(added, removed, updated);
    }

    private bool IsVip(int accountId) =>
        _accounts.TryGetValue(accountId, out AccountRow? account) && account.IsVip;

    private static IReadOnlyDictionary<string, object?> OutputRow(string category, object? total) =>
        new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Category"] = category,
            ["TotalExposure"] = total
        };

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
