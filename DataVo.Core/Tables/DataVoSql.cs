using System.Globalization;
using System.Text;
using DataVo.Core.Contracts.Results;

namespace DataVo.Core;

/// <summary>
/// Writes the statements compiled table queries run. Every value is a parameter: a filter's values are
/// <c>@p0</c>, <c>@p1</c>..., an update's are <c>@u0</c>, <c>@u1</c>..., and paging uses <c>@limit</c> and
/// <c>@offset</c>, so the parts of one statement never share a name.
/// </summary>
internal static class DataVoSql
{
    /// <summary>The parameter that holds a row limit.</summary>
    public const string LimitParameter = "limit";

    /// <summary>The parameter that holds the rows to skip.</summary>
    public const string OffsetParameter = "offset";

    /// <summary>The parameter that holds a primary-key value, for statements that change one row.</summary>
    public const string KeyParameter = "key";

    private const string RowsAffected = "Rows affected:";

    /// <summary>Writes a SELECT over one table.</summary>
    /// <param name="columns">The SELECT list.</param>
    /// <param name="table">The table.</param>
    /// <param name="where">The condition, or <see langword="null"/> for every row.</param>
    /// <param name="orderBy">The ORDER BY list, or <see langword="null"/> for storage order.</param>
    /// <param name="limit">Whether to add <c>LIMIT @limit</c>.</param>
    /// <param name="offset">Whether to add <c>OFFSET @offset</c>.</param>
    /// <returns>The statement.</returns>
    /// <example><c>SELECT Name, Level FROM players WHERE Level &gt;= @p0 ORDER BY Level DESC LIMIT @limit</c></example>
    public static string Select(string columns, string table, string? where, string? orderBy, bool limit, bool offset)
    {
        var sql = new StringBuilder("SELECT ").Append(columns).Append(" FROM ").Append(table);
        AppendWhere(sql, where);
        if (orderBy is not null)
        {
            sql.Append(" ORDER BY ").Append(orderBy);
        }

        if (limit)
        {
            sql.Append(" LIMIT @").Append(LimitParameter);
        }

        if (offset)
        {
            sql.Append(" OFFSET @").Append(OffsetParameter);
        }

        return sql.ToString();
    }

    /// <summary>Writes a DELETE over one table.</summary>
    /// <param name="table">The table.</param>
    /// <param name="where">The condition, or <see langword="null"/> to delete every row.</param>
    /// <returns>The statement.</returns>
    public static string Delete(string table, string? where) => AppendWhere(new StringBuilder("DELETE FROM ").Append(table), where).ToString();

    /// <summary>Writes an UPDATE over one table.</summary>
    /// <param name="table">The table.</param>
    /// <param name="assignments">The SET list.</param>
    /// <param name="where">The condition, or <see langword="null"/> to update every row.</param>
    /// <returns>The statement.</returns>
    public static string Update(string table, string assignments, string? where) =>
        AppendWhere(new StringBuilder("UPDATE ").Append(table).Append(" SET ").Append(assignments), where).ToString();

    /// <summary>Writes the condition that pins one row by primary key.</summary>
    /// <param name="keyColumn">The primary-key column.</param>
    /// <returns>The condition over <c>@key</c>.</returns>
    public static string KeyEquals(string keyColumn) => $"{keyColumn} = @{KeyParameter}";

    /// <summary>Adds the paging values a statement written by <see cref="Select"/> expects.</summary>
    /// <param name="parameters">The condition's values.</param>
    /// <param name="skip">The rows to skip; 0 adds no offset.</param>
    /// <param name="limit">The row limit; -1 adds none.</param>
    /// <returns>All of the statement's values.</returns>
    public static DataVoQueryParameter[] Paged(DataVoQueryParameter[] parameters, long skip, long limit)
    {
        if (skip <= 0 && limit < 0)
        {
            return parameters;
        }

        var paged = new List<DataVoQueryParameter>(parameters.Length + 2);
        paged.AddRange(parameters);
        if (limit >= 0)
        {
            paged.Add(new DataVoQueryParameter(LimitParameter, Math.Min(limit, int.MaxValue)));
        }

        if (skip > 0)
        {
            paged.Add(new DataVoQueryParameter(OffsetParameter, skip));
        }

        return [.. paged];
    }

    /// <summary>Joins two sets of values into one array.</summary>
    /// <param name="first">The first values.</param>
    /// <param name="second">The values that follow.</param>
    /// <returns>The values of both, in order.</returns>
    public static DataVoQueryParameter[] Concat(DataVoQueryParameter[] first, DataVoQueryParameter[] second)
    {
        if (first.Length == 0)
        {
            return second;
        }

        var all = new DataVoQueryParameter[first.Length + second.Length];
        first.CopyTo(all, 0);
        second.CopyTo(all, first.Length);
        return all;
    }

    /// <summary>Reads the number of rows a DELETE or UPDATE changed from its <c>Rows affected: N</c> message.</summary>
    /// <param name="result">The statement's result.</param>
    /// <returns>The number of rows changed; 0 when the result reports none.</returns>
    public static int AffectedRows(QueryResult result)
    {
        int affected = 0;
        foreach (string message in result.Messages)
        {
            if (message.StartsWith(RowsAffected, StringComparison.OrdinalIgnoreCase)
                && int.TryParse(message.Substring(RowsAffected.Length).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int count))
            {
                affected += count;
            }
        }

        return affected;
    }

    private static StringBuilder AppendWhere(StringBuilder sql, string? where) =>
        string.IsNullOrEmpty(where) ? sql : sql.Append(" WHERE ").Append(where);
}
