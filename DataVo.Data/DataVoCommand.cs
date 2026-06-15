using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser;

namespace DataVo.Data;

/// <summary>
/// Represents a SQL command to be executed against a DataVo database.
/// <para>
/// Supports parameterized queries via <see cref="Parameters"/> and executes
/// through the DataVo <see cref="QueryEngine"/> pipeline.
/// </para>
/// </summary>
/// <example>
/// <code>
/// using var cmd = connection.CreateCommand();
/// cmd.CommandText = "SELECT * FROM Users WHERE Id = @id;";
/// cmd.Parameters.AddWithValue("@id", 42);
/// using var reader = cmd.ExecuteReader();
/// </code>
/// </example>
public class DataVoCommand : DbCommand
{
    private DataVoConnection? _connection;
    private DataVoTransaction? _transaction;
    private readonly DataVoParameterCollection _parameters = new();

    /// <inheritdoc />
    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    /// <inheritdoc />
    public override int CommandTimeout { get; set; } = 30;

    /// <inheritdoc />
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <inheritdoc />
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc />
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <inheritdoc />
    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (DataVoConnection?)value;
    }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <summary>
    /// Gets the strongly-typed parameter collection for this command.
    /// </summary>
    public new DataVoParameterCollection Parameters => _parameters;

    /// <inheritdoc />
    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set => _transaction = (DataVoTransaction?)value;
    }

    /// <summary>
    /// Executes the command and returns a <see cref="DataVoDataReader"/> for reading the result set.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var results = ExecuteEngine();
        return new DataVoDataReader(results);
    }

    /// <summary>
    /// Executes a non-query command (INSERT, UPDATE, DELETE, DDL) and returns
    /// the number of rows affected, parsed from the engine's response messages.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        var results = ExecuteEngine();
        int totalAffected = 0;

        foreach (var result in results)
        {
            foreach (string msg in result.Messages)
            {
                var match = Regex.Match(msg, @"Rows affected:\s*(\d+)");
                if (match.Success)
                    totalAffected += int.Parse(match.Groups[1].Value);
            }
        }

        return totalAffected;
    }

    /// <summary>
    /// Executes the query and returns the first column of the first row in the result set.
    /// Returns <c>null</c> if the result set is empty.
    /// </summary>
    public override object? ExecuteScalar()
    {
        var results = ExecuteEngine();
        var firstWithData = results.FirstOrDefault(r => r.Data.Count > 0);
        if (firstWithData == null || firstWithData.Fields.Count == 0) return null;

        string firstField = firstWithData.Fields[0];
        return firstWithData.Data[0].GetValueOrDefault(firstField);
    }

    /// <inheritdoc />
    public override void Cancel() { }

    /// <inheritdoc />
    public override void Prepare() { }

    /// <inheritdoc />
    protected override DbParameter CreateDbParameter() => new DataVoParameter();

    /// <summary>
    /// Substitutes named parameters into the SQL text and runs it through the QueryEngine.
    /// </summary>
    private List<QueryResult> ExecuteEngine()
    {
        if (_connection == null)
            throw new InvalidOperationException("Connection is not set.");

        string sql = SubstituteParameters(CommandText);

        var engine = new QueryEngine(sql, _connection.Session, _connection.Engine);
        var results = engine.Parse();

        foreach (var result in results)
        {
            if (result.IsError)
                throw new DataVoException(string.Join("; ", result.Messages));
        }

        return results;
    }

    /// <summary>
    /// Replaces <c>@paramName</c> placeholders with their literal values.
    /// Strings are single-quoted, nulls become <c>NULL</c>, numbers remain unquoted.
    /// </summary>
    private string SubstituteParameters(string sql)
    {
        var parameters = _parameters.AllParameters
            .Where(p => !string.IsNullOrEmpty(p.ParameterName))
            .OrderByDescending(p => p.ParameterName.Length)
            .ToArray();

        if (parameters.Length == 0)
        {
            return sql;
        }

        var builder = new StringBuilder(sql.Length);
        char? activeQuote = null;
        bool inLineComment = false;
        bool inBlockComment = false;

        for (int i = 0; i < sql.Length;)
        {
            char current = sql[i];
            char next = i + 1 < sql.Length ? sql[i + 1] : '\0';

            if (inLineComment)
            {
                builder.Append(current);
                if (current is '\r' or '\n')
                {
                    inLineComment = false;
                }

                i++;
                continue;
            }

            if (inBlockComment)
            {
                builder.Append(current);
                if (current == '*' && next == '/')
                {
                    builder.Append(next);
                    inBlockComment = false;
                    i += 2;
                    continue;
                }

                i++;
                continue;
            }

            if (activeQuote == null && current == '-' && next == '-')
            {
                builder.Append(current);
                builder.Append(next);
                inLineComment = true;
                i += 2;
                continue;
            }

            if (activeQuote == null && current == '/' && next == '*')
            {
                builder.Append(current);
                builder.Append(next);
                inBlockComment = true;
                i += 2;
                continue;
            }

            if (current is '\'' or '"')
            {
                builder.Append(current);

                if (activeQuote == '\'' && current == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    builder.Append(sql[i + 1]);
                    i += 2;
                    continue;
                }

                if (activeQuote == current)
                {
                    activeQuote = null;
                }
                else if (activeQuote == null)
                {
                    activeQuote = current;
                }

                i++;
                continue;
            }

            if (activeQuote == null)
            {
                DataVoParameter? matchedParameter = parameters.FirstOrDefault(p => IsParameterMatch(sql, i, p.ParameterName));
                if (matchedParameter != null)
                {
                    builder.Append(FormatLiteral(matchedParameter.Value));
                    i += matchedParameter.ParameterName.Length;
                    continue;
                }
            }

            builder.Append(current);
            i++;
        }

        return builder.ToString();
    }

    private static bool IsParameterMatch(string sql, int index, string parameterName)
    {
        if (!sql.AsSpan(index).StartsWith(parameterName, StringComparison.Ordinal))
        {
            return false;
        }

        int nextIndex = index + parameterName.Length;
        if (nextIndex >= sql.Length)
        {
            return true;
        }

        return !IsParameterNameCharacter(sql[nextIndex]);
    }

    private static bool IsParameterNameCharacter(char value)
    {
        return char.IsLetterOrDigit(value) || value == '_';
    }

    /// <summary>
    /// Formats a CLR value as a SQL literal (e.g. <c>'hello'</c>, <c>42</c>, <c>NULL</c>).
    /// </summary>
    private static string FormatLiteral(object? value)
    {
        if (value == null || value == DBNull.Value) return "NULL";

        return value switch
        {
            float[] values => FormatVectorLiteral(values),
            double[] values => FormatVectorLiteral(values),
            decimal[] values => FormatVectorLiteral(values),
            IEnumerable<float> values => FormatVectorLiteral(values),
            IEnumerable<double> values => FormatVectorLiteral(values),
            IEnumerable<decimal> values => FormatVectorLiteral(values),
            int or long or float or double or decimal => Convert.ToString(value, CultureInfo.InvariantCulture)!,
            bool b => b ? "true" : "false",
            string s => $"'{s.Replace("'", "''")}'",
            _ => $"'{value}'"
        };
    }

    private static string FormatVectorLiteral<T>(IEnumerable<T> values) where T : IFormattable
    {
        return $"'[{string.Join(", ", values.Select(value => value.ToString(null, CultureInfo.InvariantCulture)))}]'";
    }
}
