using System.Text.RegularExpressions;

namespace DataVo.Generators.Sql;

internal static class DataVoQueryShapeParser
{
    private static readonly Regex SelectRegex = new(
        @"^\s*SELECT\s+(?<columns>[A-Za-z0-9_,\s]+)\s+FROM\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s+WHERE\s+(?<where>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*@(?<param>[A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex InsertRegex = new(
        @"^\s*INSERT\s+INTO\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s*\((?<columns>[^)]+)\)\s+VALUES\s*\((?<params>[^)]+)\)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex UpdateRegex = new(
        @"^\s*UPDATE\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s+SET\s+(?<assignments>.+?)\s+WHERE\s+(?<where>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*@(?<param>[A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static bool TryParse(string sql, out GeneratedQueryModel? model)
    {
        model = null;

        Match select = SelectRegex.Match(sql);
        if (select.Success)
        {
            model = new GeneratedQueryModel(
                "SelectSingle",
                select.Groups["table"].Value,
                SplitCsv(select.Groups["columns"].Value),
                select.Groups["where"].Value,
                select.Groups["param"].Value,
                [],
                [],
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
            return true;
        }

        Match insert = InsertRegex.Match(sql);
        if (insert.Success)
        {
            string[] columns = SplitCsv(insert.Groups["columns"].Value);
            string[] parameters = SplitCsv(insert.Groups["params"].Value)
                .Select(RemoveParameterPrefix)
                .ToArray();

            if (columns.Length != parameters.Length)
            {
                return false;
            }

            model = new GeneratedQueryModel(
                "Insert",
                insert.Groups["table"].Value,
                [],
                null,
                null,
                columns,
                parameters,
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
            return true;
        }

        Match update = UpdateRegex.Match(sql);
        if (update.Success)
        {
            Dictionary<string, string> assignments = ParseAssignments(update.Groups["assignments"].Value);
            if (assignments.Count == 0)
            {
                return false;
            }

            model = new GeneratedQueryModel(
                "Update",
                update.Groups["table"].Value,
                [],
                update.Groups["where"].Value,
                update.Groups["param"].Value,
                [],
                [],
                assignments);
            return true;
        }

        return false;
    }

    private static string[] SplitCsv(string value)
    {
        return value.Split(',')
            .Select(static item => item.Trim())
            .Where(static item => item.Length > 0)
            .ToArray();
    }

    private static string RemoveParameterPrefix(string value)
    {
        string trimmed = value.Trim();
        return trimmed.StartsWith("@", StringComparison.Ordinal) ? trimmed.Substring(1) : trimmed;
    }

    private static Dictionary<string, string> ParseAssignments(string value)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (string assignment in SplitCsv(value))
        {
            string[] parts = assignment.Split('=');
            if (parts.Length != 2)
            {
                return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            string column = parts[0].Trim();
            string parameter = RemoveParameterPrefix(parts[1]);
            if (column.Length == 0 || parameter.Length == 0)
            {
                return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            result[column] = parameter;
        }

        return result;
    }
}
