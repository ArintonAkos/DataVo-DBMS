using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal static class AuthorizationCommandHelper
{
    public static string UnquoteSqlStringLiteral(string literal)
    {
        if (string.IsNullOrWhiteSpace(literal) || literal.Length < 2 || literal[0] != '\'' || literal[^1] != '\'')
        {
            throw new AuthorizationException("Expected quoted string literal.");
        }

        string inner = literal[1..^1];
        return inner.Replace("''", "'", StringComparison.Ordinal);
    }

    public static List<DatabasePermission> ParsePermissions(IEnumerable<string> rawPermissions)
    {
        var permissions = new List<DatabasePermission>();

        foreach (string raw in rawPermissions)
        {
            if (TryParsePermission(raw, out DatabasePermission permission))
            {
                permissions.Add(permission);
                continue;
            }

            throw new AuthorizationException($"Unknown permission token '{raw}'.");
        }

        return permissions;
    }

    private static bool TryParsePermission(string raw, out DatabasePermission permission)
    {
        permission = DatabasePermission.ReadData;
        string token = raw.Trim();

        if (token.Equals("READ", StringComparison.OrdinalIgnoreCase) || token.Equals("READDATA", StringComparison.OrdinalIgnoreCase))
        {
            permission = DatabasePermission.ReadData;
            return true;
        }

        if (token.Equals("WRITE", StringComparison.OrdinalIgnoreCase) || token.Equals("WRITEDATA", StringComparison.OrdinalIgnoreCase))
        {
            permission = DatabasePermission.WriteData;
            return true;
        }

        if (token.Equals("SCHEMA", StringComparison.OrdinalIgnoreCase) || token.Equals("MANAGESCHEMA", StringComparison.OrdinalIgnoreCase))
        {
            permission = DatabasePermission.ManageSchema;
            return true;
        }

        if (token.Equals("TRANSACTION", StringComparison.OrdinalIgnoreCase)
            || token.Equals("TRANSACTIONS", StringComparison.OrdinalIgnoreCase)
            || token.Equals("MANAGETRANSACTIONS", StringComparison.OrdinalIgnoreCase))
        {
            permission = DatabasePermission.ManageTransactions;
            return true;
        }

        if (token.Equals("SECURITY", StringComparison.OrdinalIgnoreCase) || token.Equals("MANAGESECURITY", StringComparison.OrdinalIgnoreCase))
        {
            permission = DatabasePermission.ManageSecurity;
            return true;
        }

        if (token.Equals("ADMIN", StringComparison.OrdinalIgnoreCase))
        {
            permission = DatabasePermission.Admin;
            return true;
        }

        return false;
    }
}
