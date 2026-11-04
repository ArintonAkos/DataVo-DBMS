using System.Reflection;
using System.Text.RegularExpressions;
using DataVo.Core.Exceptions;

namespace DataVo.Core.Contracts.Results;

/// <summary>A classified failure: stable code, clean one-line message, and an optional source position.</summary>
internal readonly record struct DataVoError(string Code, string Message, int? Line, int? Column);

/// <summary>
/// Turns exceptions and legacy error messages into the public error contract: a stable code from
/// <see cref="DataVoErrorCodes"/>, a single human sentence (no exception type names, no stack trace),
/// and the line/column of syntax errors. An explicit code on a <see cref="DataVoException"/> wins;
/// otherwise anchored message patterns refine the exception type's default code. The error-contract
/// tests pin every classification, so a message change that breaks one fails the build.
/// </summary>
internal static class DataVoErrorClassifier
{
    private static readonly (Regex Pattern, string Code)[] MessagePatterns =
    [
        (new Regex(@"no database in use", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.NoDatabaseSelected),
        (new Regex(@"database \S+ (doesn't|does not) exist|unknown database", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.UnknownDatabase),
        (new Regex(@"ambiguous column", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.AmbiguousColumn),
        (new Regex(@"table or alias \S+ not found", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.UnknownTableAlias),
        (new Regex(@"table \S+ (doesn't|does not) exist", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.UnknownTable),
        (new Regex(@"invalid column name|column \S+ (doesn't|does not) exist|cannot resolve column|column referred by index", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.UnknownColumn),
        (new Regex(@"index( file)? \S+ (doesn't|does not) exist", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.UnknownIndex),
        (new Regex(@"database (\S+ )?already exists", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.DatabaseExists),
        (new Regex(@"table \S+ already exists", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.TableExists),
        (new Regex(@"index( file)? \S+ already exists", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.IndexExists),
        (new Regex(@"primary key cannot be null", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.NullPrimaryKey),
        (new Regex(@"primary key violation", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.PrimaryKeyViolation),
        (new Regex(@"unique key violation|duplicate value .* for unique column", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.UniqueViolation),
        (new Regex(@"foreign key violation", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.ForeignKeyViolation),
        (new Regex(@"not null", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.NotNullViolation),
        (new Regex(@"number of values provided", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.ValueCountMismatch),
        (new Regex(@"too long for column|exceeds the declared length", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.ValueTooLong),
        (new Regex(@"division by zero", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.DivisionByZero),
        (new Regex(@"type of argument doesn't match|cannot coerce|type mismatch", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.TypeMismatch),
        (new Regex(@"no active transaction", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.NoActiveTransaction),
        (new Regex(@"transaction is already active", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), DataVoErrorCodes.TransactionAlreadyActive),
    ];

    private static readonly Regex Position = new(@"at line (\d+), column (\d+)", RegexOptions.CultureInvariant);

    private static readonly Regex Prefix = new(
        @"^(?:Error:\s*|(?:Parser|Lexer|Binding|Evaluation|Catalog) Error:\s*)+",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static DataVoError Classify(Exception exception)
    {
        Exception ex = Unwrap(exception);
        string message = Clean(ex.Message);

        if (ex is DataVoException { ErrorCode: { } explicitCode } explicitError)
        {
            return new DataVoError(explicitCode, message, explicitError.Line, explicitError.Column);
        }

        if (ex is ParserException or LexerException)
        {
            (int? line, int? column) = ReadPosition(message);
            return new DataVoError(DataVoErrorCodes.Syntax, message, line, column);
        }

        string fallback = ex switch
        {
            AuthorizationException => DataVoErrorCodes.PermissionDenied,
            DeadlockDetectedException => DataVoErrorCodes.Deadlock,
            DatabaseInUseException => DataVoErrorCodes.DatabaseInUse,
            BindingException => DataVoErrorCodes.Binding,
            CatalogException => DataVoErrorCodes.Catalog,
            IndexException => DataVoErrorCodes.Index,
            EvaluationException => DataVoErrorCodes.Evaluation,
            StorageException => DataVoErrorCodes.Storage,
            DataVoException => DataVoErrorCodes.General,
            _ => DataVoErrorCodes.Internal,
        };

        // Authorization, deadlock, and lock-ownership failures keep their type-based code.
        if (fallback is DataVoErrorCodes.PermissionDenied or DataVoErrorCodes.Deadlock or DataVoErrorCodes.DatabaseInUse)
        {
            return new DataVoError(fallback, message, null, null);
        }

        string code = MatchMessage(message) ?? fallback;
        if (code == DataVoErrorCodes.Internal)
        {
            message = $"Internal error ({ex.GetType().Name}): {message}";
        }

        return new DataVoError(code, message, null, null);
    }

    /// <summary>Classifies an error that an action reported as text rather than as an exception.</summary>
    public static DataVoError ClassifyMessage(string rawMessage)
    {
        string message = Clean(rawMessage);
        return new DataVoError(MatchMessage(message) ?? DataVoErrorCodes.General, message, null, null);
    }

    /// <summary>Removes exception type names, "Error:" prefixes, and stack-trace lines.</summary>
    public static string Clean(string message)
    {
        string firstLine = message.Split('\n')[0].Trim();
        int typeSeparator = firstLine.IndexOf("Exception: ", StringComparison.Ordinal);
        if (typeSeparator >= 0 && !firstLine[..typeSeparator].Contains(' '))
        {
            firstLine = firstLine[(typeSeparator + "Exception: ".Length)..];
        }

        return Prefix.Replace(firstLine, string.Empty).Trim();
    }

    private static string? MatchMessage(string message)
    {
        foreach ((Regex pattern, string code) in MessagePatterns)
        {
            if (pattern.IsMatch(message))
            {
                return code;
            }
        }

        return null;
    }

    private static (int? Line, int? Column) ReadPosition(string message)
    {
        Match match = Position.Match(message);
        return match.Success
            ? (int.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture),
               int.Parse(match.Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture))
            : (null, null);
    }

    private static Exception Unwrap(Exception exception)
    {
        Exception current = exception;
        while (true)
        {
            switch (current)
            {
                case AggregateException { InnerExceptions.Count: 1 } aggregate:
                    current = aggregate.InnerExceptions[0];
                    continue;
                case TargetInvocationException { InnerException: { } inner }:
                    current = inner;
                    continue;
                default:
                    return current;
            }
        }
    }
}
