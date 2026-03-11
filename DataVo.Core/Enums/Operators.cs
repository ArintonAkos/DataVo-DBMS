using DataVo.Core.Utils;

namespace DataVo.Core.Enums;

/// <summary>
/// Centralizes the operators and built-in function names supported by expression parsing.
/// </summary>
public static class Operators
{
    public const string AND = "AND";
    public const string OR = "OR";

    public const string EQUALS = "=";
    public const string NOT_EQUALS = "!=";
    public const string GREATER_THAN = ">";
    public const string LESS_THAN = "<";
    public const string GREATER_THAN_OR_EQUAL_TO = ">=";
    public const string LESS_THAN_OR_EQUAL_TO = "<=";
    public const string LIKE = "LIKE";

    public const string IS_NULL = "IS NULL";
    public const string IS_NOT_NULL = "IS NOT NULL";

    public const string ADD = "+";
    public const string SUBTRACT = "-";
    public const string MUL = "*";
    public const string DIVIDE = "/";

    public const string LEN = "LEN";
    public const string UPPER = "UPPER";
    public const string LOWER = "LOWER";

    public const string NEGATE = "NOT";

    /// <summary>
    /// Gets the Boolean composition operators.
    /// </summary>
    public static List<string> ConditionOperators =
    [
        AND,
        OR,
    ];

    /// <summary>
    /// Gets the comparison operators used in predicates.
    /// </summary>
    public static List<string> LogicalOperators =
    [
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN,
        LESS_THAN,
        GREATER_THAN_OR_EQUAL_TO,
        LESS_THAN_OR_EQUAL_TO,
        LIKE,
        IS_NULL,
        IS_NOT_NULL
    ];

    /// <summary>
    /// Gets the arithmetic operators supported by scalar expression evaluation.
    /// </summary>
    public static List<string> ArithmeticOperators =
    [
        ADD,
        SUBTRACT,
        MUL,
        DIVIDE,
    ];

    /// <summary>
    /// Gets the built-in scalar function identifiers.
    /// </summary>
    public static List<string> FunctionOperators =
    [
        LEN,
        UPPER,
        LOWER,
    ];

    private static readonly Lazy<List<string>> SupportedOperators = new(() => ConditionOperators
        .Concat(LogicalOperators)
        .Concat(ArithmeticOperators)
        .Concat(FunctionOperators)
        .OrderByDescending(op => op.Length)
        .ToList());

    /// <summary>
    /// Gets all operators ordered by descending length so multi-character matches win over shorter prefixes.
    /// </summary>
    /// <returns>The supported operators in matching priority order.</returns>
    public static List<string> Supported()
    {
        return SupportedOperators.Value;
    }

    /// <summary>
    /// Determines whether the input contains a supported operator starting at a given position.
    /// </summary>
    /// <param name="input">The expression text being scanned.</param>
    /// <param name="pos">The zero-based position to inspect.</param>
    /// <param name="length">When this method returns, contains the matched operator length if successful.</param>
    /// <returns><see langword="true"/> when a supported operator is found; otherwise <see langword="false"/>.</returns>
    public static bool ContainsOperator(string input, int pos, out int length)
    {
        int remainingLength = input.Length - (pos + 1);
        length = -1;

        string? supportedOperator = Supported().FirstOrDefault(op =>
        {
            int opLen = op.Length;

            return remainingLength >= opLen - 1 && op.EqualsSerialized(input.Substring(pos, opLen));
        });


        if (supportedOperator != null)
        {
            length = supportedOperator.Length;
        }

        return supportedOperator != null;
    }
}