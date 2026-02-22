using DataVo.Core.Utils;

namespace DataVo.Core.Enums;

public class Operators
{
    public const string AND = "AND";
    public const string OR = "OR";

    public const string EQUALS = "=";
    public const string NOT_EQUALS = "!=";
    public const string GREATER_THAN = ">";
    public const string LESS_THAN = "<";
    public const string GREATER_THAN_OR_EQUAL_TO = ">=";
    public const string LESS_THAN_OR_EQUAL_TO = "<=";

    public const string ADD = "+";
    public const string SUBTRACT = "-";
    public const string MUL = "*";
    public const string DIVIDE = "/";

    public const string LEN = "LEN";
    public const string UPPER = "UPPER";
    public const string LOWER = "LOWER";

    public const string NEGATE = "NOT";

    public static List<string> ConditionOperators =
    [
        AND,
        OR,
    ];

    public static List<string> LogicalOperators =
    [
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN,
        LESS_THAN,
        GREATER_THAN_OR_EQUAL_TO,
        LESS_THAN_OR_EQUAL_TO,
    ];

    public static List<string> ArithmeticOperators =
    [
        ADD,
        SUBTRACT,
        MUL,
        DIVIDE,
    ];

    public static List<string> FunctionOperators =
    [
        LEN,
        UPPER,
        LOWER,
    ];

    public static List<string> Supported()
    {
        return ConditionOperators
            .Concat(LogicalOperators)
            .Concat(ArithmeticOperators)
            .OrderByDescending(op => op.Length)
            .ToList();
    }

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