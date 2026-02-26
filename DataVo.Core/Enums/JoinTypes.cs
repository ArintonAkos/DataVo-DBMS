namespace DataVo.Core.Enums;

public class JoinTypes
{
    public const string INNER = "INNER";
    public const string LEFT = "LEFT";
    public const string RIGHT = "RIGHT";
    public const string FULL = "FULL";
    public const string CROSS = "CROSS";

    public static readonly HashSet<string> All = new(StringComparer.OrdinalIgnoreCase)
    {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        CROSS
    };

}