namespace DataVo.Tests.BrowserParity;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, Inherited = true)]
public sealed class BrowserTranslateIgnoreAttribute(string? reason = null) : Attribute
{
    public string? Reason { get; } = reason;
}

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, Inherited = true)]
public sealed class BrowserTranslateNeedsSpecificCodeAttribute(string? reason = null) : Attribute
{
    public string? Reason { get; } = reason;
}
