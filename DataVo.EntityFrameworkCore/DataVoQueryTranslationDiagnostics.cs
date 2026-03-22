namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Describes how a DataVo guarded query will execute under the current preview flags.
/// </summary>
public enum DataVoQueryTranslationOutcome
{
    /// <summary>The shape qualifies for the current native translation preview subset.</summary>
    NativeTranslationPreview = 0,

    /// <summary>The shape will run through the guarded bridge path instead of native translation.</summary>
    GuardedFallback = 1,

    /// <summary>The shape is explicitly blocked and will not execute.</summary>
    Blocked = 2
}

/// <summary>
/// Structured translation/fallback/block diagnostics for a guarded DataVo query.
/// </summary>
public sealed record DataVoQueryTranslationDiagnostics(
    DataVoProviderMode ProviderMode,
    DataVoQueryTranslationOutcome Outcome,
    IReadOnlyList<string> Operators,
    IReadOnlyList<string> FallbackReasons,
    IReadOnlyList<string> BlockedReasons,
    string Summary)
{
    /// <summary>True when the query is on the native-translation preview path.</summary>
    public bool WillUseNativeTranslationPreview => Outcome == DataVoQueryTranslationOutcome.NativeTranslationPreview;

    /// <summary>True when the query will fall back to the guarded bridge path.</summary>
    public bool WillUseGuardedFallback => Outcome == DataVoQueryTranslationOutcome.GuardedFallback;

    /// <summary>True when execution should be blocked before query materialization.</summary>
    public bool IsBlocked => Outcome == DataVoQueryTranslationOutcome.Blocked;
}
