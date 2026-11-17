using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DataVo.Generators.Linq;

/// <summary>
/// What every lambda translator shares: rewriting a value (a constant, the state argument, a field or property of
/// it, a static member) as C# that runs inside the interceptor, and recording why a lambda cannot be translated.
/// </summary>
/// <param name="semantics">The semantic model of the file the lambda is in.</param>
/// <param name="state">The lambda's state parameter, or <see langword="null"/> when it has none.</param>
/// <param name="cancellation">Cancels the analysis.</param>
internal abstract class LinqExpressionReader(SemanticModel semantics, IParameterSymbol? state, CancellationToken cancellation)
{
    /// <summary>Gets the semantic model of the file the lambda is in.</summary>
    protected SemanticModel Semantics { get; } = semantics;

    /// <summary>Gets the token that cancels the analysis.</summary>
    protected CancellationToken Cancellation { get; } = cancellation;

    /// <summary>Gets the lambda's state parameter, or <see langword="null"/>.</summary>
    protected IParameterSymbol? State { get; } = state;

    /// <summary>Gets the names of the locals, parameters and instance members the lambda captures.</summary>
    public List<string> Captures { get; } = [];

    /// <summary>Gets the first reason the lambda cannot be translated, or <see langword="null"/>.</summary>
    public string? Failure { get; private set; }

    /// <summary>Rewrites a value expression so it can run inside the interceptor, where only <c>state</c> and statics exist.</summary>
    /// <param name="expression">The value.</param>
    /// <returns>The C# text, or <see langword="null"/> after recording a failure or a capture.</returns>
    protected string? ValueText(ExpressionSyntax expression)
    {
        switch (expression)
        {
            case ParenthesizedExpressionSyntax parenthesized:
                return ValueText(parenthesized.Expression);

            case PostfixUnaryExpressionSyntax forgiving when forgiving.IsKind(SyntaxKind.SuppressNullableWarningExpression):
                return ValueText(forgiving.Operand);

            case ThisExpressionSyntax:
                return Capture("this");

            case IdentifierNameSyntax identifier:
            {
                ISymbol? symbol = Semantics.GetSymbolInfo(identifier, Cancellation).Symbol;
                if (State is not null && SymbolEqualityComparer.Default.Equals(symbol, State))
                {
                    return "state";
                }

                return symbol switch
                {
                    IFieldSymbol or IPropertySymbol when symbol.IsStatic => StaticMember(symbol),
                    ILocalSymbol or IParameterSymbol or IFieldSymbol or IPropertySymbol => Capture(identifier.Identifier.ValueText),
                    _ => Fail($"'{Shorten(expression)}' is not a value the query can be given"),
                };
            }

            case MemberAccessExpressionSyntax access when access.IsKind(SyntaxKind.SimpleMemberAccessExpression):
            {
                ISymbol? symbol = Semantics.GetSymbolInfo(access, Cancellation).Symbol;
                if (symbol is not (IFieldSymbol or IPropertySymbol))
                {
                    return Fail($"'{Shorten(expression)}' is not a field or property");
                }

                if (symbol.IsStatic)
                {
                    return StaticMember(symbol);
                }

                if (ValueText(access.Expression) is not { } receiver)
                {
                    return null;
                }

                return IsReachable(symbol) ? $"{receiver}.{symbol.Name}" : Fail($"generated code cannot read '{symbol.Name}'");
            }

            default:
                return Fail($"'{Shorten(expression)}' is not a constant, the state value, or a field or property of it");
        }
    }

    /// <summary>Writes a static field or property by its fully qualified name, when generated code can reach it.</summary>
    protected string? StaticMember(ISymbol symbol) => IsReachable(symbol) && DataVoLinqAnalyzer.IsNameable(symbol.ContainingType, Semantics.Compilation)
        ? $"{symbol.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}.{symbol.Name}"
        : Fail($"generated code cannot read '{symbol.Name}'; make it internal or pass it as state");

    /// <summary>Whether generated code in the same assembly can use a member.</summary>
    protected bool IsReachable(ISymbol symbol) =>
        symbol.ContainingType is { IsTupleType: true } || Semantics.Compilation.IsSymbolAccessibleWithin(symbol, Semantics.Compilation.Assembly);

    /// <summary>Writes a constant as a C# literal of a column's type, or returns null when it cannot be one.</summary>
    /// <param name="value">The constant.</param>
    /// <param name="columnType">The column's C# type, without <c>?</c>.</param>
    /// <returns>The literal, such as <c>5L</c> or <c>1.5F</c>.</returns>
    protected static string? Literal(object value, ITypeSymbol columnType)
    {
        try
        {
            switch (columnType.SpecialType)
            {
                case SpecialType.System_Int32:
                    return Convert.ToInt32(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
                case SpecialType.System_Int64:
                    return Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture) + "L";
                case SpecialType.System_Single:
                {
                    float number = Convert.ToSingle(value, CultureInfo.InvariantCulture);
                    return float.IsNaN(number) || float.IsInfinity(number) ? null : number.ToString("R", CultureInfo.InvariantCulture) + "F";
                }

                case SpecialType.System_Double:
                {
                    double number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                    return double.IsNaN(number) || double.IsInfinity(number) ? null : number.ToString("R", CultureInfo.InvariantCulture) + "D";
                }

                case SpecialType.System_Boolean:
                    return value is bool flag ? (flag ? "true" : "false") : null;
                case SpecialType.System_String:
                    return value is string text ? SymbolDisplay.FormatLiteral(text, quote: true) : null;
                default:
                    return null;
            }
        }
        catch (Exception exception) when (exception is InvalidCastException or FormatException or OverflowException)
        {
            return null;
        }
    }

    /// <summary>Records a captured name; always returns null so the caller stops translating.</summary>
    protected string? Capture(string name)
    {
        if (!Captures.Contains(name))
        {
            Captures.Add(name);
        }

        return null;
    }

    /// <summary>Records why the lambda cannot be translated (the first reason wins); always returns null.</summary>
    protected string? Fail(string reason)
    {
        Failure ??= reason;
        return null;
    }

    /// <summary>Shortens source text for a diagnostic message.</summary>
    protected static string Shorten(SyntaxNode node)
    {
        string text = node.ToString().Replace("\r", string.Empty).Replace("\n", " ");
        return text.Length <= 60 ? text : text.Substring(0, 57) + "...";
    }
}
