using System.Globalization;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DataVo.Generators.Linq;

/// <summary>
/// Rewrites a <c>Select</c> selector's body as C# that builds the same value from the columns it reads, so the
/// generated projection selects those columns alone and never builds the model.
/// </summary>
/// <remarks>
/// Every <c>p.Column</c> becomes a read of that column from a <c>DataVoRow</c> named <c>row</c>. Everything else is
/// written back with its meaning fixed for a file that has no <c>using</c> directives: types and static members
/// are fully qualified, extension methods become static calls, and constants become literals (so a private
/// constant still works). A selector that uses the row itself, captures a variable, or contains syntax this
/// writer does not know is not rewritten, and the reason is recorded in <see cref="LinqExpressionReader.Failure"/>.
/// </remarks>
/// <example><c>p =&gt; new { p.Name, Rich = p.Gold &gt; 100 }</c> becomes
/// <c>new { Name = (Read…(row["Name"], "Name")), Rich = (Read…(row["Gold"], "Gold")) &gt; (100L) }</c>.</example>
internal sealed class LinqProjectionWriter(
    SemanticModel semantics,
    TableModel model,
    IParameterSymbol row,
    CancellationToken cancellation,
    bool useReactiveRowRef = false)
    : LinqExpressionReader(semantics, null, cancellation)
{
    private const string Support = "global::DataVo.Core.Annotations.DataVoGeneratedTableSupport";

    private static readonly SymbolDisplayFormat TypeFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions |
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    /// <summary>Gets the columns the selector reads, each once, in the order it first reads them.</summary>
    public List<string> Columns { get; } = [];

    /// <summary>Rewrites an expression of the selector's body.</summary>
    /// <param name="expression">The expression.</param>
    /// <returns>The C# text, or <see langword="null"/> when the selector cannot be rewritten.</returns>
    public string? Write(ExpressionSyntax expression)
    {
        Cancellation.ThrowIfCancellationRequested();
        if (Failure is not null || Captures.Count > 0)
        {
            return null;
        }

        if (Semantics.GetConstantValue(expression, Cancellation) is { HasValue: true } constant)
        {
            return Constant(constant.Value, Semantics.GetTypeInfo(expression, Cancellation).Type, expression);
        }

        return expression switch
        {
            ParenthesizedExpressionSyntax parenthesized => Join("(", Write(parenthesized.Expression), ")"),
            IdentifierNameSyntax identifier => Identifier(identifier),
            MemberAccessExpressionSyntax access when access.IsKind(SyntaxKind.SimpleMemberAccessExpression) => MemberAccess(access),
            InvocationExpressionSyntax invocation => Invocation(invocation),
            ObjectCreationExpressionSyntax creation => Creation(creation, creation.ArgumentList, creation.Initializer),
            ImplicitObjectCreationExpressionSyntax creation => Creation(creation, creation.ArgumentList, creation.Initializer),
            AnonymousObjectCreationExpressionSyntax anonymous => Anonymous(anonymous),
            TupleExpressionSyntax tuple => Join("(", Arguments(tuple.Arguments), ")"),
            CastExpressionSyntax cast => Join("((", TypeText(Semantics.GetTypeInfo(cast.Type, Cancellation).Type, cast), ")(", Write(cast.Expression), "))"),
            BinaryExpressionSyntax binary => Binary(binary),
            PrefixUnaryExpressionSyntax unary when unary.Kind() is SyntaxKind.UnaryMinusExpression or SyntaxKind.UnaryPlusExpression or SyntaxKind.LogicalNotExpression or SyntaxKind.BitwiseNotExpression
                => Join(unary.OperatorToken.Text, "(", Write(unary.Operand), ")"),
            PostfixUnaryExpressionSyntax forgiving when forgiving.IsKind(SyntaxKind.SuppressNullableWarningExpression) => Join("(", Write(forgiving.Operand), ")!"),
            ConditionalExpressionSyntax conditional => Join("(", Write(conditional.Condition), " ? ", Write(conditional.WhenTrue), " : ", Write(conditional.WhenFalse), ")"),
            ConditionalAccessExpressionSyntax conditional => Join("(", Write(conditional.Expression), ")?", Binding(conditional.WhenNotNull)),
            ElementAccessExpressionSyntax element => Join(Write(element.Expression), "[", Arguments(element.ArgumentList.Arguments), "]"),
            InterpolatedStringExpressionSyntax interpolated => Interpolated(interpolated),
            DefaultExpressionSyntax defaulted => Join("default(", TypeText(Semantics.GetTypeInfo(defaulted.Type, Cancellation).Type, defaulted), ")"),
            LiteralExpressionSyntax literal when literal.IsKind(SyntaxKind.DefaultLiteralExpression)
                => Join("default(", TypeText(Semantics.GetTypeInfo(literal, Cancellation).ConvertedType, literal), ")"),
            CheckedExpressionSyntax arithmetic => Join(arithmetic.Keyword.Text, "(", Write(arithmetic.Expression), ")"),
            IsPatternExpressionSyntax test => Join("(", Write(test.Expression), " is ", Pattern(test.Pattern), ")"),
            SwitchExpressionSyntax choice => Switch(choice),
            ImplicitArrayCreationExpressionSyntax array => Join("new[] { ", Elements(array.Initializer), " }"),
            ArrayCreationExpressionSyntax array => Array(array),
            AnonymousFunctionExpressionSyntax => Fail("a lambda inside the selector cannot be compiled"),
            _ => Fail($"'{Shorten(expression)}' cannot be compiled into a projection"),
        };
    }

    /// <summary>A name: the row (not allowed), a capture, a static member or a type.</summary>
    private string? Identifier(IdentifierNameSyntax identifier)
    {
        ISymbol? symbol = Semantics.GetSymbolInfo(identifier, Cancellation).Symbol;
        if (SymbolEqualityComparer.Default.Equals(symbol, row))
        {
            return Fail($"it uses the row '{identifier.Identifier.ValueText}' itself, which needs the whole model; select its columns instead");
        }

        return symbol switch
        {
            ILocalSymbol or IParameterSymbol => Capture(identifier.Identifier.ValueText),
            IFieldSymbol or IPropertySymbol when symbol.IsStatic => StaticMember(symbol),
            IFieldSymbol or IPropertySymbol => Capture("this"),
            ITypeSymbol type => TypeText(type, identifier),
            _ => Fail($"'{identifier.Identifier.ValueText}' cannot be compiled into a projection"),
        };
    }

    /// <summary>A member access: a column of the row, a static member, a type, or a member of another value.</summary>
    private string? MemberAccess(MemberAccessExpressionSyntax access)
    {
        ISymbol? symbol = Semantics.GetSymbolInfo(access, Cancellation).Symbol;
        if (access.Expression is IdentifierNameSyntax receiver
            && SymbolEqualityComparer.Default.Equals(Semantics.GetSymbolInfo(receiver, Cancellation).Symbol, row))
        {
            return symbol is IPropertySymbol property ? Column(property.Name) : Fail($"it calls '{access.Name.Identifier.ValueText}' on the row, which needs the whole model");
        }

        return symbol switch
        {
            ITypeSymbol type => TypeText(type, access),
            IFieldSymbol or IPropertySymbol when symbol.IsStatic => StaticMember(symbol),
            IFieldSymbol or IPropertySymbol when IsReachable(symbol) => Join(Write(access.Expression), "." + symbol.Name),
            _ => Fail($"'{Shorten(access)}' cannot be compiled into a projection"),
        };
    }

    /// <summary>Reads a stored column as its property's type, and records it.</summary>
    private string? Column(string property)
    {
        ColumnModel? column = model.Columns.FirstOrDefault(candidate => candidate.PropertyName == property);
        if (column is null)
        {
            return Fail($"it reads {property}, which is not a stored column");
        }

        int ordinal = Columns.IndexOf(column.PropertyName);
        if (ordinal < 0)
        {
            ordinal = Columns.Count;
            Columns.Add(column.PropertyName);
        }

        string name = SymbolDisplay.FormatLiteral(column.PropertyName, quote: true);
        return useReactiveRowRef
            ? $"(row.{column.Type.ReaderMethod}({ordinal.ToString(CultureInfo.InvariantCulture)}, {name}){column.Type.ReadSuffix})"
            : $"({Support}.{column.Type.ReaderMethod}(row[{name}], {name}){column.Type.ReadSuffix})";
    }

    /// <summary>A method call: static, instance, extension (rewritten as a static call) or a delegate's invoke.</summary>
    private string? Invocation(InvocationExpressionSyntax invocation)
    {
        if (Semantics.GetSymbolInfo(invocation, Cancellation).Symbol is not IMethodSymbol method
            || method.MethodKind == MethodKind.LocalFunction
            || !IsReachable(method))
        {
            return Fail($"'{Shorten(invocation)}' calls a method generated code cannot reach");
        }

        // The receiver is written before the arguments, so columns are recorded in the order the source reads them.
        string? typeArguments = TypeArguments(invocation.Expression, method);
        string? callee = method switch
        {
            { MethodKind: MethodKind.DelegateInvoke } => Write(invocation.Expression),
            { ReducedFrom: { } extension } when invocation.Expression is MemberAccessExpressionSyntax target
                => Join(TypeText(extension.ContainingType, invocation), "." + method.Name, typeArguments, "(", Write(target.Expression), invocation.ArgumentList.Arguments.Count == 0 ? string.Empty : ", "),
            { ReducedFrom: not null } => Fail($"'{Shorten(invocation)}' cannot be compiled into a projection"),
            { IsStatic: true } => Join(TypeText(method.ContainingType, invocation), "." + method.Name, typeArguments, "("),
            _ when invocation.Expression is MemberAccessExpressionSyntax instance => Join(Write(instance.Expression), "." + method.Name, typeArguments, "("),
            _ => Capture("this"),
        };

        if (method.MethodKind == MethodKind.DelegateInvoke)
        {
            callee = Join(callee, "(");
        }

        return Join(callee, Arguments(invocation.ArgumentList.Arguments), ")");
    }

    /// <summary>The type arguments of a call that names them in source; inferred ones are left to inference.</summary>
    private string? TypeArguments(ExpressionSyntax target, IMethodSymbol method)
    {
        SimpleNameSyntax? name = target switch
        {
            MemberAccessExpressionSyntax access => access.Name,
            SimpleNameSyntax simple => simple,
            _ => null,
        };
        if (name is not GenericNameSyntax)
        {
            return string.Empty;
        }

        string?[] types = [.. method.TypeArguments.Select(type => TypeText(type, target))];
        return types.Any(static type => type is null) ? null : "<" + string.Join(", ", types) + ">";
    }

    /// <summary>A <c>new</c> expression, with its type spelled out and its initializer rewritten.</summary>
    private string? Creation(ExpressionSyntax creation, ArgumentListSyntax? arguments, InitializerExpressionSyntax? initializer)
    {
        ITypeSymbol? type = Semantics.GetTypeInfo(creation, Cancellation).Type;
        if (Semantics.GetSymbolInfo(creation, Cancellation).Symbol is IMethodSymbol constructor && !IsReachable(constructor))
        {
            return Fail($"generated code cannot call the constructor of {type?.Name}");
        }

        string? written = Join("new ", TypeText(type, creation), "(", arguments is null ? string.Empty : Arguments(arguments.Arguments), ")");
        return initializer is null ? written : Join(written, " { ", Initializer(initializer), " }");
    }

    /// <summary>An object or collection initializer.</summary>
    private string? Initializer(InitializerExpressionSyntax initializer)
    {
        var parts = new List<string>();
        foreach (ExpressionSyntax entry in initializer.Expressions)
        {
            string? part = entry switch
            {
                AssignmentExpressionSyntax { Left: IdentifierNameSyntax member, Right: not InitializerExpressionSyntax } assignment
                    when initializer.IsKind(SyntaxKind.ObjectInitializerExpression)
                    => Join(member.Identifier.ValueText, " = ", Write(assignment.Right)),
                _ when initializer.IsKind(SyntaxKind.CollectionInitializerExpression) && AddsWithExtension(entry)
                    => Fail("a collection initializer that uses an extension Add method cannot be compiled"),
                InitializerExpressionSyntax element when initializer.IsKind(SyntaxKind.CollectionInitializerExpression)
                    => Join("{ ", Elements(element), " }"),
                _ when initializer.IsKind(SyntaxKind.CollectionInitializerExpression) => Write(entry),
                _ => Fail($"the initializer '{Shorten(entry)}' cannot be compiled into a projection"),
            };
            if (part is null)
            {
                return null;
            }

            parts.Add(part);
        }

        return string.Join(", ", parts);
    }

    private bool AddsWithExtension(ExpressionSyntax entry) =>
        Semantics.GetCollectionInitializerSymbolInfo(entry, Cancellation).Symbol is IMethodSymbol { IsExtensionMethod: true };

    /// <summary>An anonymous type, with each member named explicitly so the type is the same one.</summary>
    private string? Anonymous(AnonymousObjectCreationExpressionSyntax anonymous)
    {
        var members = new List<string>();
        foreach (AnonymousObjectMemberDeclaratorSyntax member in anonymous.Initializers)
        {
            string? name = member.NameEquals?.Name.Identifier.ValueText
                ?? (Semantics.GetDeclaredSymbol(member, Cancellation) as IPropertySymbol)?.Name;
            string? value = Write(member.Expression);
            if (name is null || value is null)
            {
                return name is null ? Fail($"the member '{Shorten(member)}' has no name") : null;
            }

            members.Add($"{name} = {value}");
        }

        return "new { " + string.Join(", ", members) + " }";
    }

    /// <summary>A binary operator; <c>is</c> and <c>as</c> take a type on the right.</summary>
    private string? Binary(BinaryExpressionSyntax binary)
    {
        string op = " " + binary.OperatorToken.Text + " ";
        if (binary.Kind() is SyntaxKind.IsExpression or SyntaxKind.AsExpression)
        {
            return Join("(", Write(binary.Left), op, TypeText(Semantics.GetTypeInfo(binary.Right, Cancellation).Type, binary.Right), ")");
        }

        return Join(Write(binary.Left), op, Write(binary.Right));
    }

    /// <summary>What follows <c>?</c> in a null-conditional access.</summary>
    private string? Binding(ExpressionSyntax binding) => binding switch
    {
        MemberBindingExpressionSyntax member when Semantics.GetSymbolInfo(member, Cancellation).Symbol is { } symbol && IsReachable(symbol)
            => "." + member.Name.Identifier.ValueText,
        ElementBindingExpressionSyntax element => Join("[", Arguments(element.ArgumentList.Arguments), "]"),
        MemberAccessExpressionSyntax access => Join(Binding(access.Expression), "." + access.Name.Identifier.ValueText),
        InvocationExpressionSyntax call when Semantics.GetSymbolInfo(call, Cancellation).Symbol is IMethodSymbol { ReducedFrom: null } method && IsReachable(method)
            => Join(Binding(call.Expression), "(", Arguments(call.ArgumentList.Arguments), ")"),
        ConditionalAccessExpressionSyntax nested => Join(Binding(nested.Expression), "?", Binding(nested.WhenNotNull)),
        _ => Fail($"'{Shorten(binding)}' after '?.' cannot be compiled into a projection"),
    };

    /// <summary>An interpolated string, rewritten piece by piece. Raw string literals are not rewritten.</summary>
    private string? Interpolated(InterpolatedStringExpressionSyntax interpolated)
    {
        string start = interpolated.StringStartToken.Text;
        if (start.Contains("\"\"\""))
        {
            return Fail("a raw interpolated string cannot be compiled into a projection");
        }

        var text = new System.Text.StringBuilder(start);
        foreach (InterpolatedStringContentSyntax content in interpolated.Contents)
        {
            if (content is InterpolatedStringTextSyntax literal)
            {
                text.Append(literal.TextToken.Text);
                continue;
            }

            var hole = (InterpolationSyntax)content;
            if (Write(hole.Expression) is not { } value)
            {
                return null;
            }

            text.Append(hole.OpenBraceToken.Text).Append('(').Append(value).Append(')');
            if (hole.AlignmentClause is { } alignment)
            {
                if (Semantics.GetConstantValue(alignment.Value, Cancellation) is not { HasValue: true, Value: int width })
                {
                    return Fail("an interpolation's alignment must be a constant");
                }

                text.Append(',').Append(width.ToString(CultureInfo.InvariantCulture));
            }

            if (hole.FormatClause is { } format)
            {
                text.Append(':').Append(format.FormatStringToken.Text);
            }

            text.Append(hole.CloseBraceToken.Text);
        }

        return text.Append(interpolated.StringEndToken.Text).ToString();
    }

    /// <summary>A <c>switch</c> expression over constant, relational and logical patterns.</summary>
    private string? Switch(SwitchExpressionSyntax choice)
    {
        var arms = new List<string>();
        foreach (SwitchExpressionArmSyntax arm in choice.Arms)
        {
            string? when = arm.WhenClause is null ? string.Empty : Join(" when ", Write(arm.WhenClause.Condition));
            string? written = Join(Pattern(arm.Pattern), when, " => ", Write(arm.Expression));
            if (written is null)
            {
                return null;
            }

            arms.Add(written);
        }

        return Join("(", Write(choice.GoverningExpression), " switch { ", string.Join(", ", arms), " })");
    }

    /// <summary>A pattern of an <c>is</c> or <c>switch</c>: constants, relations, types, <c>not</c>, <c>and</c>, <c>or</c>, <c>_</c>.</summary>
    private string? Pattern(PatternSyntax pattern) => pattern switch
    {
        ConstantPatternSyntax constant => Write(constant.Expression),
        RelationalPatternSyntax relation => Join(relation.OperatorToken.Text, " ", Write(relation.Expression)),
        UnaryPatternSyntax negated => Join("not ", Pattern(negated.Pattern)),
        BinaryPatternSyntax both => Join(Pattern(both.Left), " " + both.OperatorToken.Text + " ", Pattern(both.Right)),
        ParenthesizedPatternSyntax parenthesized => Join("(", Pattern(parenthesized.Pattern), ")"),
        DiscardPatternSyntax => "_",
        TypePatternSyntax type => TypeText(Semantics.GetTypeInfo(type.Type, Cancellation).Type, type),
        _ => Fail($"the pattern '{Shorten(pattern)}' cannot be compiled into a projection"),
    };

    /// <summary>An array creation with an initializer, such as <c>new int[] { a, b }</c>.</summary>
    private string? Array(ArrayCreationExpressionSyntax array)
    {
        if (Semantics.GetTypeInfo(array, Cancellation).Type is not IArrayTypeSymbol { Rank: 1 } type || array.Initializer is null)
        {
            return Fail($"'{Shorten(array)}' cannot be compiled into a projection");
        }

        return Join("new ", TypeText(type.ElementType, array), "[] { ", Elements(array.Initializer), " }");
    }

    private string? Elements(InitializerExpressionSyntax initializer) => JoinAll(initializer.Expressions.Select(Write));

    /// <summary>Call or tuple arguments, keeping argument names; <c>ref</c>, <c>out</c> and <c>in</c> are not supported.</summary>
    private string? Arguments(SeparatedSyntaxList<ArgumentSyntax> arguments) => JoinAll(arguments.Select(argument =>
        argument.RefKindKeyword.IsKind(SyntaxKind.None)
            ? Join(argument.NameColon is null ? string.Empty : argument.NameColon.Name.Identifier.ValueText + ": ", Write(argument.Expression))
            : Fail($"the argument '{Shorten(argument)}' passes a reference, which cannot be compiled into a projection")));

    /// <summary>Spells a type so generated code names the same type; fails for a type it cannot name or reach.</summary>
    private string? TypeText(ITypeSymbol? type, SyntaxNode at) =>
        type is not null && DataVoLinqAnalyzer.IsNameable(type, Semantics.Compilation)
            ? type.ToDisplayString(TypeFormat)
            : Fail($"'{Shorten(at)}' uses a type generated code cannot name");

    /// <summary>Writes a constant as a literal of its own type, so overloads and conversions resolve as in source.</summary>
    private string? Constant(object? value, ITypeSymbol? type, ExpressionSyntax at)
    {
        if (value is null)
        {
            return type is null || type.SpecialType == SpecialType.System_Object && at is LiteralExpressionSyntax ? "null" : Join("default(", TypeText(type, at), ")");
        }

        if (type is INamedTypeSymbol { TypeKind: TypeKind.Enum, EnumUnderlyingType: { } underlying } enumeration)
        {
            return Join("((", TypeText(enumeration, at), ")", Constant(value, underlying, at), ")");
        }

        string? literal = value switch
        {
            bool flag => flag ? "true" : "false",
            string text => SymbolDisplay.FormatLiteral(text, quote: true),
            char letter => SymbolDisplay.FormatLiteral(letter, quote: true),
            int number => number.ToString(CultureInfo.InvariantCulture),
            long number => number.ToString(CultureInfo.InvariantCulture) + "L",
            uint number => number.ToString(CultureInfo.InvariantCulture) + "U",
            ulong number => number.ToString(CultureInfo.InvariantCulture) + "UL",
            float number => Real(float.IsNaN(number), float.IsPositiveInfinity(number), float.IsNegativeInfinity(number), "global::System.Single", number.ToString("R", CultureInfo.InvariantCulture) + "F"),
            double number => Real(double.IsNaN(number), double.IsPositiveInfinity(number), double.IsNegativeInfinity(number), "global::System.Double", number.ToString("R", CultureInfo.InvariantCulture) + "D"),
            decimal number => number.ToString(CultureInfo.InvariantCulture) + "M",
            short or ushort or byte or sbyte => Join("((", TypeText(type, at), ")", Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture), ")"),
            _ => null,
        };
        return literal is null ? Fail($"the constant '{Shorten(at)}' cannot be written in generated code") : "(" + literal + ")";
    }

    /// <summary>Writes a floating-point literal, or the named constant for NaN and the infinities.</summary>
    private static string Real(bool nan, bool positiveInfinity, bool negativeInfinity, string type, string written) =>
        nan ? type + ".NaN" : positiveInfinity ? type + ".PositiveInfinity" : negativeInfinity ? type + ".NegativeInfinity" : written;

    /// <summary>Concatenates pieces, or returns null when any piece is null (it failed).</summary>
    private static string? Join(params string?[] parts) => parts.Any(static part => part is null) ? null : string.Concat(parts);

    /// <summary>Joins rewritten items with commas, or returns null when any item failed.</summary>
    private static string? JoinAll(IEnumerable<string?> items)
    {
        string?[] written = [.. items];
        return written.Any(static item => item is null) ? null : string.Join(", ", written);
    }
}
