using System.Globalization;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DataVo.Generators.Linq;

/// <summary>
/// Translates a query's predicate into a SQL condition over <c>@p0</c>, <c>@p1</c>..., the C# that binds those
/// parameters from the state argument, and, for unindexed columns, the condition as C# compiled into the scan.
/// </summary>
internal sealed class LinqConditionTranslator(SemanticModel semantics, TableModel model, IParameterSymbol row, IParameterSymbol? state, CancellationToken cancellation)
    : LinqExpressionReader(semantics, state, cancellation)
{
    private const string NullLiteral = "\0null";

    /// <summary>Gets the values bound to <c>@p0</c>, <c>@p1</c>..., in order.</summary>
    public List<LinqParameter> Parameters { get; } = [];

    /// <summary>Whether the condition requires the primary key to equal one value.</summary>
    public bool Unique { get; private set; }

    /// <summary>
    /// The condition that compares the model's integer primary key to a value, when the predicate has one
    /// at its top level. A predicate that is nothing else is a lookup by key.
    /// </summary>
    public string? IntegerKeyCondition { get; private set; }

    // Each leaf of the condition (a comparison, a null test, a bare bool column) as C# in three forms, when
    // its column can be read directly. A leaf without an entry keeps the whole query on the SQL path.
    // The fourth form is the leaf as a call on a ColumnarPredicateBuilder, for tables with a columnar index.
    private readonly Dictionary<SyntaxNode, (string Row, string Bytes, string Cells, string Columnar)> _kernelLeaves = [];
    private readonly List<string> _kernelColumns = [];
    private readonly List<char> _kernelKinds = [];

    /// <summary>
    /// Builds the condition as C# for the scan. Call after <see cref="Condition"/> succeeded. Returns null
    /// when a column is indexed (the SQL path can seek) or has a type without a direct reader.
    /// </summary>
    public LinqKernel? Kernel(ExpressionSyntax body) =>
        KernelText(body, 0) is { } row && KernelText(body, 1) is { } bytes && KernelText(body, 2) is { } cells
            ? new LinqKernel(row, bytes, cells, new EquatableArray<string>([.. _kernelColumns]), new string([.. _kernelKinds]), new EquatableArray<string>([.. KernelSteps(body, [])]))
            : null;

    /// <summary>
    /// The condition as builder calls in postfix order: each leaf, then the AND or OR that joins the two
    /// operands before it. Call only for a condition <see cref="KernelText"/> accepted.
    /// </summary>
    private List<string> KernelSteps(ExpressionSyntax expression, List<string> steps)
    {
        if (_kernelLeaves.TryGetValue(expression, out (string Row, string Bytes, string Cells, string Columnar) leaf))
        {
            steps.Add(leaf.Columnar);
        }
        else if (expression is ParenthesizedExpressionSyntax parenthesized)
        {
            KernelSteps(parenthesized.Expression, steps);
        }
        else if (expression is BinaryExpressionSyntax binary)
        {
            KernelSteps(binary.Left, steps);
            KernelSteps(binary.Right, steps);
            steps.Add(binary.IsKind(SyntaxKind.LogicalAndExpression) ? "builder.And();" : "builder.Or();");
        }

        return steps;
    }

    /// <summary>The condition in one form: 0 over a RowRef, 1 over a row's bytes, 2 over a typed row's cells.</summary>
    private string? KernelText(ExpressionSyntax expression, int form)
    {
        if (_kernelLeaves.TryGetValue(expression, out (string Row, string Bytes, string Cells, string Columnar) leaf))
        {
            return form switch { 0 => leaf.Row, 1 => leaf.Bytes, _ => leaf.Cells };
        }

        switch (expression)
        {
            case ParenthesizedExpressionSyntax parenthesized:
                return KernelText(parenthesized.Expression, form) is { } inner ? $"({inner})" : null;

            case BinaryExpressionSyntax binary when binary.IsKind(SyntaxKind.LogicalAndExpression) || binary.IsKind(SyntaxKind.LogicalOrExpression):
            {
                string? left = KernelText(binary.Left, form);
                string? right = KernelText(binary.Right, form);
                bool and = binary.IsKind(SyntaxKind.LogicalAndExpression);

                // Leaves are self-contained and source parentheses are kept. Over a RowRef the operators
                // short-circuit, as each leaf is a call. The direct forms read cells that are already known
                // to be present, so both sides are always evaluated and no branch is needed between them.
                string op = form == 0 ? (and ? "&&" : "||") : (and ? "&" : "|");
                return left is null || right is null ? null : $"{left} {op} {right}";
            }

            default:
                return null;
        }
    }

    /// <summary>How a column's type is read in each form, or null when the type has no direct reader.</summary>
    private static (string RowRef, char Kind, string Bytes, string Cells)? KernelReader(ColumnModel column) => column.Type.SqlType switch
    {
        "INT" => ("GetInt32", 'i', "Int32", "{0}.AsInt32()"),
        "BIGINT" => ("GetInt64", 'l', "Int64", "{0}.AsInt64()"),
        "FLOAT" => ("GetFloat", 'f', "Single", "(float){0}.AsDouble()"),
        "DOUBLE" => ("GetDouble", 'd', "Double", "{0}.AsDouble()"),
        "BIT" => ("GetBoolean", 'b', "Boolean", "{0}.AsBoolean()"),
        "GUID" => ("GetGuid", 'g', "Guid", "{0}.AsGuid()"),
        _ => null,
    };

    /// <summary>The index of a column in the compiled condition, or -1 when the SQL path serves it better.</summary>
    /// <param name="kind">How the condition reads the column; <c>n</c> when it only tests it for NULL.</param>
    private int KernelColumn(ColumnModel column, char kind)
    {
        if (column.IsPrimaryKey || column.IsIndexed || column.IsSpatialIndexed)
        {
            return -1;
        }

        int index = _kernelColumns.IndexOf(column.PropertyName);
        if (index < 0)
        {
            index = _kernelColumns.Count;
            _kernelColumns.Add(column.PropertyName);
            _kernelKinds.Add(kind);
        }
        else if (kind != 'n')
        {
            _kernelKinds[index] = kind;
        }

        return index;
    }

    private void KernelNullTest(SyntaxNode node, ColumnModel column, bool isNull)
    {
        int index = KernelColumn(column, 'n');
        if (index >= 0)
        {
            // The direct forms only see rows whose cells are all present, so the test is a constant there.
            string constant = isNull ? "false" : "true";
            string number = index.ToString(CultureInfo.InvariantCulture);
            _kernelLeaves[node] = (
                $"{(isNull ? string.Empty : "!")}row.IsNull(c{number})",
                constant,
                constant,
                $"builder.IsNull({number}, {(isNull ? "true" : "false")});");
        }
    }

    /// <summary>Records a comparison between a column and parameter <paramref name="parameter"/>, in C#'s null rules.</summary>
    private void KernelComparison(SyntaxNode node, ColumnModel column, string comparison, int parameter)
    {
        bool ordered = comparison is "<" or "<=" or ">" or ">=";
        if (KernelReader(column) is not { } reader
            || Parameters[parameter].KernelType is null
            || (ordered && reader.Kind is 'b' or 'g'))
        {
            return;
        }

        int index = KernelColumn(column, reader.Kind);
        if (index < 0)
        {
            return;
        }

        // Over a RowRef one call tests for NULL and reads the value. A stored NULL differs from every value,
        // and is neither equal to nor ordered against one.
        string number = index.ToString(CultureInfo.InvariantCulture);
        string value = "p" + parameter.ToString(CultureInfo.InvariantCulture);
        string local = "v" + _kernelLeaves.Count.ToString(CultureInfo.InvariantCulture);
        string read = $"row.Try{reader.RowRef}(c{number}, out {Parameters[parameter].KernelType} {local})";
        string op = comparison switch { "=" => "==", "<>" => "!=", _ => comparison };
        string bytes = $"global::DataVo.Core.RowBytes.{reader.Bytes}(row, o{number})";
        string cells = string.Format(CultureInfo.InvariantCulture, reader.Cells, $"cells[c{number}]");
        string test = comparison switch
        {
            "=" => "Equal",
            "<>" => "NotEqual",
            "<" => "Less",
            "<=" => "LessOrEqual",
            ">" => "Greater",
            _ => "GreaterOrEqual",
        };
        string valueType = reader.Kind switch
        {
            'i' => "int",
            'l' => "long",
            'f' => "float",
            'd' => "double",
            'b' => "bool",
            _ => "global::System.Guid",
        };
        _kernelLeaves[node] = (
            comparison == "<>" ? $"(!{read} || {local} != {value})" : $"({read} && {local} {op} {value})",
            $"({bytes} {op} {value})",
            $"({cells} {op} {value})",
            $"builder.Compare({number}, global::DataVo.Core.ColumnarTest.{test}, ({valueType}){value});");
    }

    public string? Condition(ExpressionSyntax expression, bool topLevel)
    {
        switch (expression)
        {
            case ParenthesizedExpressionSyntax parenthesized:
                return Condition(parenthesized.Expression, topLevel) is { } inner ? $"({inner})" : null;

            case BinaryExpressionSyntax binary when binary.IsKind(SyntaxKind.LogicalAndExpression) || binary.IsKind(SyntaxKind.LogicalOrExpression):
            {
                bool and = binary.IsKind(SyntaxKind.LogicalAndExpression);
                string? left = Condition(binary.Left, topLevel && and);
                string? right = Condition(binary.Right, topLevel && and);
                return left is null || right is null ? null : $"{left} {(and ? "AND" : "OR")} {right}";
            }

            case BinaryExpressionSyntax binary when ComparisonOperator(binary) is { } comparison:
                return Comparison(binary, comparison, topLevel);

            case PrefixUnaryExpressionSyntax not when not.IsKind(SyntaxKind.LogicalNotExpression) && AsColumn(not.Operand) is { } negated:
                return BooleanColumn(not, negated.Column, negated.Property, "false");

            case IsPatternExpressionSyntax { Pattern: ConstantPatternSyntax { Expression: LiteralExpressionSyntax literal } } isNull
                when literal.IsKind(SyntaxKind.NullLiteralExpression) && AsColumn(isNull.Expression) is { } tested:
                KernelNullTest(isNull, tested.Column, isNull: true);
                return $"{tested.Column.PropertyName} IS NULL";

            case IsPatternExpressionSyntax { Pattern: UnaryPatternSyntax { Pattern: ConstantPatternSyntax { Expression: LiteralExpressionSyntax literal } } } isNotNull
                when literal.IsKind(SyntaxKind.NullLiteralExpression) && AsColumn(isNotNull.Expression) is { } tested:
                KernelNullTest(isNotNull, tested.Column, isNull: false);
                return $"{tested.Column.PropertyName} IS NOT NULL";

            default:
                return AsColumn(expression) is { } column
                    ? BooleanColumn(expression, column.Column, column.Property, "true")
                    : Fail($"'{Shorten(expression)}' is not a comparison between a column and a value");
        }
    }

    private static string? ComparisonOperator(BinaryExpressionSyntax binary) => binary.Kind() switch
    {
        SyntaxKind.EqualsExpression => "=",
        SyntaxKind.NotEqualsExpression => "<>",
        SyntaxKind.LessThanExpression => "<",
        SyntaxKind.LessThanOrEqualExpression => "<=",
        SyntaxKind.GreaterThanExpression => ">",
        SyntaxKind.GreaterThanOrEqualExpression => ">=",
        _ => null,
    };

    private static string Flip(string comparison) => comparison switch
    {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        _ => comparison,
    };

    private string? Comparison(BinaryExpressionSyntax binary, string comparison, bool topLevel)
    {
        (ColumnModel Column, IPropertySymbol Property)? left = AsColumn(binary.Left);
        (ColumnModel Column, IPropertySymbol Property)? right = AsColumn(binary.Right);
        if (left is null == right is null)
        {
            return Fail($"'{Shorten(binary)}' must compare one column with one value");
        }

        (ColumnModel column, IPropertySymbol property) = (left ?? right)!.Value;
        if (column.Type.IsVector3)
        {
            return Fail($"column {column.PropertyName} is a 3D position, which SQL cannot compare; use WithinRadius, WithinBoundingBox or NearestNeighbors");
        }

        ExpressionSyntax columnSide = left is null ? binary.Right : binary.Left;
        ExpressionSyntax valueSide = left is null ? binary.Left : binary.Right;
        if (left is null)
        {
            comparison = Flip(comparison);
        }

        ITypeSymbol columnType = DataVoLinqAnalyzer.StripNullable(property.Type)!;
        if (!SymbolEqualityComparer.Default.Equals(DataVoLinqAnalyzer.StripNullable(Semantics.GetTypeInfo(columnSide, Cancellation).ConvertedType), columnType))
        {
            return Fail($"'{Shorten(binary)}' converts column {column.PropertyName} to another type; compare it with a value of type {columnType.ToDisplayString()}");
        }

        string? parameter = Value(valueSide, columnType, column);
        if (parameter is null)
        {
            return null;
        }

        string name = column.PropertyName;
        if (parameter == NullLiteral)
        {
            if (comparison is "=" or "<>")
            {
                KernelNullTest(binary, column, isNull: comparison == "=");
            }

            return comparison switch
            {
                "=" => $"{name} IS NULL",
                "<>" => $"{name} IS NOT NULL",
                _ => Fail($"'{Shorten(binary)}' orders a column against null"),
            };
        }

        KernelComparison(binary, column, comparison, Parameters.Count - 1);

        if (comparison == "=" && column.IsPrimaryKey && topLevel)
        {
            Unique = true;
            if (column.Type.SqlType is "INT" or "BIGINT")
            {
                IntegerKeyCondition = $"{name} = {parameter}";
            }
        }

        // SQL's NULL <> value is unknown, while C# counts a null column as different from the value.
        return comparison == "<>" && column.IsNullable
            ? $"({name} <> {parameter} OR {name} IS NULL)"
            : $"{name} {comparison} {parameter}";
    }

    private string? BooleanColumn(SyntaxNode node, ColumnModel column, IPropertySymbol property, string literal)
    {
        if (property.Type.SpecialType != SpecialType.System_Boolean)
        {
            return Fail($"column {column.PropertyName} is not a bool, so it needs a comparison");
        }

        string parameter = Add(new LinqParameter(literal, null, false, "bool"));
        KernelComparison(node, column, "=", Parameters.Count - 1);
        return $"{column.PropertyName} = {parameter}";
    }

    private (ColumnModel Column, IPropertySymbol Property)? AsColumn(ExpressionSyntax expression)
    {
        while (expression is ParenthesizedExpressionSyntax parenthesized)
        {
            expression = parenthesized.Expression;
        }

        if (expression is not MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax receiver } access
            || !IsRow(receiver)
            || Semantics.GetSymbolInfo(access, Cancellation).Symbol is not IPropertySymbol property)
        {
            return null;
        }

        ColumnModel? column = model.Columns.FirstOrDefault(candidate => candidate.PropertyName == property.Name);
        return column is null ? null : (column, property);
    }

    private bool IsRow(IdentifierNameSyntax identifier) =>
        SymbolEqualityComparer.Default.Equals(Semantics.GetSymbolInfo(identifier, Cancellation).Symbol, row);

    /// <summary>Binds a value expression to a new parameter and returns its SQL name.</summary>
    private string? Value(ExpressionSyntax expression, ITypeSymbol columnType, ColumnModel column)
    {
        while (expression is ParenthesizedExpressionSyntax parenthesized)
        {
            expression = parenthesized.Expression;
        }

        if (expression.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>().Any(IsRow))
        {
            return Fail($"'{Shorten(expression)}' computes a value from the row; only a plain column can be compared");
        }

        string cast = columnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        string? kernelType = KernelReader(column) is null ? null : cast;
        Optional<object?> constant = Semantics.GetConstantValue(expression, Cancellation);
        if (constant.HasValue)
        {
            if (constant.Value is null)
            {
                return NullLiteral;
            }

            return Literal(constant.Value, columnType) is { } literal
                ? Add(new LinqParameter(literal, null, column.Type.UsesObjectParameter, kernelType))
                : Fail($"the constant '{Shorten(expression)}' cannot be written as a {columnType.ToDisplayString()}");
        }

        Microsoft.CodeAnalysis.TypeInfo type = Semantics.GetTypeInfo(expression, Cancellation);
        if (!SymbolEqualityComparer.Default.Equals(DataVoLinqAnalyzer.StripNullable(type.ConvertedType), columnType))
        {
            return Fail($"'{Shorten(expression)}' is not a {columnType.ToDisplayString()}, the type of column {column.PropertyName}");
        }

        string? text = ValueText(expression);
        if (text is null)
        {
            return null;
        }

        bool nullableValue = type.Type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T };
        if (nullableValue)
        {
            return Add(new LinqParameter($"({cast})({text}).GetValueOrDefault()", $"({text}).HasValue", column.Type.UsesObjectParameter, kernelType));
        }

        return type.Type is { IsReferenceType: true }
            ? Add(new LinqParameter($"({cast})({text})", $"({text}) != null", column.Type.UsesObjectParameter, kernelType))
            : Add(new LinqParameter($"({cast})({text})", null, column.Type.UsesObjectParameter, kernelType));
    }

    private string Add(LinqParameter parameter)
    {
        Parameters.Add(parameter);
        return "@p" + (Parameters.Count - 1).ToString(CultureInfo.InvariantCulture);
    }
}
