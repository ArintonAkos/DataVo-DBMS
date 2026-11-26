using System.Text;
using DataVo.Generators.Diagnostics;
using DataVo.Generators.Sql;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace DataVo.Generators;

/// <summary>
/// Implements <c>[DataVoQuery("...")] static partial</c> methods as compiled DataVo query plans. Methods are found with
/// <see cref="SyntaxValueProvider.ForAttributeWithMetadataName{T}"/> and reduced to a value-equatable
/// <see cref="QueryMethodModel"/>, so the incremental pipeline re-emits only methods that changed.
/// </summary>
[Generator]
public sealed class DataVoQueryGenerator : IIncrementalGenerator
{
    private const string QueryAttribute = "DataVo.Core.CompiledQueries.DataVoQueryAttribute";
    private const string Plan = "global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan";
    private const string Query = "global::DataVo.Core.CompiledQueries.DataVoCompiledQuery";
    private const string Parameter = "global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter";
    private const string AccessPath = "global::DataVo.Core.CompiledQueries.CompiledAccessPath";

    private static readonly SymbolDisplayFormat FullyQualifiedNullableFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions |
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    private static readonly DiagnosticDescriptor[] Descriptors =
    [
        DataVoGeneratorDiagnostics.UnsupportedSql,
        DataVoGeneratorDiagnostics.MissingParameter,
        DataVoGeneratorDiagnostics.InvalidQueryMethod,
        DataVoGeneratorDiagnostics.RowTypeMismatch,
    ];

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValuesProvider<QueryMethodInspection> methods = context.SyntaxProvider.ForAttributeWithMetadataName(
            QueryAttribute,
            static (node, _) => node is MethodDeclarationSyntax,
            static (attributeContext, ct) => Inspect(attributeContext, ct));

        // Compile-time schema catalog built from AdditionalFiles flagged DataVoSchemaManifest="true".
        IncrementalValueProvider<CompileTimeCatalog> catalog = context.AdditionalTextsProvider
            .Combine(context.AnalyzerConfigOptionsProvider)
            .Where(static pair => IsSchemaManifest(pair.Left, pair.Right))
            .Select(static (pair, ct) => pair.Left.GetText(ct)?.ToString() ?? string.Empty)
            .Collect()
            .Select(static (texts, _) => DataVoDdlManifestParser.Parse(texts));

        context.RegisterSourceOutput(methods.Combine(catalog), static (output, pair) =>
        {
            QueryMethodInspection inspection = pair.Left;
            if (inspection.Diagnostic is { } diagnostic)
            {
                output.ReportDiagnostic(ToDiagnostic(diagnostic));
            }

            if (inspection.Model is { } model)
            {
                output.AddSource(model.HintName, Emit(model, pair.Right));
            }
        });
    }

    private static bool IsSchemaManifest(AdditionalText text, AnalyzerConfigOptionsProvider optionsProvider)
    {
        return optionsProvider.GetOptions(text)
                   .TryGetValue("build_metadata.AdditionalFiles.DataVoSchemaManifest", out string? value)
               && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    // ------------------------------------------------------------------ inspection (symbols -> model)

    private static QueryMethodInspection Inspect(GeneratorAttributeSyntaxContext context, CancellationToken cancellationToken)
    {
        if (context.TargetSymbol is not IMethodSymbol method || context.TargetNode is not MethodDeclarationSyntax syntax)
        {
            return new QueryMethodInspection(null, null);
        }

        Location location = syntax.Identifier.GetLocation();
        AttributeData attribute = context.Attributes[0];
        string sql = attribute.ConstructorArguments.Length == 1
            ? attribute.ConstructorArguments[0].Value?.ToString() ?? string.Empty
            : string.Empty;

        if (!DataVoQueryShapeParser.TryParse(sql, out GeneratedQueryModel? query) || query is null)
        {
            return Fail(DataVoGeneratorDiagnostics.UnsupportedSql, location, sql);
        }

        if (ValidateSignature(method) is { } signatureProblem)
        {
            return Fail(DataVoGeneratorDiagnostics.InvalidQueryMethod, location, method.Name, signatureProblem);
        }

        string[] sqlParameters = GetSqlParameters(query);
        var methodArguments = new string[sqlParameters.Length];
        for (int i = 0; i < sqlParameters.Length; i++)
        {
            IParameterSymbol? bound = method.Parameters.Skip(1)
                .FirstOrDefault(parameter => string.Equals(parameter.Name, sqlParameters[i], StringComparison.OrdinalIgnoreCase));
            if (bound is null)
            {
                return Fail(DataVoGeneratorDiagnostics.MissingParameter, location, sqlParameters[i]);
            }

            methodArguments[i] = EscapeIdentifier(bound.Name);
        }

        if (ResolveShape(method, query, out string? shapeProblem) is not { } shape)
        {
            return Fail(DataVoGeneratorDiagnostics.InvalidQueryMethod, location, method.Name, shapeProblem!);
        }

        cancellationToken.ThrowIfCancellationRequested();
        string? rowType = null;
        string[] typedGetters = [];
        string[] dictionaryArguments = [];
        if (shape is QueryExecutionShape.SelectSingle or QueryExecutionShape.SelectMany)
        {
            var row = (INamedTypeSymbol)GetSelectRowType(method)!;
            if (!TryMapConstructor(row, query.ProjectedColumns, out IParameterSymbol[] arguments, out string[] columns, out string? mismatch))
            {
                return Fail(DataVoGeneratorDiagnostics.RowTypeMismatch, location, row.ToDisplayString(), mismatch!);
            }

            rowType = row.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            dictionaryArguments = [.. arguments.Select((argument, i) =>
                $"({argument.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)})row[\"{columns[i]}\"]!")];
            string?[] getters = [.. arguments.Select((argument, i) => TypedGetter(argument.Type, columns[i]))];
            typedGetters = getters.All(static getter => getter is not null) ? [.. getters.Select(static getter => getter!)] : [];
        }

        var model = new QueryMethodModel(
            HintName(method),
            method.ContainingNamespace.IsGlobalNamespace ? null : method.ContainingNamespace.ToDisplayString(),
            Equatable(ContainingTypeDeclarations(method.ContainingType)),
            MethodSignature(method),
            MemberSuffix(method),
            shape,
            query.TableName,
            Equatable(query.ProjectedColumns),
            query.WhereColumn,
            query.WhereParameterName,
            Equatable(query.InsertColumns),
            Equatable(query.InsertParameterNames),
            Equatable([.. query.Assignments.Keys]),
            Equatable([.. query.Assignments.Values]),
            EscapeIdentifier(method.Parameters[0].Name),
            Equatable(sqlParameters),
            Equatable(methodArguments),
            rowType,
            Equatable(typedGetters),
            Equatable(dictionaryArguments));
        return new QueryMethodInspection(model, null);
    }

    /// <summary>The reason the method cannot be implemented, or null when its declaration is usable.</summary>
    private static string? ValidateSignature(IMethodSymbol method)
    {
        if (!method.IsStatic || !method.IsPartialDefinition)
        {
            return "it must be a static partial method declaration without a body";
        }

        if (method.IsGenericMethod)
        {
            return "it cannot be generic";
        }

        for (INamedTypeSymbol? type = method.ContainingType; type is not null; type = type.ContainingType)
        {
            if (type.IsGenericType)
            {
                return $"its containing type '{type.Name}' cannot be generic";
            }

            bool isPartial = type.DeclaringSyntaxReferences
                .Select(static reference => reference.GetSyntax())
                .OfType<TypeDeclarationSyntax>()
                .Any(static declaration => declaration.Modifiers.Any(SyntaxKind.PartialKeyword));
            if (!isPartial)
            {
                return $"its containing type '{type.Name}' must be partial";
            }
        }

        if (!method.ContainingType.IsStatic)
        {
            return $"its containing type '{method.ContainingType.Name}' must be static";
        }

        if (method.Parameters.Length == 0 || method.Parameters[0].Type.ToDisplayString() != "DataVo.Core.DataVoContext")
        {
            return "its first parameter must be a DataVoContext";
        }

        return method.Parameters.Any(static parameter => parameter.RefKind != RefKind.None || parameter.IsParams)
            ? "its parameters cannot be ref, out, in or params"
            : null;
    }

    private static QueryExecutionShape? ResolveShape(IMethodSymbol method, GeneratedQueryModel query, out string? problem)
    {
        problem = null;
        switch (query.Kind)
        {
            case "Insert":
                if (IsReadOnlyListOfRowKey(method.ReturnType))
                {
                    return QueryExecutionShape.Insert;
                }

                problem = "an INSERT method must return IReadOnlyList<DataVo.Core.RowKey>";
                return null;
            case "Update":
                if (method.ReturnType.SpecialType == SpecialType.System_Int32)
                {
                    return QueryExecutionShape.Update;
                }

                problem = "an UPDATE method must return int (the number of rows changed)";
                return null;
            default:
                if (GetSelectRowType(method) is { } rowType && IsSupportedSelectRowType(rowType))
                {
                    return IsListLike(method.ReturnType) ? QueryExecutionShape.SelectMany : QueryExecutionShape.SelectSingle;
                }

                problem = "a SELECT method must return a class, record or struct row type (nullable for one row), or a List<T>/IReadOnlyList<T> of it";
                return null;
        }
    }

    /// <summary>
    /// Matches the row type's constructor to the projected columns: by parameter name when every name matches a column,
    /// otherwise by position when exactly one constructor takes as many parameters as there are columns.
    /// </summary>
    private static bool TryMapConstructor(
        INamedTypeSymbol rowType,
        string[] projected,
        out IParameterSymbol[] arguments,
        out string[] columns,
        out string? mismatch)
    {
        arguments = [];
        columns = [];
        mismatch = null;
        IMethodSymbol[] candidates = [.. rowType.InstanceConstructors
            .Where(ctor => !ctor.IsImplicitlyDeclared && ctor.Parameters.Length == projected.Length && ctor.DeclaredAccessibility != Microsoft.CodeAnalysis.Accessibility.Private)];

        IMethodSymbol? byName = candidates.FirstOrDefault(ctor => ctor.Parameters.All(parameter =>
            projected.Any(column => string.Equals(column, parameter.Name, StringComparison.OrdinalIgnoreCase))));
        if (byName is not null)
        {
            arguments = [.. byName.Parameters];
            columns = [.. byName.Parameters.Select(parameter =>
                projected.First(column => string.Equals(column, parameter.Name, StringComparison.OrdinalIgnoreCase)))];
            return true;
        }

        if (candidates.Length == 1)
        {
            arguments = [.. candidates[0].Parameters];
            columns = projected;
            return true;
        }

        mismatch = candidates.Length == 0
            ? $"it needs a constructor with {projected.Length} parameter(s), one per selected column ({string.Join(", ", projected)})"
            : $"several constructors take {projected.Length} parameters and none is matched by name to the selected columns";
        return false;
    }

    private static string? TypedGetter(ITypeSymbol type, string column)
    {
        if (type is INamedTypeSymbol nullable &&
            nullable.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
            nullable.TypeArguments.Length == 1)
        {
            string? inner = ValueGetterName(nullable.TypeArguments[0]);
            return inner is null ? null : $"reader.{inner}OrNull(\"{column}\")";
        }

        string? valueGetter = ValueGetterName(type);
        if (valueGetter is not null)
        {
            return $"reader.{valueGetter}(\"{column}\")";
        }

        if (type.SpecialType == SpecialType.System_String)
        {
            return type.NullableAnnotation == NullableAnnotation.Annotated
                ? $"reader.GetString(\"{column}\")"
                : $"reader.GetString(\"{column}\")!";
        }

        if (type is IArrayTypeSymbol array && array.ElementType.SpecialType == SpecialType.System_Single)
        {
            return $"reader.GetVector(\"{column}\")";
        }

        return null;
    }

    private static string? ValueGetterName(ITypeSymbol type) => type.SpecialType switch
    {
        SpecialType.System_Int32 => "GetInt32",
        SpecialType.System_Int64 => "GetInt64",
        SpecialType.System_Int16 => "GetInt16",
        SpecialType.System_Byte => "GetByte",
        SpecialType.System_Single => "GetSingle",
        SpecialType.System_Double => "GetDouble",
        SpecialType.System_Decimal => "GetDecimal",
        SpecialType.System_Boolean => "GetBoolean",
        _ => type.ToDisplayString() switch
        {
            "DataVo.Core.DataVoDate" => "GetDate",
            "System.DateOnly" => "GetDateOnly",
            "System.DateTime" => "GetDateTime",
            "System.DateTimeOffset" => "GetDateTimeOffset",
            "System.Guid" => "GetGuid",
            "System.Numerics.Vector3" => "GetVector3",
            "System.ReadOnlyMemory<float>" => "GetVectorMemory",
            _ => null,
        },
    };

    private static string[] GetSqlParameters(GeneratedQueryModel query) => query.Kind switch
    {
        "Insert" => query.InsertParameterNames,
        "Update" => [.. query.Assignments.Values.Concat([query.WhereParameterName!]).Distinct(StringComparer.OrdinalIgnoreCase)],
        _ => [query.WhereParameterName!],
    };

    private static ITypeSymbol? GetSelectRowType(IMethodSymbol method)
    {
        if (IsListLike(method.ReturnType) && method.ReturnType is INamedTypeSymbol listType)
        {
            return UnwrapNullableValueType(listType.TypeArguments[0]);
        }

        return UnwrapNullableValueType(method.ReturnType);
    }

    private static ITypeSymbol UnwrapNullableValueType(ITypeSymbol type) =>
        type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T, TypeArguments.Length: 1 } named
            ? named.TypeArguments[0]
            : type;

    private static bool IsSupportedSelectRowType(ITypeSymbol rowType) =>
        rowType is INamedTypeSymbol { TypeKind: TypeKind.Class or TypeKind.Struct, SpecialType: SpecialType.None, IsAnonymousType: false, IsTupleType: false, IsAbstract: false };

    private static bool IsListLike(ITypeSymbol returnType) =>
        returnType is INamedTypeSymbol { TypeArguments.Length: 1 } named &&
        named.Name is "List" or "IReadOnlyList" &&
        named.ContainingNamespace.ToDisplayString() == "System.Collections.Generic";

    // Insert methods return the inserted rows' keys (DataVo.Core.RowKey: an integer or a GUID primary key).
    private static bool IsReadOnlyListOfRowKey(ITypeSymbol returnType) =>
        returnType is INamedTypeSymbol { Name: "IReadOnlyList", TypeArguments.Length: 1 } named &&
        named.ContainingNamespace.ToDisplayString() == "System.Collections.Generic" &&
        named.TypeArguments[0].ToDisplayString() == "DataVo.Core.RowKey";

    /// <summary>The implementing declaration, keeping the method's declared accessibility.</summary>
    private static string MethodSignature(IMethodSymbol method)
    {
        string parameters = string.Join(", ", method.Parameters.Select(parameter =>
            $"{parameter.Type.ToDisplayString(FullyQualifiedNullableFormat)} {EscapeIdentifier(parameter.Name)}"));
        return $"{AccessibilityModifier(method.DeclaredAccessibility)}static partial {method.ReturnType.ToDisplayString(FullyQualifiedNullableFormat)} {EscapeIdentifier(method.Name)}({parameters})";
    }

    /// <summary>The declarations of the containing types, outermost first, each with its accessibility and kind.</summary>
    private static string[] ContainingTypeDeclarations(INamedTypeSymbol innermost)
    {
        var declarations = new List<string>();
        for (INamedTypeSymbol? type = innermost; type is not null; type = type.ContainingType)
        {
            string kind = type.IsRecord
                ? type.TypeKind == TypeKind.Struct ? "record struct" : "record"
                : type.TypeKind == TypeKind.Struct ? "struct" : type.TypeKind == TypeKind.Interface ? "interface" : "class";
            string staticModifier = type.IsStatic ? "static " : string.Empty;
            declarations.Insert(0, $"{AccessibilityModifier(type.DeclaredAccessibility)}{staticModifier}partial {kind} {EscapeIdentifier(type.Name)}");
        }

        return [.. declarations];
    }

    private static string AccessibilityModifier(Accessibility accessibility) => accessibility switch
    {
        Microsoft.CodeAnalysis.Accessibility.Public => "public ",
        Microsoft.CodeAnalysis.Accessibility.Internal => "internal ",
        Microsoft.CodeAnalysis.Accessibility.Private => "private ",
        Microsoft.CodeAnalysis.Accessibility.Protected => "protected ",
        Microsoft.CodeAnalysis.Accessibility.ProtectedAndInternal => "private protected ",
        Microsoft.CodeAnalysis.Accessibility.ProtectedOrInternal => "protected internal ",
        _ => string.Empty,
    };

    /// <summary>
    /// A hint name unique per method: the namespace, every containing type and the method name, plus a hash of the
    /// full signature so overloads (and names that sanitize alike) never collide.
    /// </summary>
    private static string HintName(IMethodSymbol method)
    {
        string signature = method.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) + "(" +
            string.Join(",", method.Parameters.Select(static parameter => parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat))) + ")";
        string readable = method.ContainingType.ToDisplayString() + "." + method.Name;
        var sanitized = new StringBuilder(readable.Length);
        foreach (char c in readable)
        {
            sanitized.Append(char.IsLetterOrDigit(c) || c is '.' or '_' ? c : '_');
        }

        return $"{sanitized}_{StableHash(signature):x8}.DataVo.g.cs";
    }

    /// <summary>A suffix for generated helper members, distinct per overload of the same method name.</summary>
    private static string MemberSuffix(IMethodSymbol method) =>
        $"{method.Name}_{StableHash(string.Join(",", method.Parameters.Select(static parameter => parameter.Type.ToDisplayString()))):x8}";

    // FNV-1a: string.GetHashCode is randomized per process, and hint names must be stable between builds.
    private static uint StableHash(string text)
    {
        uint hash = 2166136261;
        foreach (char c in text)
        {
            hash = unchecked((hash ^ c) * 16777619);
        }

        return hash;
    }

    /// <summary>Prefixes a C# keyword used as an identifier (<c>@class</c>) so the emitted code compiles.</summary>
    private static string EscapeIdentifier(string name) =>
        SyntaxFacts.GetKeywordKind(name) != SyntaxKind.None ? "@" + name : name;

    private static EquatableArray<string> Equatable(IEnumerable<string> values) => new([.. values]);

    private static QueryMethodInspection Fail(DiagnosticDescriptor descriptor, Location location, params string[] arguments)
    {
        FileLinePositionSpan lines = location.GetLineSpan();
        return new QueryMethodInspection(
            null,
            new DiagnosticInfo(descriptor.Id, lines.Path ?? string.Empty, location.SourceSpan, lines.Span, new EquatableArray<string>([.. arguments])));
    }

    private static Diagnostic ToDiagnostic(DiagnosticInfo info)
    {
        DiagnosticDescriptor descriptor = Descriptors.First(candidate => candidate.Id == info.DescriptorId);
        Location location = string.IsNullOrEmpty(info.FilePath) ? Location.None : Location.Create(info.FilePath, info.Span, info.LineSpan);
        return Diagnostic.Create(descriptor, location, info.Arguments.Cast<object?>().ToArray());
    }

    // ------------------------------------------------------------------ emission (model -> source)

    private static string Emit(QueryMethodModel model, CompileTimeCatalog catalog)
    {
        string planName = $"__DataVoPlan_{model.MemberSuffix}";
        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated />");
        builder.AppendLine("#nullable enable");
        if (model.Namespace is not null)
        {
            builder.AppendLine($"namespace {model.Namespace};");
        }

        string indent = string.Empty;
        foreach (string declaration in model.ContainingTypes)
        {
            builder.AppendLine($"{indent}{declaration}");
            builder.AppendLine($"{indent}{{");
            indent += "    ";
        }

        builder.AppendLine($"{indent}[global::System.CodeDom.Compiler.GeneratedCode(\"DataVo.Generators\", \"{GeneratorInfo.Version}\")]");
        builder.AppendLine($"{indent}private static readonly {Plan} {planName} = {EmitPlan(model, catalog)};");

        string invocation;
        if (model.HasTypedGetters)
        {
            string mapName = $"__DataVoMap_{model.MemberSuffix}";
            string mapperName = $"__DataVoMapper_{model.MemberSuffix}";
            builder.AppendLine($"{indent}private static {model.RowType} {mapName}(global::DataVo.Core.CompiledQueries.CompiledRowReader reader) => new {model.RowType}({string.Join(", ", model.TypedGetters)});");
            builder.AppendLine($"{indent}private static readonly global::DataVo.Core.CompiledQueries.CompiledRowMapper<{model.RowType}> {mapperName} = {mapName};");
            string typedMethod = model.Shape == QueryExecutionShape.SelectMany ? "SelectManyTyped" : "SelectSingleTyped";
            invocation = $"{Query}.{typedMethod}<{model.RowType}>({model.DbParameter}, {planName}, {EmitParameters(model)}, {mapperName})";
        }
        else
        {
            invocation = model.Shape switch
            {
                QueryExecutionShape.SelectSingle or QueryExecutionShape.SelectMany =>
                    $"{Query}.{model.Shape}<{model.RowType}>({model.DbParameter}, {planName}, {EmitParameters(model)}, static row => new {model.RowType}({string.Join(", ", model.DictionaryArguments)}))",
                QueryExecutionShape.Insert => $"{Query}.Insert({model.DbParameter}, {planName}, {EmitParameters(model)})",
                _ => $"{Query}.Update({model.DbParameter}, {planName}, {EmitParameters(model)})",
            };
        }

        builder.AppendLine($"{indent}[global::System.CodeDom.Compiler.GeneratedCode(\"DataVo.Generators\", \"{GeneratorInfo.Version}\")]");
        builder.AppendLine($"{indent}{model.MethodSignature}");
        builder.AppendLine($"{indent}{{");
        builder.AppendLine($"{indent}    return {invocation};");
        builder.AppendLine($"{indent}}}");

        for (int depth = model.ContainingTypes.Count - 1; depth >= 0; depth--)
        {
            builder.AppendLine($"{new string(' ', depth * 4)}}}");
        }

        return builder.ToString();
    }

    private static string EmitParameters(QueryMethodModel model)
    {
        string[] sql = [.. model.SqlParameters];
        string[] arguments = [.. model.MethodArguments];
        return $"new {Parameter}[] {{ {string.Join(", ", sql.Select((name, i) => $"new {Parameter}(\"{name}\", {arguments[i]})"))} }}";
    }

    private static string EmitPlan(QueryMethodModel model, CompileTimeCatalog catalog) => model.Shape switch
    {
        QueryExecutionShape.SelectSingle => EmitSelectPlan("SelectSingle", model, catalog),
        QueryExecutionShape.SelectMany => EmitSelectPlan("SelectMany", model, catalog),
        QueryExecutionShape.Insert =>
            $"{Plan}.Insert(\"{model.TableName}\", new string[] {{ {QuoteList(model.InsertColumns)} }}, new string[] {{ {QuoteList(model.InsertParameterNames)} }})",
        _ =>
            $"{Plan}.Update(\"{model.TableName}\", new global::System.Collections.Generic.Dictionary<string, string>(global::System.StringComparer.OrdinalIgnoreCase) {{ {AssignmentList(model)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\")",
    };

    // Shared by SelectSingle and SelectMany: a manifest primary key or single-column index pre-resolves the access
    // path; the runtime falls back to its own resolution when the bet is wrong.
    private static string EmitSelectPlan(string factoryName, QueryMethodModel model, CompileTimeCatalog catalog)
    {
        string baseArguments =
            $"\"{model.TableName}\", new string[] {{ {QuoteList(model.ProjectedColumns)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\"";

        if (catalog.IsPrimaryKey(model.TableName, model.WhereColumn!))
        {
            return $"{Plan}.{factoryName}({baseArguments}, accessPath: {AccessPath}.PrimaryKey)";
        }

        if (catalog.TryResolveSingleColumnIndex(model.TableName, model.WhereColumn!, out string indexName))
        {
            return $"{Plan}.{factoryName}({baseArguments}, accessPath: {AccessPath}.SingleColumnIndex, resolvedIndexName: \"{indexName}\")";
        }

        return $"{Plan}.{factoryName}({baseArguments})";
    }

    private static string QuoteList(IEnumerable<string> values) => string.Join(", ", values.Select(static value => $"\"{value}\""));

    private static string AssignmentList(QueryMethodModel model) =>
        string.Join(", ", model.AssignmentColumns.Zip(model.AssignmentParameters, static (column, parameter) => $"[\"{column}\"] = \"{parameter}\""));
}
