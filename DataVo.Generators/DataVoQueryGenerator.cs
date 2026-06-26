using System.Collections.Immutable;
using System.Text;
using DataVo.Generators.Diagnostics;
using DataVo.Generators.Sql;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace DataVo.Generators;

[Generator]
public sealed class DataVoQueryGenerator : IIncrementalGenerator
{
    private static readonly SymbolDisplayFormat FullyQualifiedNullableFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions |
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValuesProvider<MethodDeclarationSyntax> methods = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is MethodDeclarationSyntax method && method.AttributeLists.Count > 0,
                static (ctx, _) => (MethodDeclarationSyntax)ctx.Node)
            .Where(static method => method.Modifiers.Any(SyntaxKind.PartialKeyword));

        IncrementalValueProvider<Compilation> compilation = context.CompilationProvider;

        // Compile-time schema catalog built from AdditionalFiles flagged DataVoSchemaManifest="true".
        IncrementalValueProvider<CompileTimeCatalog> catalog = context.AdditionalTextsProvider
            .Combine(context.AnalyzerConfigOptionsProvider)
            .Where(static pair => IsSchemaManifest(pair.Left, pair.Right))
            .Select(static (pair, ct) => pair.Left.GetText(ct)?.ToString() ?? string.Empty)
            .Collect()
            .Select(static (texts, _) => DataVoDdlManifestParser.Parse(texts));

        context.RegisterSourceOutput(
            methods.Combine(compilation).Combine(catalog),
            static (spc, pair) => EmitForMethod(spc, pair.Left.Left, pair.Left.Right, pair.Right));
    }

    private static bool IsSchemaManifest(AdditionalText text, AnalyzerConfigOptionsProvider optionsProvider)
    {
        return optionsProvider.GetOptions(text)
                   .TryGetValue("build_metadata.AdditionalFiles.DataVoSchemaManifest", out string? value)
               && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    private static void EmitForMethod(SourceProductionContext context, MethodDeclarationSyntax method, Compilation compilation, CompileTimeCatalog catalog)
    {
        SemanticModel semanticModel = compilation.GetSemanticModel(method.SyntaxTree);
        if (semanticModel.GetDeclaredSymbol(method) is not IMethodSymbol symbol)
        {
            return;
        }

        AttributeData? attribute = symbol.GetAttributes()
            .FirstOrDefault(static attr => attr.AttributeClass?.ToDisplayString() == "DataVo.Core.CompiledQueries.DataVoQueryAttribute");
        if (attribute is null)
        {
            return;
        }

        string sql = attribute.ConstructorArguments.Length == 1
            ? attribute.ConstructorArguments[0].Value?.ToString() ?? string.Empty
            : string.Empty;

        if (!DataVoQueryShapeParser.TryParse(sql, out GeneratedQueryModel? model) || model is null)
        {
            context.ReportDiagnostic(Diagnostic.Create(DataVoGeneratorDiagnostics.UnsupportedSql, method.Identifier.GetLocation(), sql));
            return;
        }

        string[] sqlParameters = GetSqlParameters(model);
        var methodParameters = new HashSet<string>(
            symbol.Parameters
                .Skip(1)
                .Select(static parameter => parameter.Name),
            StringComparer.OrdinalIgnoreCase);

        foreach (string sqlParameter in sqlParameters)
        {
            if (!methodParameters.Contains(sqlParameter))
            {
                context.ReportDiagnostic(Diagnostic.Create(DataVoGeneratorDiagnostics.MissingParameter, method.Identifier.GetLocation(), sqlParameter));
                return;
            }
        }

        if (!SupportsMethodShape(symbol, model))
        {
            context.ReportDiagnostic(Diagnostic.Create(DataVoGeneratorDiagnostics.UnsupportedSql, method.Identifier.GetLocation(), sql));
            return;
        }

        string source = GenerateMethod(symbol, model, catalog);
        context.AddSource($"{symbol.ContainingType.Name}_{symbol.Name}.DataVo.g.cs", source);
    }

    private static bool SupportsMethodShape(IMethodSymbol method, GeneratedQueryModel model)
    {
        if (!method.IsStatic || !method.IsPartialDefinition ||
            method.ContainingType is not { IsStatic: true } containingType ||
            !containingType.DeclaringSyntaxReferences
                .Select(static reference => reference.GetSyntax())
                .OfType<TypeDeclarationSyntax>()
                .Any(static declaration => declaration.Modifiers.Any(SyntaxKind.PartialKeyword)))
        {
            return false;
        }

        if (method.Parameters.Length == 0 ||
            method.Parameters[0].Type.ToDisplayString() != "DataVo.Core.DataVoContext")
        {
            return false;
        }

        return ResolveExecutionShape(method, model) != GeneratedExecutionShape.Unsupported;
    }

    private static string GenerateMethod(IMethodSymbol method, GeneratedQueryModel model, CompileTimeCatalog catalog)
    {
        string namespaceDeclaration = method.ContainingNamespace.IsGlobalNamespace
            ? string.Empty
            : $"namespace {method.ContainingNamespace.ToDisplayString()};";
        string containingType = GetContainingTypeDeclaration(method.ContainingType);
        string returnType = method.ReturnType.ToDisplayString(FullyQualifiedNullableFormat);
        string parameterList = string.Join(
            ", ",
            method.Parameters.Select(parameter => $"{parameter.Type.ToDisplayString(FullyQualifiedNullableFormat)} {parameter.Name}"));
        string planName = $"__DataVoPlan_{method.Name}";

        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated />");
        builder.AppendLine("#nullable enable");
        if (namespaceDeclaration.Length > 0)
        {
            builder.AppendLine(namespaceDeclaration);
        }

        GeneratedExecutionShape shape = ResolveExecutionShape(method, model);
        ITypeSymbol? rowType = shape is GeneratedExecutionShape.SelectSingle or GeneratedExecutionShape.SelectMany
            ? GetSelectRowType(method)
            : null;
        string[]? typedGetters = rowType is null ? null : TryBuildTypedGetters(rowType, model.ProjectedColumns);

        builder.AppendLine(containingType);
        builder.AppendLine("{");
        builder.AppendLine($"    private static readonly global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan {planName} = {GeneratePlan(method, model, catalog)};");

        string invocation;
        if (typedGetters is not null && rowType is not null)
        {
            string rowTypeName = rowType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            string mapName = $"__DataVoMap_{method.Name}";
            string mapperFieldName = $"__DataVoMapper_{method.Name}";
            builder.AppendLine($"    private static {rowTypeName} {mapName}(global::DataVo.Core.CompiledQueries.CompiledRowReader reader) => new {rowTypeName}({string.Join(", ", typedGetters)});");
            builder.AppendLine($"    private static readonly global::DataVo.Core.CompiledQueries.CompiledRowMapper<{rowTypeName}> {mapperFieldName} = {mapName};");
            invocation = GenerateTypedInvocation(method, model, planName, shape, rowTypeName, mapperFieldName);
        }
        else
        {
            invocation = GenerateInvocation(method, model, planName);
        }

        builder.AppendLine($"    public static partial {returnType} {method.Name}({parameterList})");
        builder.AppendLine("    {");
        builder.AppendLine($"        return {invocation};");
        builder.AppendLine("    }");
        builder.AppendLine("}");
        return builder.ToString();
    }

    private static string GeneratePlan(IMethodSymbol method, GeneratedQueryModel model, CompileTimeCatalog catalog)
    {
        GeneratedExecutionShape executionShape = ResolveExecutionShape(method, model);

        return executionShape switch
        {
            GeneratedExecutionShape.SelectSingle => GenerateSelectPlan("SelectSingle", model, catalog),
            GeneratedExecutionShape.SelectMany => GenerateSelectPlan("SelectMany", model, catalog),
            GeneratedExecutionShape.Insert => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.Insert(\"{model.TableName}\", new string[] {{ {QuoteList(model.InsertColumns)} }}, new string[] {{ {QuoteList(model.InsertParameterNames)} }})",
            GeneratedExecutionShape.Update => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.Update(\"{model.TableName}\", new global::System.Collections.Generic.Dictionary<string, string>(global::System.StringComparer.OrdinalIgnoreCase) {{ {AssignmentList(model.Assignments)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\")",
            _ => throw new InvalidOperationException($"Unsupported query kind '{model.Kind}'.")
        };
    }

    // Shared by the SelectSingle and SelectMany shapes: both honor the same compile-time tag and share the
    // runtime path (DataVoCompiledQuery.ExecuteSelect -> TryReadMatchingRowEntries).
    private static string GenerateSelectPlan(string factoryName, GeneratedQueryModel model, CompileTimeCatalog catalog)
    {
        string baseArguments =
            $"\"{model.TableName}\", new string[] {{ {QuoteList(model.ProjectedColumns)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\"";

        if (catalog.TryResolveSingleColumnIndex(model.TableName, model.WhereColumn!, out string indexName))
        {
            return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.{factoryName}({baseArguments}, accessPath: global::DataVo.Core.CompiledQueries.CompiledAccessPath.SingleColumnIndex, resolvedIndexName: \"{indexName}\")";
        }

        return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.{factoryName}({baseArguments})";
    }

    private static string GenerateInvocation(IMethodSymbol method, GeneratedQueryModel model, string planName)
    {
        string dbParameter = method.Parameters[0].Name;
        ITypeSymbol? selectRowType = GetSelectRowType(method);
        string parameters = string.Join(
            ", ",
            GetSqlParameters(model).Select(static name => name)
                .Select(name => $"new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter(\"{name}\", {FindMethodParameterName(method, name)})"));

        return ResolveExecutionShape(method, model) switch
        {
            GeneratedExecutionShape.SelectSingle => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.SelectSingle<{selectRowType!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}>({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }}, static row => new {selectRowType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}({MapperArguments(selectRowType, model.ProjectedColumns)}))",
            GeneratedExecutionShape.SelectMany => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.SelectMany<{selectRowType!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}>({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }}, static row => new {selectRowType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}({MapperArguments(selectRowType, model.ProjectedColumns)}))",
            GeneratedExecutionShape.Insert => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.Insert({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }})",
            GeneratedExecutionShape.Update => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.Update({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }})",
            _ => throw new InvalidOperationException($"Unsupported query kind '{model.Kind}'.")
        };
    }

    private static string MapperArguments(ITypeSymbol rowType, string[] columns)
    {
        if (rowType is INamedTypeSymbol namedType)
        {
            IMethodSymbol? constructor = namedType.InstanceConstructors
                .Where(static ctor => !ctor.IsImplicitlyDeclared)
                .OrderByDescending(static ctor => ctor.Parameters.Length)
                .FirstOrDefault();

            if (constructor is not null &&
                constructor.Parameters.Length == columns.Length &&
                constructor.Parameters.All(parameter => columns.Any(column => string.Equals(column, parameter.Name, StringComparison.OrdinalIgnoreCase))))
            {
                return string.Join(
                    ", ",
                    constructor.Parameters.Select(parameter =>
                    {
                        string column = columns.First(candidate => string.Equals(candidate, parameter.Name, StringComparison.OrdinalIgnoreCase));
                        return $"({parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)})row[\"{column}\"]!";
                    }));
            }
        }

        return string.Join(", ", columns.Select(column => $"({InferCastType(column)})row[\"{column}\"]!"));
    }

    // Builds the per-column typed getter calls when the projection is a clean ctor-name match with supported
    // param types; returns null to signal "fall back to the dictionary mapper".
    private static string[]? TryBuildTypedGetters(ITypeSymbol rowType, string[] columns)
    {
        if (rowType is not INamedTypeSymbol named)
        {
            return null;
        }

        IMethodSymbol? constructor = named.InstanceConstructors
            .Where(static ctor => !ctor.IsImplicitlyDeclared)
            .OrderByDescending(static ctor => ctor.Parameters.Length)
            .FirstOrDefault();

        if (constructor is null ||
            constructor.Parameters.Length != columns.Length ||
            !constructor.Parameters.All(parameter => columns.Any(column => string.Equals(column, parameter.Name, StringComparison.OrdinalIgnoreCase))))
        {
            return null;
        }

        var getters = new string[constructor.Parameters.Length];
        for (int i = 0; i < constructor.Parameters.Length; i++)
        {
            IParameterSymbol parameter = constructor.Parameters[i];
            string column = columns.First(candidate => string.Equals(candidate, parameter.Name, StringComparison.OrdinalIgnoreCase));
            string? getter = TypedGetter(parameter.Type, column);
            if (getter is null)
            {
                return null;
            }

            getters[i] = getter;
        }

        return getters;
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
        SpecialType.System_Double => "GetDouble",
        SpecialType.System_Decimal => "GetDecimal",
        SpecialType.System_Boolean => "GetBoolean",
        _ => type.ToDisplayString() == "System.DateOnly" ? "GetDate" : null,
    };

    private static string GenerateTypedInvocation(
        IMethodSymbol method,
        GeneratedQueryModel model,
        string planName,
        GeneratedExecutionShape shape,
        string rowTypeName,
        string mapperFieldName)
    {
        string dbParameter = method.Parameters[0].Name;
        string parameters = string.Join(
            ", ",
            GetSqlParameters(model).Select(name => $"new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter(\"{name}\", {FindMethodParameterName(method, name)})"));
        string typedMethod = shape == GeneratedExecutionShape.SelectMany ? "SelectManyTyped" : "SelectSingleTyped";

        return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.{typedMethod}<{rowTypeName}>({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }}, {mapperFieldName})";
    }

    private static string InferCastType(string column)
    {
        return column.EndsWith("Id", StringComparison.OrdinalIgnoreCase) ||
               column.Equals("Level", StringComparison.OrdinalIgnoreCase) ||
               column.Equals("Frame", StringComparison.OrdinalIgnoreCase)
            ? "int"
            : "string";
    }

    private static string[] GetSqlParameters(GeneratedQueryModel model)
    {
        return model.Kind switch
        {
            "Insert" => model.InsertParameterNames,
            "Update" => model.Assignments.Values.Concat(new[] { model.WhereParameterName! }).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
            _ => new[] { model.WhereParameterName! }
        };
    }

    private static GeneratedExecutionShape ResolveExecutionShape(IMethodSymbol method, GeneratedQueryModel model)
    {
        if (model.Kind == "Insert")
        {
            return IsReadOnlyListOfInt64(method.ReturnType) ? GeneratedExecutionShape.Insert : GeneratedExecutionShape.Unsupported;
        }

        if (model.Kind == "Update")
        {
            return method.ReturnType.SpecialType == SpecialType.System_Int32 ? GeneratedExecutionShape.Update : GeneratedExecutionShape.Unsupported;
        }

        if (model.Kind == "SelectSingle")
        {
            ITypeSymbol? rowType = GetSelectRowType(method);
            if (rowType is null || !IsSupportedSelectRowType(rowType))
            {
                return GeneratedExecutionShape.Unsupported;
            }

            return IsListLike(method.ReturnType) ? GeneratedExecutionShape.SelectMany : GeneratedExecutionShape.SelectSingle;
        }

        return GeneratedExecutionShape.Unsupported;
    }

    private static ITypeSymbol? GetSelectRowType(IMethodSymbol method)
    {
        if (IsListLike(method.ReturnType) &&
            method.ReturnType is INamedTypeSymbol listType &&
            listType.TypeArguments.Length == 1)
        {
            return UnwrapNullableValueType(listType.TypeArguments[0]);
        }

        return UnwrapNullableValueType(method.ReturnType);
    }

    private static ITypeSymbol UnwrapNullableValueType(ITypeSymbol type)
    {
        return type is INamedTypeSymbol named &&
               named.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
               named.TypeArguments.Length == 1
            ? named.TypeArguments[0]
            : type;
    }

    private static bool IsSupportedSelectRowType(ITypeSymbol rowType)
    {
        return rowType is INamedTypeSymbol named &&
               named.TypeKind is TypeKind.Class or TypeKind.Struct &&
               named.SpecialType == SpecialType.None &&
               !named.IsAnonymousType &&
               !named.IsTupleType;
    }

    private static bool IsListLike(ITypeSymbol returnType)
    {
        return returnType is INamedTypeSymbol named &&
               named.TypeArguments.Length == 1 &&
               (named.Name == "List" && named.ContainingNamespace.ToDisplayString() == "System.Collections.Generic" ||
                named.Name == "IReadOnlyList" && named.ContainingNamespace.ToDisplayString() == "System.Collections.Generic");
    }

    private static bool IsReadOnlyListOfInt64(ITypeSymbol returnType)
    {
        return returnType is INamedTypeSymbol named &&
               named.Name == "IReadOnlyList" &&
               named.ContainingNamespace.ToDisplayString() == "System.Collections.Generic" &&
               named.TypeArguments.Length == 1 &&
               named.TypeArguments[0].SpecialType == SpecialType.System_Int64;
    }

    private static string FindMethodParameterName(IMethodSymbol method, string sqlParameter)
    {
        return method.Parameters
            .Skip(1)
            .First(parameter => string.Equals(parameter.Name, sqlParameter, StringComparison.OrdinalIgnoreCase))
            .Name;
    }

    private static string GetContainingTypeDeclaration(INamedTypeSymbol containingType)
    {
        string accessibility = containingType.DeclaredAccessibility switch
        {
            Accessibility.Public => "public ",
            Accessibility.Internal => "internal ",
            Accessibility.Private => "private ",
            Accessibility.Protected => "protected ",
            Accessibility.ProtectedAndInternal => "private protected ",
            Accessibility.ProtectedOrInternal => "protected internal ",
            _ => string.Empty
        };

        string kind = containingType.TypeKind switch
        {
            TypeKind.Struct => "struct",
            TypeKind.Class => "class",
            _ => "class"
        };

        string staticModifier = containingType.IsStatic ? "static " : string.Empty;
        return $"{accessibility}{staticModifier}partial {kind} {containingType.Name}";
    }

    private static string QuoteList(IEnumerable<string> values) => string.Join(", ", values.Select(static value => $"\"{value}\""));

    private static string AssignmentList(IReadOnlyDictionary<string, string> assignments) => string.Join(", ", assignments.Select(static pair => $"[\"{pair.Key}\"] = \"{pair.Value}\""));

    private enum GeneratedExecutionShape
    {
        Unsupported,
        SelectSingle,
        SelectMany,
        Insert,
        Update
    }
}
