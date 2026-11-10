using System.Collections.Immutable;
using System.Text.RegularExpressions;
using DataVo.Generators.Diagnostics;
using Microsoft.CodeAnalysis;

namespace DataVo.Generators.Tables;

/// <summary>Turns a <c>[DataVoTable]</c> type into a <see cref="TableModel"/>, or diagnostics explaining why it cannot be one.</summary>
internal static class DataVoTableInspector
{
    private const string Annotations = "DataVo.Core.Annotations.";
    private static readonly Regex Identifier = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.CultureInvariant);

    private static readonly SymbolDisplayFormat TypeNameFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions |
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    public static TableInspection Inspect(INamedTypeSymbol type, string? tableName)
    {
        var diagnostics = ImmutableArray.CreateBuilder<DiagnosticInfo>();
        void Report(DiagnosticDescriptor descriptor, ISymbol at, params string[] arguments) =>
            diagnostics.Add(ToInfo(descriptor, at, arguments));

        string? shapeProblem = ShapeProblem(type);
        if (shapeProblem is not null)
        {
            Report(DataVoGeneratorDiagnostics.UnsupportedModelShape, type, type.Name, shapeProblem);
            return Failed(diagnostics);
        }

        if (tableName is null || !Identifier.IsMatch(tableName))
        {
            Report(DataVoGeneratorDiagnostics.InvalidTableName, type, tableName ?? string.Empty);
        }

        IMethodSymbol? constructor = ChooseConstructor(type);
        if (constructor is null)
        {
            Report(DataVoGeneratorDiagnostics.NoUsableConstructor, type, type.Name);
            return Failed(diagnostics);
        }

        ImmutableArray<ColumnModel>.Builder columns = CollectColumns(type, constructor, Report);
        ValidateKey(type, columns, Report);
        if (diagnostics.Count > 0)
        {
            return Failed(diagnostics);
        }

        var model = new TableModel(
            type.Name,
            type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            type.ContainingNamespace.IsGlobalNamespace ? null : type.ContainingNamespace.ToDisplayString(),
            HintNameOf(type),
            tableName!,
            IsPubliclyVisible(type),
            type.IsReferenceType,
            new EquatableArray<string>([.. constructor.Parameters.Select(parameter => parameter.Name)]),
            new EquatableArray<ColumnModel>(columns.ToImmutable()));
        return new TableInspection(model, default);
    }

    private static TableInspection Failed(ImmutableArray<DiagnosticInfo>.Builder diagnostics) =>
        new(null, new EquatableArray<DiagnosticInfo>(diagnostics.ToImmutable()));

    private static string? ShapeProblem(INamedTypeSymbol type)
    {
        if (type.IsGenericType)
        {
            return "generic types are not supported";
        }

        if (type.IsAbstract)
        {
            return "abstract types cannot be constructed";
        }

        for (INamedTypeSymbol? current = type; current is not null; current = current.ContainingType)
        {
            if (current.IsGenericType || current.DeclaredAccessibility is Accessibility.Private or Accessibility.Protected or Accessibility.ProtectedAndInternal)
            {
                return "the generated extension methods could not reach it";
            }
        }

        return null;
    }

    private static bool IsPubliclyVisible(INamedTypeSymbol type)
    {
        for (INamedTypeSymbol? current = type; current is not null; current = current.ContainingType)
        {
            if (current.DeclaredAccessibility != Accessibility.Public)
            {
                return false;
            }
        }

        return true;
    }

    private static string HintNameOf(INamedTypeSymbol type) =>
        Regex.Replace(type.ToDisplayString(), "[^A-Za-z0-9_.]", "_") + ".DataVoTable.g.cs";

    private static bool IsAccessible(ISymbol symbol) => symbol.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal;

    private static IMethodSymbol? ChooseConstructor(INamedTypeSymbol type)
    {
        IMethodSymbol[] constructors = [.. type.InstanceConstructors.Where(IsAccessible).Where(constructor => !IsCopyConstructor(type, constructor))];
        return constructors.FirstOrDefault(static constructor => constructor.Parameters.Length == 0)
            ?? constructors.OrderByDescending(static constructor => constructor.Parameters.Length)
                .FirstOrDefault(constructor => constructor.Parameters.All(parameter => FindProperty(type, parameter.Name) is not null));
    }

    private static bool IsCopyConstructor(INamedTypeSymbol type, IMethodSymbol constructor) =>
        constructor.Parameters.Length == 1 && SymbolEqualityComparer.Default.Equals(constructor.Parameters[0].Type, type);

    private static IPropertySymbol? FindProperty(INamedTypeSymbol type, string name) =>
        AllProperties(type).FirstOrDefault(property => property.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    /// <summary>Public instance properties, base types first, with an override replacing what it overrides.</summary>
    private static IEnumerable<IPropertySymbol> AllProperties(INamedTypeSymbol type)
    {
        var chain = new Stack<INamedTypeSymbol>();
        for (INamedTypeSymbol? current = type; current is not null && current.SpecialType != SpecialType.System_Object; current = current.BaseType)
        {
            chain.Push(current);
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var ordered = new List<IPropertySymbol>();
        foreach (INamedTypeSymbol level in chain)
        {
            foreach (IPropertySymbol property in level.GetMembers().OfType<IPropertySymbol>())
            {
                if (property is { IsStatic: false, IsIndexer: false, DeclaredAccessibility: Accessibility.Public, GetMethod: not null })
                {
                    if (seen.Add(property.Name))
                    {
                        ordered.Add(property);
                    }
                    else
                    {
                        ordered[ordered.FindIndex(existing => existing.Name == property.Name)] = property;
                    }
                }
            }
        }

        return ordered;
    }

    private static ImmutableArray<ColumnModel>.Builder CollectColumns(
        INamedTypeSymbol type,
        IMethodSymbol constructor,
        Action<DiagnosticDescriptor, ISymbol, string[]> report)
    {
        var columns = ImmutableArray.CreateBuilder<ColumnModel>();
        foreach (IPropertySymbol property in AllProperties(type))
        {
            IParameterSymbol? parameter = constructor.Parameters.FirstOrDefault(item => item.Name.Equals(property.Name, StringComparison.OrdinalIgnoreCase));
            bool settable = property.SetMethod is not null && IsAccessible(property.SetMethod);
            if (!settable && parameter is null)
            {
                continue; // a computed property has nothing to store
            }

            if (!Identifier.IsMatch(property.Name))
            {
                report(DataVoGeneratorDiagnostics.InvalidTableName, property, [property.Name]);
                continue;
            }

            if (MapType(property.Type) is not { } column)
            {
                report(DataVoGeneratorDiagnostics.UnsupportedPropertyType, property, [property.Name, property.Type.ToDisplayString()]);
                continue;
            }

            bool indexed = HasAttribute(property, parameter, "IndexedAttribute");
            bool spatialIndexed = HasAttribute(property, parameter, "SpatialIndexAttribute");
            if (spatialIndexed && !column.Type.IsVector3)
            {
                report(DataVoGeneratorDiagnostics.SpatialIndexNeedsVector3, property, [property.Name, property.Type.ToDisplayString()]);
                continue;
            }

            // A B-tree cannot order points, so [Indexed] on a position means the index that can: a spatial one.
            if (column.Type.IsVector3)
            {
                spatialIndexed |= indexed;
                indexed = false;
            }

            columns.Add(new ColumnModel(
                property.Name,
                property.Type.ToDisplayString(TypeNameFormat),
                column.Type,
                column.IsNullable,
                column.IsExplicitlyNullable,
                HasAttribute(property, parameter, "PrimaryKeyAttribute"),
                indexed,
                spatialIndexed,
                parameter is not null,
                settable));
        }

        return columns;
    }

    private static bool HasAttribute(IPropertySymbol property, IParameterSymbol? parameter, string name)
    {
        string fullName = Annotations + name;
        return property.GetAttributes().Concat(parameter?.GetAttributes() ?? [])
            .Any(attribute => attribute.AttributeClass?.ToDisplayString() == fullName);
    }

    private static void ValidateKey(
        INamedTypeSymbol type,
        ImmutableArray<ColumnModel>.Builder columns,
        Action<DiagnosticDescriptor, ISymbol, string[]> report)
    {
        ColumnModel[] keys = [.. columns.Where(static column => column.IsPrimaryKey)];
        if (keys.Length == 0)
        {
            report(DataVoGeneratorDiagnostics.MissingPrimaryKey, type, [type.Name]);
        }
        else if (keys.Length > 1)
        {
            report(DataVoGeneratorDiagnostics.MultiplePrimaryKeys, type, [type.Name]);
        }
        else if (keys[0].IsExplicitlyNullable)
        {
            report(DataVoGeneratorDiagnostics.NullablePrimaryKey, type, [keys[0].PropertyName]);
        }
    }

    private static (ColumnType Type, bool IsNullable, bool IsExplicitlyNullable)? MapType(ITypeSymbol type)
    {
        bool nullableValue = type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T };
        ITypeSymbol core = nullableValue ? ((INamedTypeSymbol)type).TypeArguments[0] : type;

        // Without nullable annotations (Unity's default) a string may legitimately hold null.
        bool explicitlyNullable = nullableValue || type.NullableAnnotation == NullableAnnotation.Annotated;
        bool nullable = explicitlyNullable || (type.IsReferenceType && type.NullableAnnotation == NullableAnnotation.None);
        string prefix = nullable ? "ReadNullable" : "Read";
        ColumnType? column = core.SpecialType switch
        {
            SpecialType.System_Int32 => new ColumnType("INT", prefix + "Int32", string.Empty, true, false),
            SpecialType.System_Int64 => new ColumnType("BIGINT", prefix + "Int64", string.Empty, true, false),
            SpecialType.System_Single => new ColumnType("FLOAT", prefix + "Single", string.Empty, true, false),
            SpecialType.System_Double => new ColumnType("DOUBLE", prefix + "Double", string.Empty, true, false),
            SpecialType.System_Boolean => new ColumnType("BIT", prefix + "Boolean", string.Empty, true, false),
            SpecialType.System_String => new ColumnType("VARCHAR", prefix + "String", string.Empty, false, false),
            _ => core.ToDisplayString() switch
            {
                "System.Guid" => new ColumnType("GUID", prefix + "Guid", string.Empty, true, false),
                "DataVo.Core.DataVoDate" => new ColumnType("DATE", prefix + "Date", string.Empty, true, true),
                "System.DateOnly" => new ColumnType("DATE", prefix + "Date", nullable ? "?.ToDateOnly()" : ".ToDateOnly()", true, true),
                "System.Numerics.Vector3" => new ColumnType("VECTOR(3)", prefix + "Vector3", string.Empty, true, false, IsVector3: true),
                _ => null,
            },
        };

        return column is null ? null : (column, nullable, explicitlyNullable);
    }

    private static DiagnosticInfo ToInfo(DiagnosticDescriptor descriptor, ISymbol at, string[] arguments)
    {
        Location location = at.Locations.FirstOrDefault() ?? Location.None;
        FileLinePositionSpan lines = location.GetLineSpan();
        return new DiagnosticInfo(descriptor.Id, lines.Path ?? string.Empty, location.SourceSpan, lines.Span, new EquatableArray<string>([.. arguments]));
    }
}
