using DataVo.Generators.Diagnostics;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace DataVo.Generators;

/// <summary>Emits typed schema, lookup, insert and update extension methods for <c>[DataVoTable]</c> models.</summary>
[Generator]
public sealed class DataVoTableGenerator : IIncrementalGenerator
{
    private const string TableAttribute = "DataVo.Core.Annotations.DataVoTableAttribute";

    private static readonly DiagnosticDescriptor[] Descriptors =
    [
        DataVoGeneratorDiagnostics.MissingPrimaryKey,
        DataVoGeneratorDiagnostics.MultiplePrimaryKeys,
        DataVoGeneratorDiagnostics.UnsupportedPropertyType,
        DataVoGeneratorDiagnostics.NullablePrimaryKey,
        DataVoGeneratorDiagnostics.NoUsableConstructor,
        DataVoGeneratorDiagnostics.UnsupportedModelShape,
        DataVoGeneratorDiagnostics.InvalidTableName,
    ];

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValuesProvider<TableInspection> models = context.SyntaxProvider.ForAttributeWithMetadataName(
            TableAttribute,
            static (node, _) => node is TypeDeclarationSyntax,
            static (attributeContext, _) => DataVoTableInspector.Inspect(
                (INamedTypeSymbol)attributeContext.TargetSymbol,
                attributeContext.Attributes[0].ConstructorArguments.FirstOrDefault().Value as string));

        context.RegisterSourceOutput(models, static (output, inspection) =>
        {
            foreach (DiagnosticInfo info in inspection.Diagnostics)
            {
                output.ReportDiagnostic(ToDiagnostic(info));
            }

            if (inspection.Model is { } model)
            {
                output.AddSource(model.HintName, SourceText.From(DataVoTableEmitter.Emit(model), System.Text.Encoding.UTF8));
            }
        });
    }

    private static Diagnostic ToDiagnostic(DiagnosticInfo info)
    {
        DiagnosticDescriptor descriptor = Descriptors.First(candidate => candidate.Id == info.DescriptorId);
        Location location = string.IsNullOrEmpty(info.FilePath) ? Location.None : Location.Create(info.FilePath, info.Span, info.LineSpan);
        return Diagnostic.Create(descriptor, location, info.Arguments.Cast<object?>().ToArray());
    }
}
