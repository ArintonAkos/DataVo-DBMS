using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Model-building extensions for DataVo-specific column configuration.
/// </summary>
public static class DataVoPropertyBuilderExtensions
{
    /// <summary>
    /// Maps a <c>float[]</c> or <c>ReadOnlyMemory&lt;float&gt;</c> property to a DataVo <c>VECTOR(n)</c>
    /// column with <paramref name="dimensions"/> elements.
    /// </summary>
    /// <param name="propertyBuilder">The property being configured.</param>
    /// <param name="dimensions">The number of floats in every vector.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static PropertyBuilder<TProperty> HasDataVoVectorDimensions<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        int dimensions)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dimensions);

        propertyBuilder.HasAnnotation(DataVoAnnotationNames.VectorDimensions, dimensions);
        return propertyBuilder;
    }

    /// <inheritdoc cref="HasDataVoVectorDimensions{TProperty}(PropertyBuilder{TProperty}, int)"/>
    public static PrimitiveCollectionBuilder<TProperty> HasDataVoVectorDimensions<TProperty>(
        this PrimitiveCollectionBuilder<TProperty> propertyBuilder,
        int dimensions)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dimensions);

        propertyBuilder.HasAnnotation(DataVoAnnotationNames.VectorDimensions, dimensions);
        return propertyBuilder;
    }
}

/// <summary>
/// Names of the EF model annotations DataVo reads.
/// </summary>
public static class DataVoAnnotationNames
{
    /// <summary>The element count of a <c>VECTOR(n)</c> column (an <see cref="int"/>).</summary>
    public const string VectorDimensions = "DataVo:VectorDimensions";
}
