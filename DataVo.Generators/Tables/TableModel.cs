using System.Collections;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace System.Runtime.CompilerServices
{
    /// <summary>Lets the compiler emit records on netstandard2.0.</summary>
    internal static class IsExternalInit;
}

namespace DataVo.Generators.Tables
{
    /// <summary>An immutable array compared by content, so the incremental pipeline can tell when nothing changed.</summary>
    internal readonly struct EquatableArray<T>(ImmutableArray<T> items) : IEquatable<EquatableArray<T>>, IEnumerable<T>
        where T : IEquatable<T>
    {
        private readonly ImmutableArray<T> _items = items;

        public int Count => _items.IsDefault ? 0 : _items.Length;

        public bool Equals(EquatableArray<T> other) => AsArray().SequenceEqual(other.AsArray());

        public override bool Equals(object? obj) => obj is EquatableArray<T> other && Equals(other);

        public override int GetHashCode() => AsArray().Aggregate(17, static (hash, item) => unchecked((hash * 31) + item.GetHashCode()));

        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)AsArray()).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private ImmutableArray<T> AsArray() => _items.IsDefault ? ImmutableArray<T>.Empty : _items;
    }

    /// <summary>How a C# property maps to a column.</summary>
    /// <param name="SqlType">The SQL type.</param>
    /// <param name="ReaderMethod">The support method that reads the value.</param>
    /// <param name="ReadSuffix">Code appended to the read to convert it to the property's type.</param>
    /// <param name="IsVector3">Whether the column is a 3D position (<c>System.Numerics.Vector3</c> stored as VECTOR(3)).</param>
    internal sealed record ColumnType(string SqlType, string ReaderMethod, string ReadSuffix, bool IsValueType, bool UsesObjectParameter, bool IsVector3 = false);

    /// <summary>One model property stored as a column.</summary>
    internal sealed record ColumnModel(
        string PropertyName,
        string TypeName,
        ColumnType Type,
        bool IsNullable,
        bool IsExplicitlyNullable,
        bool IsPrimaryKey,
        bool IsIndexed,
        bool IsSpatialIndexed,
        bool IsConstructorParameter,
        bool IsSettable);

    /// <summary>Everything the emitter needs about one model; holds no compiler symbols.</summary>
    internal sealed record TableModel(
        string ModelName,
        string FullTypeName,
        string? Namespace,
        string HintName,
        string TableName,
        bool IsPublic,
        bool IsReferenceType,
        EquatableArray<string> ConstructorParameters,
        EquatableArray<ColumnModel> Columns);

    /// <summary>A diagnostic captured as plain values, so results stay comparable between builds.</summary>
    internal sealed record DiagnosticInfo(
        string DescriptorId,
        string FilePath,
        TextSpan Span,
        LinePositionSpan LineSpan,
        EquatableArray<string> Arguments);

    /// <summary>The outcome of inspecting one model.</summary>
    internal sealed record TableInspection(TableModel? Model, EquatableArray<DiagnosticInfo> Diagnostics);
}
