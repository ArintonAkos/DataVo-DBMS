using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

/// <summary>
/// GC Reduction Slice 4, P0.2: <see cref="CellValue"/> gains owned <c>VECTOR</c> (<see cref="float"/>[])
/// support. Ownership is strict at every boundary — the cell clones on store and on every read, so a
/// stored vector can never be mutated through an input reference or a returned reference.
/// </summary>
public class CellValueVectorTests
{
    [Fact]
    public void From_Vector_RoundTripsViaAsVector()
    {
        var v = new float[] { 1f, 2f, 3f };
        CellValue cell = CellValue.From(v);

        Assert.Equal(CellType.Vector, cell.Type);
        Assert.False(cell.IsNull);
        Assert.Equal(v, cell.AsVector());
    }

    [Fact]
    public void From_ClonesInput_MutatingOriginalDoesNotAffectCell()
    {
        var v = new float[] { 1f, 2f, 3f };
        CellValue cell = CellValue.From(v);

        v[0] = 99f; // mutate the caller's array after storing

        Assert.Equal(new float[] { 1f, 2f, 3f }, cell.AsVector());
    }

    [Fact]
    public void AsVector_ResultMutation_DoesNotAffectCell()
    {
        CellValue cell = CellValue.From(new float[] { 1f, 2f, 3f });

        float[] got = cell.AsVector();
        got[0] = 99f; // mutate the returned array

        Assert.Equal(new float[] { 1f, 2f, 3f }, cell.AsVector());
    }

    [Fact]
    public void ToObject_ReturnsClone_MutationDoesNotAffectCell()
    {
        CellValue cell = CellValue.From(new float[] { 1f, 2f, 3f });

        var got = Assert.IsType<float[]>(cell.ToObject());
        got[1] = 99f;

        Assert.Equal(new float[] { 1f, 2f, 3f }, cell.AsVector());
    }

    [Fact]
    public void FromObject_BoxedVector_ProducesVectorCell()
    {
        object boxed = new float[] { 4f, 5f };
        CellValue cell = CellValue.From(boxed);

        Assert.Equal(CellType.Vector, cell.Type);
        Assert.Equal(new float[] { 4f, 5f }, cell.AsVector());
    }

    [Fact]
    public void EmptyVector_RoundTrips()
    {
        CellValue cell = CellValue.From(Array.Empty<float>());

        Assert.Equal(CellType.Vector, cell.Type);
        Assert.Empty(cell.AsVector());
    }

    [Fact]
    public void TypeMismatch_Throws_BothDirections()
    {
        CellValue vector = CellValue.From(new float[] { 1f });
        Assert.Throws<InvalidOperationException>(() => vector.AsInt32()); // vector read as another type

        CellValue notVector = CellValue.From(42);
        Assert.Throws<InvalidOperationException>(() => notVector.AsVector()); // another cell read as vector
    }
}
