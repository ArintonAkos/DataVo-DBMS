using DataVo.Core.BTree;

namespace DataVo.Tests.BTree;

public class IndexKeyEncoderTests
{
    [Fact]
    public void CompareKeys_IntValues_PreserveNumericOrdering()
    {
        byte[] left = IndexKeyEncoder.Encode("9");
        byte[] right = IndexKeyEncoder.Encode("20");

        Assert.True(IndexKeyEncoder.CompareKeys(left, right) < 0);
    }

    [Fact]
    public void CompareKeys_LongValuesBeyondIntRange_PreserveNumericOrdering()
    {
        byte[] left = IndexKeyEncoder.Encode("2147483648");
        byte[] right = IndexKeyEncoder.Encode("9223372036854775807");

        Assert.True(IndexKeyEncoder.CompareKeys(left, right) < 0);
    }

    [Fact]
    public void CompareKeys_NegativeLongAndPositiveLong_PreserveNumericOrdering()
    {
        byte[] left = IndexKeyEncoder.Encode("-9223372036854775808");
        byte[] right = IndexKeyEncoder.Encode("1");

        Assert.True(IndexKeyEncoder.CompareKeys(left, right) < 0);
    }

    [Fact]
    public void CompareKeys_IntMaxAndLongBoundary_PreserveNumericOrdering()
    {
        byte[] left = IndexKeyEncoder.Encode("2147483647");
        byte[] right = IndexKeyEncoder.Encode("2147483648");

        Assert.True(IndexKeyEncoder.CompareKeys(left, right) < 0);
    }

    [Fact]
    public void CompareKeys_LongBoundaryAndIntMin_PreserveNumericOrdering()
    {
        byte[] left = IndexKeyEncoder.Encode("-2147483649");
        byte[] right = IndexKeyEncoder.Encode("-2147483648");

        Assert.True(IndexKeyEncoder.CompareKeys(left, right) < 0);
    }
}
