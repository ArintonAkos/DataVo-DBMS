using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class InternalKeyTests
{
    private static byte[] Build(long pk, ulong seqno, LsmValueType type)
    {
        Span<byte> user = stackalloc byte[8];
        InternalKey.EncodeInt64UserKey(user, pk);
        var dest = new byte[InternalKey.MeasureSize(user.Length)];
        InternalKey.Write(dest, user, seqno, type);
        return dest;
    }

    [Fact]
    public void Write_Then_Extract_RoundTripsTagFields()
    {
        byte[] key = Build(pk: 42, seqno: 7, LsmValueType.Put);

        Assert.Equal(7UL, InternalKey.Sequence(key));
        Assert.Equal(LsmValueType.Put, InternalKey.ValueType(key));
        Assert.Equal(8, InternalKey.UserKey(key).Length);
    }

    [Fact]
    public void Compare_SameUserKey_HigherSeqnoSortsFirst()
    {
        byte[] newer = Build(pk: 42, seqno: 9, LsmValueType.Put);
        byte[] older = Build(pk: 42, seqno: 4, LsmValueType.Put);

        Assert.True(InternalKey.Compare(newer, older) < 0);
        Assert.True(InternalKey.Compare(older, newer) > 0);
    }

    [Fact]
    public void Compare_DifferentUserKeys_SortsAscendingRegardlessOfSeqno()
    {
        byte[] lowKeyOldSeq = Build(pk: 1, seqno: 1, LsmValueType.Put);
        byte[] highKeyNewSeq = Build(pk: 2, seqno: 999, LsmValueType.Put);

        Assert.True(InternalKey.Compare(lowKeyOldSeq, highKeyNewSeq) < 0);
    }

    [Fact]
    public void EncodeInt64UserKey_IsSignCorrect()
    {
        Span<byte> neg = stackalloc byte[8];
        Span<byte> zero = stackalloc byte[8];
        Span<byte> pos = stackalloc byte[8];
        InternalKey.EncodeInt64UserKey(neg, -5);
        InternalKey.EncodeInt64UserKey(zero, 0);
        InternalKey.EncodeInt64UserKey(pos, 1);

        Assert.True(neg.SequenceCompareTo(zero) < 0);
        Assert.True(zero.SequenceCompareTo(pos) < 0);
    }

    [Fact]
    public void Compare_SameUserKeyAndSeqno_PutSortsBeforeDeletion()
    {
        byte[] put = Build(pk: 42, seqno: 5, LsmValueType.Put);
        byte[] del = Build(pk: 42, seqno: 5, LsmValueType.Deletion);

        Assert.True(InternalKey.Compare(put, del) < 0);
    }
}
