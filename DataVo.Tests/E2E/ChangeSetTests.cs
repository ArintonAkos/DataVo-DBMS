using DataVo.Core.Runtime.Changes;

namespace DataVo.Tests.E2E;

public class ChangeSetTests
{
    [Fact]
    public void RowChange_Insert_HasAfterOnly()
    {
        var after = new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada" };
        var change = new RowChange("Players", rowId: 10, ChangeKind.Insert, before: null, after: after);

        Assert.Equal("Players", change.Table);
        Assert.Equal(10, change.RowId);
        Assert.Equal(ChangeKind.Insert, change.Kind);
        Assert.Null(change.Before);
        Assert.Equal("Ada", change.After!["Name"]);
    }

    [Fact]
    public void ChangeSet_ExposesDistinctTables()
    {
        var changes = new[]
        {
            new RowChange("Players", 1, ChangeKind.Insert, null, new Dictionary<string, object?> { ["Id"] = 1 }),
            new RowChange("Items",   2, ChangeKind.Delete, new Dictionary<string, object?> { ["Id"] = 2 }, null),
        };
        var set = new ChangeSet(sequenceId: 5, databaseName: "Demo", changes);

        Assert.Equal(5, set.SequenceId);
        Assert.Equal(2, set.Changes.Count);
        Assert.Contains("Players", set.Tables);
        Assert.Contains("Items", set.Tables);
    }

    [Fact]
    public void ChangeCapture_DisabledByDefault()
    {
        var capture = new DataVo.Core.Runtime.Changes.ChangeCapture();
        Assert.False(capture.Enabled);
    }

    [Fact]
    public void ChangeCapture_SequenceIdsIncrease()
    {
        var capture = new DataVo.Core.Runtime.Changes.ChangeCapture { Enabled = true };
        Assert.True(capture.NextSequenceId() < capture.NextSequenceId());
    }
}
