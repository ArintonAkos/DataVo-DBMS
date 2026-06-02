using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.E2E;

public class ReactiveSubscriptionTests
{
    private static IReadOnlyDictionary<string, object?> Row(int id, int hp) =>
        new Dictionary<string, object?> { ["Id"] = id, ["Health"] = hp };

    [Fact]
    public void Rejects_Unsupported_Sql()
    {
        Assert.Throws<NotSupportedException>(() =>
            new ReactiveSubscription("SELECT Id FROM A JOIN B ON A.Id=B.Id"));
        Assert.Throws<NotSupportedException>(() =>
            new ReactiveSubscription("SELECT COUNT(*) FROM Players"));
    }

    [Fact]
    public void Apply_ComputesEnterLeaveStay()
    {
        var sub = new ReactiveSubscription("SELECT Id, Health FROM Players WHERE Health < 20");
        sub.Seed("Players", new (long, IReadOnlyDictionary<string, object?>)[] { (1, Row(1, 10)) }); // row 1 already matches

        var changes = new[]
        {
            new RowChange("Players", 2, ChangeKind.Insert, null, Row(2, 5)),        // enters -> Added
            new RowChange("Players", 1, ChangeKind.Update, Row(1, 10), Row(1, 50)), // leaves -> Removed
            new RowChange("Players", 3, ChangeKind.Insert, null, Row(3, 99)),       // never matches -> ignore
        };

        QueryChange qc = sub.Apply(changes);
        Assert.Single(qc.Added);
        Assert.Equal(2, qc.Added[0]["Id"]);
        Assert.Single(qc.Removed);
        Assert.Equal(1, qc.Removed[0]["Id"]);
        Assert.Empty(qc.Updated);
    }

    [Fact]
    public void Apply_StayInSet_IsUpdated()
    {
        var sub = new ReactiveSubscription("SELECT Id, Health FROM Players WHERE Health < 20");
        sub.Seed("Players", new (long, IReadOnlyDictionary<string, object?>)[] { (1, Row(1, 10)) });

        QueryChange qc = sub.Apply(new[]
        {
            new RowChange("Players", 1, ChangeKind.Update, Row(1, 10), Row(1, 15)),
        });

        Assert.Empty(qc.Added);
        Assert.Empty(qc.Removed);
        Assert.Single(qc.Updated);
        Assert.Equal(15, qc.Updated[0]["Health"]);
    }
}
