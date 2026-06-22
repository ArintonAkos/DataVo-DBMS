using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.DeepDocument;

namespace Research.Benchmark.Tests;

/// <summary>
/// Correctness contract for the Scenario B (Deep Document) engines: a saved nested order must reload with
/// all of its items and addresses intact, whether stored as one BSON document (LiteDB) or reconstructed
/// from normalized tables (DataVo).
/// </summary>
public sealed class DeepDocumentEngineTests
{
    public static IEnumerable<object[]> Engines()
    {
        yield return [new DataVoDeepDocumentEngine()];
        yield return [new LiteDbDeepDocumentEngine()];
        yield return [new SqliteDeepDocumentEngine()];
    }

    [Theory]
    [MemberData(nameof(Engines))]
    public void SavedNestedOrderReloadsIntact(IDeepDocumentEngine engine)
    {
        using (engine)
        {
            engine.Initialize();
            engine.BeginBatch();
            for (int id = 1; id <= 50; id++)
            {
                engine.Save(new DeepOrder(
                    id,
                    $"cust-{id}",
                    Total: id * 100.0,
                    Items:
                    [
                        new OrderItem(id * 10 + 1, "widget", 2, 4.5),
                        new OrderItem(id * 10 + 2, "gadget", 1, 9.0),
                        new OrderItem(id * 10 + 3, "gizmo", 3, 1.5),
                    ],
                    Addresses:
                    [
                        new OrderAddress("billing", $"{id} Main St", "Springfield", "12345"),
                        new OrderAddress("shipping", $"{id} Oak Ave", "Shelbyville", "67890"),
                    ]));
            }

            engine.CompleteBatch();

            DeepOrder? order = engine.Load(7);
            Assert.NotNull(order);
            Assert.Equal(7, order!.Id);
            Assert.Equal("cust-7", order.Customer);
            Assert.Equal(3, order.Items.Count);
            Assert.Equal(2, order.Addresses.Count);
            Assert.Contains(order.Items, i => i.Name == "gadget" && i.Quantity == 1 && i.UnitPrice == 9.0);
            Assert.Contains(order.Addresses, a => a.Kind == "shipping" && a.City == "Shelbyville");

            Assert.Null(engine.Load(9999));
        }
    }
}
