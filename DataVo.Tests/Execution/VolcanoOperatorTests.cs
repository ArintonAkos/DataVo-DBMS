using DataVo.Core.Execution.Volcano;

namespace DataVo.Tests.Execution;

public class VolcanoOperatorTests
{
    [Fact]
    public void TableScan_Filter_Project_Pipeline_StreamsExpectedRows()
    {
        var input = new List<ExecutionRow>
        {
            new(1, new Dictionary<string, dynamic> { ["Id"] = 1, ["Name"] = "A", ["Score"] = 50 }),
            new(2, new Dictionary<string, dynamic> { ["Id"] = 2, ["Name"] = "B", ["Score"] = 95 }),
            new(3, new Dictionary<string, dynamic> { ["Id"] = 3, ["Name"] = "C", ["Score"] = 99 }),
        };

        IQueryOperator scan = new TableScanOperator(input);
        IQueryOperator filtered = new FilterOperator(scan, row => (int)row["Score"] >= 90);
        IQueryOperator projected = new ProjectOperator(filtered, row => new Dictionary<string, dynamic>
        {
            ["Id"] = row["Id"],
            ["Name"] = row["Name"]
        });

        List<ExecutionRow> result = OperatorPipelineRunner.ExecuteToList(projected);

        Assert.Equal(2, result.Count);
        Assert.Equal(2, (int)result[0]["Id"]);
        Assert.Equal("B", (string)result[0]["Name"]);
        Assert.Equal(3, (int)result[1]["Id"]);
        Assert.Equal("C", (string)result[1]["Name"]);
        Assert.DoesNotContain("Score", result[0].Values.Keys);
    }

    [Fact]
    public void FilterOperator_GetNextRow_ProducesRowsIncrementally()
    {
        var input = new List<ExecutionRow>
        {
            new(10, new Dictionary<string, dynamic> { ["V"] = 1 }),
            new(11, new Dictionary<string, dynamic> { ["V"] = 2 }),
            new(12, new Dictionary<string, dynamic> { ["V"] = 3 })
        };

        IQueryOperator op = new FilterOperator(new TableScanOperator(input), row => (int)row["V"] >= 2);

        op.Open();
        try
        {
            var first = op.GetNextRow();
            var second = op.GetNextRow();
            var done = op.GetNextRow();

            Assert.NotNull(first);
            Assert.NotNull(second);
            Assert.Equal(11, first!.RowId);
            Assert.Equal(12, second!.RowId);
            Assert.Null(done);
        }
        finally
        {
            op.Close();
        }
    }

    [Fact]
    public void TakeOperator_ReturnsAtMostConfiguredRows()
    {
        var input = new List<ExecutionRow>
        {
            new(1, new Dictionary<string, dynamic> { ["V"] = 10 }),
            new(2, new Dictionary<string, dynamic> { ["V"] = 20 }),
            new(3, new Dictionary<string, dynamic> { ["V"] = 30 })
        };

        IQueryOperator op = new TakeOperator(new TableScanOperator(input), 2);

        List<ExecutionRow> rows = OperatorPipelineRunner.ExecuteToList(op);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].RowId);
        Assert.Equal(2, rows[1].RowId);
    }

    [Fact]
    public void SkipOperator_SkipsConfiguredRows()
    {
        var input = new List<ExecutionRow>
        {
            new(1, new Dictionary<string, dynamic> { ["V"] = 10 }),
            new(2, new Dictionary<string, dynamic> { ["V"] = 20 }),
            new(3, new Dictionary<string, dynamic> { ["V"] = 30 })
        };

        IQueryOperator op = new SkipOperator(new TableScanOperator(input), 2);

        List<ExecutionRow> rows = OperatorPipelineRunner.ExecuteToList(op);

        Assert.Single(rows);
        Assert.Equal(3, rows[0].RowId);
    }

    [Fact]
    public void InnerJoinOperator_JoinsRowsOnMatchingKeys()
    {
        var leftRows = new List<ExecutionRow>
        {
            new(1, new Dictionary<string, dynamic> { ["Id"] = 1, ["CustomerId"] = 10 }),
            new(2, new Dictionary<string, dynamic> { ["Id"] = 2, ["CustomerId"] = 11 }),
            new(3, new Dictionary<string, dynamic> { ["Id"] = 3, ["CustomerId"] = 99 })
        };

        var rightRows = new List<ExecutionRow>
        {
            new(10, new Dictionary<string, dynamic> { ["Id"] = 10, ["Name"] = "Alice" }),
            new(11, new Dictionary<string, dynamic> { ["Id"] = 11, ["Name"] = "Bob" })
        };

        IQueryOperator leftScan = new TableScanOperator(leftRows);
        IQueryOperator rightScan = new TableScanOperator(rightRows);
        IQueryOperator join = new InnerJoinOperator(leftScan, rightScan, "CustomerId", "Id", "Orders", "Customers");

        List<ExecutionRow> rows = OperatorPipelineRunner.ExecuteToList(join);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0]["Orders.Id"]);
        Assert.Equal("Alice", (string)rows[0]["Customers.Name"]);
        Assert.Equal(2, (int)rows[1]["Orders.Id"]);
        Assert.Equal("Bob", (string)rows[1]["Customers.Name"]);
    }

    [Fact]
    public void SortOperator_OrdersRowsByKey()
    {
        var input = new List<ExecutionRow>
        {
            new(1, new Dictionary<string, dynamic> { ["Id"] = 1, ["Score"] = 70 }),
            new(2, new Dictionary<string, dynamic> { ["Id"] = 2, ["Score"] = 90 }),
            new(3, new Dictionary<string, dynamic> { ["Id"] = 3, ["Score"] = 80 })
        };

        IQueryOperator scan = new TableScanOperator(input);
        IQueryOperator sort = new SortOperator(scan, row => row["Score"], ascending: false);

        List<ExecutionRow> rows = OperatorPipelineRunner.ExecuteToList(sort);

        Assert.Equal(3, rows.Count);
        Assert.Equal(2, (int)rows[0]["Id"]);
        Assert.Equal(3, (int)rows[1]["Id"]);
        Assert.Equal(1, (int)rows[2]["Id"]);
    }
}
