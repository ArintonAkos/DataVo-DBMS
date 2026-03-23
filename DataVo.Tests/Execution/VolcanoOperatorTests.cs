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
}
