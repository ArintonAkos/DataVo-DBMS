using Research.Benchmark.Host;

namespace Research.Benchmark.Tests;

public sealed class ArenaLiveViewsCommandTests
{
    [Fact]
    public void Command_RejectsUnknownProfile()
    {
        Assert.Throws<ArgumentException>(() => ArenaLiveViewsCommand.Parse(
            ["--profile", "fast", "--repetitions", "7", "--output", "/tmp/out"]));
    }

    [Fact]
    public void Command_RejectsUnknownEngineRatherThanOmittingIt()
    {
        Assert.Throws<ArgumentException>(() => ArenaLiveViewsCommand.Parse(
            ["--profile", "gameplay", "--engine", "rocksdb", "--output", "/tmp/out"]));
    }

    [Fact]
    public void Command_ParsesEveryAcceptedOptionAndSelectsAllEngines()
    {
        ArenaLiveViewsCommandOptions command = ArenaLiveViewsCommand.Parse(
            ["--profile", "ivm-stress", "--engine", "all", "--repetitions", "3", "--output", "artifacts/test", "--format", "json"]);

        Assert.Equal("ivm-stress", command.Profile.Id);
        Assert.Equal("all", command.Engine);
        Assert.Equal(3, command.Repetitions);
        Assert.Equal(ArenaLiveViewsOutputFormat.Json, command.Format);
        Assert.True(Path.IsPathFullyQualified(command.OutputDirectory));
        Assert.Equal(4, command.CreateEngineFactories().Count);
    }
}
