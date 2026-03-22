using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class WasmStorageBackendTests
{
    [Fact]
    public void DataVoEngine_WasmMode_ResolvesWasmBackend_AndExecutesCrud()
    {
        using var context = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Wasm
        });

        Assert.NotNull(context.Engine.StorageContext.Backend);
        Assert.Equal("Wasm", context.Engine.StorageContext.Backend!.BackendKind);

        context.Execute("CREATE DATABASE WasmDb");
        context.Execute("USE WasmDb");
        context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR)");
        context.Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Alpha')");

        var result = context.Execute("SELECT Name FROM Items WHERE Id = 1").Last();

        Assert.False(result.IsError);
        Assert.Single(result.Data);
        Assert.Equal("Alpha", result.Data[0]["Name"]);
    }
}
