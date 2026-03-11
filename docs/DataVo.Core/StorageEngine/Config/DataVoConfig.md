# DataVoConfig

`DataVoConfig` controls how a `DataVo` engine instance stores and recovers data.

## Main properties

- `StorageMode`
- `DiskStoragePath`
- `WalEnabled`
- `WalFilePath`
- `WalCheckpointThreshold`

## Example

```csharp
var config = new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true,
    WalFilePath = "datavo.wal",
    WalCheckpointThreshold = 1000
};
```

## Path resolution behavior

`ResolveWalFilePath()` keeps absolute paths unchanged. Relative paths are resolved against the disk storage directory in disk mode and against the current working directory in memory mode.
