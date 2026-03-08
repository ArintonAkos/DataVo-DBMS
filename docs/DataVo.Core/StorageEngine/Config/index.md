# Config (StorageEngine) Overview

The `Config` directory centralizes the runtime settings that determine how `DataVo` persists data. It now controls not only the storage mode, but also whether write-ahead logging is enabled and where the WAL file is stored.

## Core Responsibilities
* **Storage Selection:** Switches the engine between `InMemory` and `Disk` operation.
* **Durability Controls:** Enables or disables WAL, chooses the WAL file location, and defines checkpoint pruning thresholds.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `DataVoConfig.cs` | Defines the active storage mode, disk storage path, WAL enablement, WAL file path, checkpoint threshold, and helper logic for resolving the effective WAL location. |

## Dependencies & Interactions
`DataVoConfig` is passed into `StorageContext.Initialize()`. From there it influences storage-engine selection, startup recovery, and commit-time WAL behavior.

## WAL-Related Settings

| Property | Meaning | Default Behavior |
| :--- | :--- | :--- |
| `WalEnabled` | Enables write-ahead logging for committed transactions. | Defaults to `true` in disk mode and `false` in memory mode. |
| `WalFilePath` | The path of the append-only WAL file. | Defaults to `datavo.wal`. Relative paths are resolved against the disk storage directory in disk mode. |
| `WalCheckpointThreshold` | Controls when checkpointed WAL entries are pruned. | Defaults to `1000` entries. |
