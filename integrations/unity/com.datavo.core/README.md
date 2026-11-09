# DataVo Core for Unity (experimental preview)

DataVo is an embedded SQL engine written in pure C#. This package carries its `netstandard2.1` build and the assemblies that build needs at runtime.

## Status

This is a preview for evaluation. Test save data on every platform you ship to.

| Area | Status |
| --- | --- |
| Unity 6000.5.4f1 Editor | The portable build has passed EditMode smoke tests |
| macOS IL2CPP (6000.5.4f1, Medium stripping) | The portable build has passed a built Player smoke test |
| Windows, consoles, mobile | Not verified |
| `StorageMode.InMemory` | Available |
| `StorageMode.Disk`, `StorageMode.Lsm` | Save, close and reopen pass on macOS (Editor and IL2CPP); other platforms are unverified |
| Burst | Not supported: DataVo is managed code. Call it from the main thread or a managed thread |

## Install

In the Package Manager, choose **Add package from tarball** and select `com.datavo.core-<version>.tgz`.

## Quick start

Import the **Quick Start** sample from the Package Manager, or:

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Game");
db.Execute("USE Game");
db.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(40))");
db.Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Sword')");

var lookup = db.Prepare("SELECT Name FROM Items WHERE Id = @id");
var result = lookup.Execute(("id", 1));
```

For a per-frame loop, `ExecuteBorrowed` avoids allocating a result. The returned object is reused, so read it before the next query on that thread.
