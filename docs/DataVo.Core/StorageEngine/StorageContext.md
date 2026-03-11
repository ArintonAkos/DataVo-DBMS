# StorageContext

`StorageContext` is the engine's storage-facing façade. Higher layers use it to read and write logical rows without depending directly on the concrete storage backend.

## Responsibilities

- choose the active storage engine from `DataVoConfig`
- trigger recovery in disk/WAL mode
- serialize rows through `RowSerializer`
- expose table-centric read/write APIs to parser actions and services

## Supported operations

- insert one row
- insert multiple rows
- delete rows by row id
- fetch all rows or specific row ids
- drop tables and databases
- compact a table

## Example

```csharp
StorageContext.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });

var context = StorageContext.Instance;
context.InsertOneIntoTable(
    new Dictionary<string, dynamic> { ["Id"] = 1, ["Name"] = "Alice" },
    "Users",
    "DemoDb");
```

## Contributor notes

- projection normalization strips table qualifiers such as `Users.Name`
- row materialization flows through `RowSerializer.Deserialize(...)`
- index-backed reads still use row ids and then hydrate rows here
