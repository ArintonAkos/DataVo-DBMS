# RowSerializer

`RowSerializer` converts logical row dictionaries into binary payloads and back again.

## Why it exists

The storage backends operate on raw bytes. The serializer makes that possible while keeping schema knowledge in one place.

## Supported value kinds

- `INT`
- `FLOAT`
- `BIT`
- `DATE`
- `DATETIME`
- string-like values such as `VARCHAR`

## Key design details

- rows are serialized in catalog column order
- each field starts with a null flag
- schema metadata is cached per engine/database/table key
- cache entries are invalidated automatically through schema version changes

## Example

```csharp
byte[] bytes = RowSerializer.Serialize("DemoDb", "Users", new Dictionary<string, dynamic>
{
    ["Id"] = 1,
    ["Name"] = "Alice"
});

Dictionary<string, dynamic> row = RowSerializer.Deserialize("DemoDb", "Users", bytes);
```
