# Serialization Overview

`RowSerializer` converts between in-memory row dictionaries (`Dictionary<string, object?>`) and the binary representation stored by the storage engines.

## Encoding

- Values are written in catalog column order.
- Each column begins with a null flag (`bool`): `true` means null, `false` means value follows.
- Non-null encodings:
	- `INT`: `int32`
	- `FLOAT`: IEEE float stored as int bits
	- `BIT`: `bool`
	- `DATE` / `DATETIME`: `DateTime.ToBinary()` (`int64`)
	- `VECTOR`: `[int32 count][float bits * count]`
	- default: UTF-8 string via `BinaryWriter.Write(string)`

## Schema awareness & caching

Schema columns are loaded from the `EngineCatalog` and cached per engine/database/table + schema version.

## File documentation

- [RowSerializer](./RowSerializer.md)

## Interactions

Used by both disk and memory storage paths via `StorageContext`.
