# Unity Automatic Type Binding Design

## Status

Approved in conversation on 2026-07-21, pending review of this written
specification. This is a follow-up to the narrow B2b Unity Editor/Mono smoke
proof. It does not change the experimental status of the Unity packages or make
a general Unity support claim.

## Problem

The existing Unity package exposes `ToDataVoCell()` and `AsVector3()`-style
extension methods. They are useful as low-level escape hatches, but they force
game/application code to know about DataVo's `CellValue` and `VECTOR(n)`
representation. That is the wrong default API for an embedded database driver.

DataVo must convert Unity values automatically on writes and convert stored
vectors back to the Unity type the caller requests on reads. It must do so
without adding Unity references to `DataVo.Core`, silently guessing quaternion
semantics, or creating avoidable temporary vector arrays.

## Decisions

- A Unity project calls one opt-in setup method per `DataVoContext`:

  ```csharp
  context.UseUnityTypes();
  ```

  It registers both UnityEngine and Unity Mathematics types. The developer does
  not choose one family globally; one project may use both.
- The first supported types are `Vector2`, `Vector3`, `Vector4`, `Quaternion`,
  `float2`, `float3`, `float4`, and `quaternion`.
- `VECTOR(2)`, `VECTOR(3)`, and `VECTOR(4)` have no semantic type tag in SQL.
  Therefore DataVo must never infer `Quaternion` versus `Vector4`, or `Vector3`
  versus `float3`, from vector length alone.
- A normal read supplies the desired C# type explicitly:

  ```csharp
  Vector3 position = context.Get<Vector3>(row, "Position");
  ```

  C# cannot infer a generic method's type argument from the assignment target,
  so `context.Get(row, "Position")` cannot be a type-safe equivalent.
- Model mapping is the preferred convenience API where a model exists:

  ```csharp
  PlayerState player = context.Map<PlayerState>(row);
  ```

  The model property's declared C# type selects the converter. A model is never
  required: `Get<T>` covers scalar, debugging, tooling, aggregate, and ad-hoc
  reads.
- `ToDataVoCell()` and `As*()` remain public, documented low-level APIs. They
  are not the recommended gameplay/business-code path.
- This slice does not invent a string-SQL parameter API. `DataVoContext.Execute`
  currently accepts only SQL text; automatic conversion integrates with the
  existing typed insert, `BulkInsert`, compiled-query parameter, and result-row
  boundaries instead.

## Alternatives Considered

### Dimension-only inference

Convert every `VECTOR(3)` to `Vector3` and every `VECTOR(4)` to `Quaternion`.
This is concise but incorrect: a four-component embedding, color, vector, and
rotation are all valid `VECTOR(4)` values. It also arbitrarily excludes
`float3` and `float4`. Rejected.

### `object?` / `float[]` materialization in application code

Keep the current raw result and require casts or calls to `ToDataVoCell()`.
This is compatible but leaks storage representation and moves driver work into
business code. Retained only as the legacy/advanced path.

### Typed target plus model mapping (chosen)

`Get<T>` supplies the missing semantic target for an individual field; `Map<T>`
obtains it from property types where a model already exists. This is explicit
only where it must be, remains strongly typed, and needs no SQL dialect-specific
code in Unity gameplay code.

## Public Shape

### Core conversion registry

`DataVo.Core` gains a Unity-free, per-context value-converter registry. Its
generic registration contract converts a supported CLR value to an owned
`CellValue` for typed writes and converts a DataVo scalar/vector value back to
the requested CLR type for reads. Core knows only CLR `Type`, `CellValue`, and
the converter contract; it never references Unity assemblies.

The registry is owned by `DataVoContext`, not static global state. Registration
must reject a conflicting converter for the same CLR type and must be atomic for
a converter group so a failed Unity setup cannot leave half the Unity types
registered.

`UseUnityTypes()` lives in `com.datavo.unity.mathematics`, registers all eight
converters as one group, and is idempotent for that package's own registrations.
It throws on a genuine user-supplied conflict instead of silently replacing a
converter.

### Read APIs

Core exposes these context methods over the existing materialized result-row
shape:

```csharp
T Get<T>(IReadOnlyDictionary<string, object?> row, string columnName);
T Map<T>(IReadOnlyDictionary<string, object?> row);
```

`Get<T>` looks up the column, returns an already-compatible built-in value
directly, or invokes the registered converter for `T`. It does not expose a
`float[]` to Unity application code.

`Map<T>` is a convenience mapper for a public parameterless model with public
writable properties. It caches the case-insensitive column-to-property binding
per model type. Returned fields with no writable matching property are ignored,
which permits projections. A matching property whose value cannot be converted
throws; DataVo never substitutes a default Unity value for an invalid VECTOR.
Constructor-bound immutable records, custom column-name attributes, and source
generation are intentionally deferred.

### Write APIs

Core adds a matching typed entity write:

```csharp
long Insert<T>(string tableName, T entity);
```

It maps public readable properties by case-insensitive column name, converts
registered values to `CellValue`, and uses the existing `InsertTyped` pipeline.
This lets ordinary entity code write a `Vector3`, `float3`, or quaternion without
constructing a `CellValue` or a `float[]`.

`BulkInsert` and compiled-query DML parameters also pass supplied values through
the same registry's canonical binding before their existing column normalization.
This preserves their APIs while accepting Unity values automatically, without
serializing a Unity vector to text and parsing it again. The typed `Insert<T>`
route is the recommended model-write path because it can use the typed cell
pipeline directly.

## Unity Mapping Rules

| CLR type | Stored DataVo value | Component order |
| --- | --- | --- |
| `Vector2`, `float2` | `VECTOR(2)` | `x, y` |
| `Vector3`, `float3` | `VECTOR(3)` | `x, y, z` |
| `Vector4`, `float4` | `VECTOR(4)` | `x, y, z, w` |
| `Quaternion`, `quaternion` | `VECTOR(4)` | `x, y, z, w` |

Quaternion conversion never normalizes, canonicalizes, or flips signs. It is a
lossless component transfer, not a rotation-distance feature.

Each Unity read validates that the source is a DataVo VECTOR and that its
dimension exactly matches the requested target. `Get<Vector3>` from `VECTOR(2)`
or `VECTOR(4)` fails with a message naming both expected and actual dimensions.
The same rule applies to model properties.

## Allocation and Performance Contract

- A fixed-size Unity vector written through the typed path produces one owned
  `float[]`: the array retained by the `CellValue`. It does not build a
  temporary `float[]`, serialize it to text, then parse a second array.
- Unity vector reads copy components directly into a stack buffer/value type;
  they create no additional managed vector array.
- The existing dictionary and compiled-query parameter APIs accept `object?`,
  so a struct passed through them can be boxed. This is inherent in those legacy
  public signatures. The registry must still avoid an additional vector-array
  copy.
- `Map<T>` caches reflection metadata but is a convenience materialization API,
  not the zero-allocation per-frame lane. Existing typed/compiled APIs and the
  low-level cell methods remain available for specialized hot paths.

## Errors

- Unknown result column: `KeyNotFoundException` naming the column.
- No registered converter for a requested non-built-in type: a clear
  `NotSupportedException` naming the requested CLR type.
- Non-VECTOR data or dimension mismatch: a conversion exception naming the
  requested CLR type, expected `VECTOR(n)`, and actual DataVo value/dimension.
- A conflicting converter registration: `InvalidOperationException`; the
  existing registration remains untouched.
- A missing required table column or an entity value incompatible with a matching
  table column: a write-time validation error before any row is inserted.

## Testing and Evidence

1. Core tests run on both `net10.0` and opt-in `netstandard2.1` using fake
   non-Unity converters. They cover registry isolation, atomic registration,
   `Get<T>`, `Map<T>`, and `Insert<T>` error behavior.
2. Unity Editor tests on the pinned `6000.5.4f1` project prove that one
   `UseUnityTypes()` call supports all eight types in both directions.
3. Unity tests prove model mapping, ad-hoc `Get<T>`, model insert, quaternion
   component preservation, mixed UnityEngine/Mathematics models, and exact
   dimension errors.
4. Allocation tests verify the direct typed Unity write creates only the owned
   vector backing array and that typed Unity reads do not allocate a temporary
   vector array. Broader gameplay/IL2CPP benchmarks remain B2d work.
5. Existing B2b package/import tests remain low-level transport evidence. The
   new tests extend that evidence; they do not claim Burst, Jobs, player, or
   production-save-data support.

## Non-Goals

- A new `Execute(sql, parameters)` API or SQL-dialect interpolation.
- Choosing UnityEngine versus Unity Mathematics globally.
- Dimension-only semantic inference.
- Tensor/Sentis conversion, matrices, colors, bounds, integer vectors, or
  boolean vectors.
- Burst calls into DataVo, Jobs/Collections support, IL2CPP execution, and
  player support claims.
- Constructor binding, source generators, or implicit conversion magic that C#
  cannot add to third-party Unity types.

## Delivery Order

1. Add and test the Core registry plus `Get<T>`, `Map<T>`, and `Insert<T>` with
   non-Unity test converters.
2. Refactor the Unity package adapters to register the eight converters through
   a single `UseUnityTypes()` extension while preserving the low-level adapters.
3. Exercise the feature in the real Unity Editor smoke project and record
   allocation evidence appropriate to the narrow scope.
4. Design POCO constructor binding/source generation only after the reflection
   mapper has proven useful in real Unity tooling.
