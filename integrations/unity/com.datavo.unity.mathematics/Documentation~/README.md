# DataVo Unity Mathematics package

This experimental UPM package gives Unity projects direct adapters between DataVo
VECTOR cells and the common float-vector value types:

- Unity.Mathematics.float2, float3, float4, and quaternion.
- UnityEngine.Vector2, Vector3, Vector4, and Quaternion.

For each type, components are stored in x,y, x,y,z, or x,y,z,w order. Input uses
stack-backed fixed-size storage and CellValue.FromVector, so it creates one owned
vector allocation. Output verifies the expected vector length and uses
CellValue.CopyVectorTo without a temporary managed vector array.

SearchNearest extensions are available only for ordinary 2-, 3-, and 4-component
vectors. A quaternion is not an ordinary rotation-distance query: its antipode
encodes the same rotation, while cosine/L2 treat it as different.

## Automatic type binding

For normal game and tooling code, add `using DataVo.Unity;` and configure the
context once:

```csharp
context.UseUnityTypes();

PlayerState player = context.Map<PlayerState>(row);
Vector3 position = context.Get<Vector3>(row, "Position");
context.Insert("Players", player);
```

`UseUnityTypes()` returns `void`: it is a per-context setup action, not a
builder. It registers UnityEngine.Vector2/3/4 and Quaternion together with
Unity.Mathematics.float2/3/4 and quaternion. Property types drive `Map<T>`;
the explicit `T` drives `Get<T>`. This matters because a SQL `VECTOR(4)` cannot
be safely guessed as either `Vector4` or `Quaternion` from its dimension.

The existing `ToDataVoCell()` and `As*()` methods are still available for
low-level code. Application models and normal DataVo writes do not need to
mention them.

The package depends on `com.datavo.core` 0.5.1, which supplies the DataVo.Core
DLL; install the `com.datavo.core` UPM tarball first. It does not include a Core
binary, does not use unofficial Unity NuGet mirrors, and does not make DataVo
callable from Burst. Native-container adapters and an explicit Burst-to-managed
drain bridge are planned for a separate `com.datavo.unity.jobs` package, which is
not implemented and does not exist in this repository yet.

On Unity `6000.5.4f1`, Unity Mathematics `1.4.0` forwards its value types through
`UnityEngine.MathematicsModule`. The Mathematics assembly therefore retains Unity
engine-module references while still exposing only Unity Mathematics adapters.

## Verification status

The pinned Unity 6000.5.4f1 Editor and macOS IL2CPP Player have passed the
narrow automatic type-binding smoke (`tests/unity/DataVo.UnitySmoke`). That
result does not establish Windows, other Unity versions or targets, Burst, Jobs,
Mono, persistence, vector search, or reactive-query support. The repository's
static package contract checks (`scripts/test-unity-mathematics-package.sh`)
validate source layout and intended APIs only.
