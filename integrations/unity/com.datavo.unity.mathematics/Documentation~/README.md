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

The package must be paired with an exact portable DataVo.Core candidate produced by
the repository's later Unity preparation flow. It does not include a Core binary,
does not use unofficial Unity NuGet mirrors, and does not make DataVo callable
from Burst. Native container adapters and the explicit Burst-to-managed drain
bridge are planned for com.datavo.unity.jobs.

On Unity `6000.5.4f1`, Unity Mathematics `1.4.0` forwards its value types through
`UnityEngine.MathematicsModule`. The Mathematics assembly therefore retains Unity
engine-module references while still exposing only Unity Mathematics adapters.

## Verification status

Pinned Unity 6000.5.4f1 Editor/Mono, Burst, package import, and executed Windows
x64 IL2CPP verification have not run yet. The repository's static package
contract checks validate source layout and intended APIs only; they are not a
Unity support claim.
