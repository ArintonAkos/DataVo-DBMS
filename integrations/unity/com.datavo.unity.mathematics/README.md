# DataVo Unity Mathematics

com.datavo.unity.mathematics is the experimental Unity Package Manager source
package for converting common floating-point Unity values to and from DataVo
VECTOR cells.

It contains two independent adapter assemblies:

- DataVo.Unity.Mathematics for Unity.Mathematics.float2, float3, float4, and
  quaternion.
- DataVo.Unity.EngineTypes for UnityEngine.Vector2, Vector3, Vector4, and
  Quaternion.

It also contains DataVo.Unity.TypeBinding. This is the normal application API:

```csharp
using DataVo.Unity;

context.UseUnityTypes();

PlayerState player = context.Map<PlayerState>(row);
Vector3 position = context.Get<Vector3>(row, "Position");
context.Insert("Players", player);
```

`UseUnityTypes()` is a void, per-context setup call. It registers both the
UnityEngine and Unity.Mathematics types together; there is no builder and no
global registry. `Map<T>` uses property types and `Get<T>` uses the explicit
generic type, so a `VECTOR(4)` is never guessed to be either `Vector4` or
`Quaternion` from its length alone.

The existing `ToDataVoCell()` and `As*()` extensions remain available as
low-level escape hatches. Normal model, bulk, and compiled-query writes do not
need to expose them in game code.

The adapters preserve component order (x,y, x,y,z, or x,y,z,w). Conversion from
a fixed-size Unity value creates one owned DataVo vector; conversion back copies
through caller-owned stack storage without a temporary managed vector array.

The package depends on `com.datavo.core` 0.5.1, which supplies the DataVo.Core
DLL: install the `com.datavo.core` UPM tarball first. The repository's smoke and
arena projects satisfy the dependency with a generated local `com.datavo.core`
package (`scripts/prepare-unity-candidate.sh`). This package ships no Core DLL.

## Status

The pinned Unity 6000.5.4f1 Editor and macOS IL2CPP Player have passed the
narrow automatic type-binding smoke. That result does not establish Windows,
other Unity versions or targets, Burst, Jobs, Mono, persistence, vector search,
or reactive-query support.

Do not call DataVo from Burst-compiled code. Native-container adapters and a
Burst-to-managed drain boundary are planned for a separate
`com.datavo.unity.jobs` package. That package is not implemented and does not
exist in this repository yet.

Quaternion conversion is provided for storage and retrieval only. q and -q
represent the same rotation but are different ordinary vectors, so ordinary
L2/cosine vector distance is not quaternion angular distance. For that reason,
there is deliberately no quaternion nearest-neighbor overload.
