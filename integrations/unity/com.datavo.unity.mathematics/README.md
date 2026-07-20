# DataVo Unity Mathematics

com.datavo.unity.mathematics is the experimental Unity Package Manager source
package for converting common floating-point Unity values to and from DataVo
VECTOR cells.

It contains two independent adapter assemblies:

- DataVo.Unity.Mathematics for Unity.Mathematics.float2, float3, float4, and
  quaternion.
- DataVo.Unity.EngineTypes for UnityEngine.Vector2, Vector3, Vector4, and
  Quaternion.

The adapters preserve component order (x,y, x,y,z, or x,y,z,w). Conversion from
a fixed-size Unity value creates one owned DataVo vector; conversion back copies
through caller-owned stack storage without a temporary managed vector array.

This source package requires the generated portable DataVo.Core candidate owned by
the future Unity smoke-project preparation flow. It does not supply a Core DLL or
claim that an arbitrary DataVo package is Unity-compatible.

## Status

This package is unverified until it has been imported and tested by the pinned
Unity 6000.5.3f1 Editor and an executed Windows x64 IL2CPP player. Static source
checks do not establish Unity, Burst, Mono, or IL2CPP support.

Do not call DataVo from Burst-compiled code. Native containers and the
Burst-to-managed drain boundary belong to the separate future
com.datavo.unity.jobs package.

Quaternion conversion is provided for storage and retrieval only. q and -q
represent the same rotation but are different ordinary vectors, so ordinary
L2/cosine vector distance is not quaternion angular distance. For that reason,
there is deliberately no quaternion nearest-neighbor overload.
