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

The package must be paired with an exact portable DataVo.Core candidate produced by
the repository's later Unity preparation flow. It does not include a Core binary,
does not use unofficial Unity NuGet mirrors, and does not make DataVo callable
from Burst. Native container adapters and the explicit Burst-to-managed drain
bridge are planned for com.datavo.unity.jobs.

## Verification status

Pinned Unity 6000.5.4f1 Editor/Mono, Burst, package import, and executed Windows
x64 IL2CPP verification have not run yet. The repository's static package
contract checks validate source layout and intended APIs only; they are not a
Unity support claim.
