#!/usr/bin/env bash
set -euo pipefail

mode="${1:-all}"
case "$mode" in
  --metadata|--mathematics|--engine-types|--type-binding|--all)
    ;;
  *)
    echo "Usage: $0 [--metadata|--mathematics|--engine-types|--type-binding|--all]" >&2
    exit 64
    ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package_root="$repo_root/integrations/unity/com.datavo.unity.mathematics"
manifest="$package_root/package.json"
mathematics_asmdef="$package_root/Runtime/Mathematics/DataVo.Unity.Mathematics.asmdef"
engine_types_asmdef="$package_root/Runtime/EngineTypes/DataVo.Unity.EngineTypes.asmdef"
mathematics_source="$package_root/Runtime/Mathematics/UnityMathematicsCellExtensions.cs"
engine_types_source="$package_root/Runtime/EngineTypes/UnityEngineCellExtensions.cs"
type_binding_asmdef="$package_root/Runtime/TypeBinding/DataVo.Unity.TypeBinding.asmdef"
type_binding_source="$package_root/Runtime/TypeBinding/UnityTypeBindingExtensions.cs"

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "Missing required Unity package file: $1" >&2
    exit 1
  fi
}

require_file "$manifest"
require_file "$mathematics_asmdef"
require_file "$engine_types_asmdef"
require_file "$package_root/README.md"
require_file "$package_root/Documentation~/README.md"

node - "$manifest" "$mathematics_asmdef" "$engine_types_asmdef" <<'NODE'
const fs = require("node:fs");

const [manifestPath, mathematicsAsmdefPath, engineTypesAsmdefPath] = process.argv.slice(2);

function readJson(path) {
  try {
    return JSON.parse(fs.readFileSync(path, "utf8"));
  } catch (error) {
    console.error("Could not parse JSON at " + path + ": " + error.message);
    process.exit(1);
  }
}

function assert(condition, message) {
  if (!condition) {
    console.error(message);
    process.exit(1);
  }
}

function equalArrays(actual, expected, message) {
  assert(
    Array.isArray(actual) &&
      actual.length === expected.length &&
      actual.every((value, index) => value === expected[index]),
    message,
  );
}

const manifest = readJson(manifestPath);
assert(manifest.name === "com.datavo.unity.mathematics", "UPM package name must be com.datavo.unity.mathematics.");
assert(manifest.version === "0.1.0-preview.1", "UPM package version must be 0.1.0-preview.1.");
assert(manifest.unity === "6000.0", "UPM package must declare Unity 6000.0.");
assert(
  manifest.dependencies &&
    Object.keys(manifest.dependencies).length === 1 &&
    manifest.dependencies["com.unity.mathematics"] === "1.4.0",
  "UPM package must declare only com.unity.mathematics 1.4.0.",
);

const mathematicsAsmdef = readJson(mathematicsAsmdefPath);
assert(mathematicsAsmdef.name === "DataVo.Unity.Mathematics", "Mathematics asmdef name is incorrect.");
equalArrays(
  mathematicsAsmdef.references,
  ["Unity.Mathematics"],
  "Mathematics asmdef must reference only Unity.Mathematics as an asmdef.",
);
assert(mathematicsAsmdef.overrideReferences === true, "Mathematics asmdef must override automatic plugin references.");
equalArrays(
  mathematicsAsmdef.precompiledReferences,
  ["DataVo.Core.dll"],
  "Mathematics asmdef must explicitly reference the DataVo.Core plugin.",
);
assert(mathematicsAsmdef.noEngineReferences === false, "Mathematics asmdef must retain UnityEngine module references required by Unity Mathematics 1.4.0.");

const engineTypesAsmdef = readJson(engineTypesAsmdefPath);
assert(engineTypesAsmdef.name === "DataVo.Unity.EngineTypes", "EngineTypes asmdef name is incorrect.");
equalArrays(
  engineTypesAsmdef.references,
  [],
  "EngineTypes asmdef must not reference source asmdefs.",
);
assert(engineTypesAsmdef.overrideReferences === true, "EngineTypes asmdef must override automatic plugin references.");
equalArrays(
  engineTypesAsmdef.precompiledReferences,
  ["DataVo.Core.dll"],
  "EngineTypes asmdef must explicitly reference the DataVo.Core plugin.",
);
assert(engineTypesAsmdef.noEngineReferences === false, "EngineTypes asmdef must retain UnityEngine references.");
NODE

if [[ "$mode" == "--metadata" ]]; then
  echo "Unity Mathematics package metadata contract passed."
  exit 0
fi

assert_source_contract() {
  local source_path="$1"
  local namespace_name="$2"
  local vector2_name="$3"
  local vector3_name="$4"
  local vector4_name="$5"
  local quaternion_name="$6"
  local as_vector2_name="$7"
  local as_vector3_name="$8"
  local as_vector4_name="$9"
  local as_quaternion_name="${10}"

  require_file "$source_path"

  rg -Fq "#nullable enable" "$source_path"
  rg -Fq "namespace $namespace_name" "$source_path"
  rg -Fq "ToDataVoCell(this $vector2_name value)" "$source_path"
  rg -Fq "ToDataVoCell(this $vector3_name value)" "$source_path"
  rg -Fq "ToDataVoCell(this $vector4_name value)" "$source_path"
  rg -Fq "ToDataVoCell(this $quaternion_name value)" "$source_path"
  rg -Fq "As$as_vector2_name(this CellValue value)" "$source_path"
  rg -Fq "As$as_vector3_name(this CellValue value)" "$source_path"
  rg -Fq "As$as_vector4_name(this CellValue value)" "$source_path"
  rg -Fq "As$as_quaternion_name(this CellValue value)" "$source_path"
  rg -Fq "SearchNearest(this DataVoContext context" "$source_path"
  rg -Fq "CellValue.FromVector" "$source_path"
  rg -Fq "value.CopyVectorTo" "$source_path"
  rg -Fq "value.VectorLength" "$source_path"
  rg -Fq "stackalloc float" "$source_path"
  rg -Fq "value.x" "$source_path"
  rg -Fq "value.y" "$source_path"
  rg -Fq "value.z" "$source_path"
  rg -Fq "value.w" "$source_path"

  if rg -n -P "SearchNearest\\([^)]*\\b$quaternion_name\\b" "$source_path" >/dev/null; then
    echo "Quaternion nearest-neighbor overloads are intentionally unsupported: $source_path" >&2
    exit 1
  fi
}

if [[ "$mode" == "--mathematics" || "$mode" == "--all" ]]; then
  assert_source_contract "$mathematics_source" "DataVo.Unity.Mathematics" "float2" "float3" "float4" "quaternion" "Float2" "Float3" "Float4" "Quaternion"

  if rg -Fq "UnityEngine" "$mathematics_source"; then
    echo "Mathematics adapter must not reference UnityEngine." >&2
    exit 1
  fi
fi

if [[ "$mode" == "--engine-types" || "$mode" == "--all" ]]; then
  assert_source_contract "$engine_types_source" "DataVo.Unity.EngineTypes" "Vector2" "Vector3" "Vector4" "Quaternion" "Vector2" "Vector3" "Vector4" "Quaternion"
  rg -Fq "using UnityEngine;" "$engine_types_source"
fi

if [[ "$mode" == "--type-binding" || "$mode" == "--all" ]]; then
  require_file "$type_binding_asmdef"
  require_file "$type_binding_source"

  node - "$type_binding_asmdef" <<'NODE'
const fs = require("node:fs");
const asmdefPath = process.argv[2];

function assert(condition, message) {
  if (!condition) {
    console.error(message);
    process.exit(1);
  }
}

const asmdef = JSON.parse(fs.readFileSync(asmdefPath, "utf8"));
assert(asmdef.name === "DataVo.Unity.TypeBinding", "TypeBinding asmdef name is incorrect.");
assert(
  Array.isArray(asmdef.references) && asmdef.references.length === 1 && asmdef.references[0] === "Unity.Mathematics",
  "TypeBinding asmdef must reference Unity.Mathematics only.",
);
assert(asmdef.overrideReferences === true, "TypeBinding asmdef must override automatic plugin references.");
assert(
  Array.isArray(asmdef.precompiledReferences) &&
    asmdef.precompiledReferences.length === 1 &&
    asmdef.precompiledReferences[0] === "DataVo.Core.dll",
  "TypeBinding asmdef must explicitly reference the DataVo.Core plugin.",
);
assert(asmdef.noEngineReferences === false, "TypeBinding asmdef must retain UnityEngine references.");
NODE

  rg -Fq "#nullable enable" "$type_binding_source"
  rg -Fq "namespace DataVo.Unity" "$type_binding_source"
  rg -Fq "using DataVo.Core.ValueConversion;" "$type_binding_source"
  rg -Fq "public static void UseUnityTypes(this DataVoContext context)" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<Vector2>" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<Vector3>" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<Vector4>" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<Quaternion>" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<float2>" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<float3>" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<float4>" "$type_binding_source"
  rg -Fq "new DataVoValueConverter<quaternion>" "$type_binding_source"
  rg -Fq "context.RegisterValueConverters(" "$type_binding_source"
fi

echo "Unity Mathematics package $mode contract passed."
