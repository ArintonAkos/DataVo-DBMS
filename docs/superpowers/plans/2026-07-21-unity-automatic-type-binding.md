# Unity Automatic Type Binding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Add checked automatic Unity VECTOR conversion through UseUnityTypes(), Get<T>(), Map<T>(), and Insert<T>(), while leaving DataVo.Core Unity-free.

**Architecture:** A per-context Core registry stores CLR type converters and is first exercised with a fake vector type. Cached model bindings route writes through InsertTyped and materialized reads through the registry. A new UPM assembly registers all eight Unity types using a non-fluent void UseUnityTypes() extension. An internal bound-cell carrier crosses legacy dictionary and compiled DML boundaries without text-vector round trips.

**Tech Stack:** C# (net10.0 and opt-in netstandard2.1), xUnit, Unity 6000.5.4f1, Unity Mathematics 1.4.0, NUnit, UPM, Bash.

## Global Constraints

- DataVo.Core never references a Unity type or assembly.
- void UseUnityTypes(this DataVoContext context) registers both UnityEngine and Unity Mathematics types atomically and idempotently. It is not a builder API.
- Reads must use Get<T>() or a model property type. Never infer Quaternion versus Vector4 from VECTOR(4).
- Preserve ToDataVoCell()/As*() as low-level APIs. Do not add string-SQL parameters, tensors, Jobs/Burst, Collections, IL2CPP, or a general Unity support claim.
- A typed Unity vector write owns exactly one float array; a typed Unity read creates no temporary managed vector array.
- Map<T>() supports public parameterless models with public writable properties only. Constructor binding, attributes, and source generation are out of scope.
- The pinned Editor is /Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity, revision d550df8bd089.
- Stage exact paths only and preserve unrelated untracked user files. Force-stage docs/superpowers files.

## File Structure

| File | Responsibility |
| --- | --- |
| DataVo.Core/ValueConversion/IDataVoValueConverter.cs | Unity-free erased converter contract. |
| DataVo.Core/ValueConversion/DataVoValueConverter.cs | Generic integration-facing converter wrapper. |
| DataVo.Core/ValueConversion/DataVoValueConverterRegistry.cs | Atomic, per-context registration and typed reads. |
| DataVo.Core/ValueConversion/DataVoModelBinding.cs | Cached Map<T>/Insert<T> reflection bindings. |
| DataVo.Core/ValueConversion/DataVoBoundValue.cs | Internal converted-cell carrier for legacy writes. |
| DataVo.Core/DataVoContext.cs | Public registration, read, and model APIs. |
| DataVo.Core/Parser/DML/InsertRowService.cs | Bound-cell validation and vector-index span routing. |
| DataVo.Core/StorageEngine/Serialization/RowSerializer.cs | Direct bound-cell serialization. |
| DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs | Compiled INSERT/UPDATE binding. |
| DataVo.Tests/E2E/ValueConversionRegistryTests.cs | Registry/Get<T> fake-vector tests. |
| DataVo.Tests/E2E/AutomaticModelBindingTests.cs | Map<T>/Insert<T>/BulkInsert fake-vector tests. |
| integrations/unity/com.datavo.unity.mathematics/Runtime/TypeBinding/* | Unity converter group and asmdef. |
| tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/* | Real Unity EditMode proof. |

## Task 1: Create the Core registry and Get<T>()

**Files:**
- Create: DataVo.Core/ValueConversion/IDataVoValueConverter.cs
- Create: DataVo.Core/ValueConversion/DataVoValueConverter.cs
- Create: DataVo.Core/ValueConversion/DataVoValueConverterRegistry.cs
- Modify: DataVo.Core/DataVoContext.cs
- Create: DataVo.Tests/E2E/ValueConversionRegistryTests.cs

**Produces:**

    public interface IDataVoValueConverter
    {
        Type ValueType { get; }
        string RegistrationKey { get; }
        CellValue ToCellValue(object value);
        object FromMaterialized(object? value);
    }

    public sealed class DataVoValueConverter<T> : IDataVoValueConverter
    {
        public DataVoValueConverter(string registrationKey, Func<T, CellValue> toCellValue,
            Func<object?, T> fromMaterialized);
    }

    public void RegisterValueConverters(params IDataVoValueConverter[] converters);
    public T Get<T>(IReadOnlyDictionary<string, object?> row, string columnName);

- [x] **Step 1: Write the failing tests**

Create a local TestVector(float X, float Y) and this converter:

    private static readonly DataVoValueConverter<TestVector> TestVectorConverter = new(
        "DataVo.Tests.TestVector.v1",
        value => CellValue.FromVector([value.X, value.Y]),
        value => value is float[] vector && vector.Length == 2
            ? new TestVector(vector[0], vector[1])
            : throw new ArgumentException("Expected VECTOR(2).", nameof(value)));

Test Get<TestVector>() from a materialized float array, direct built-in reads, missing-column KeyNotFoundException, unregistered target NotSupportedException, context isolation, atomic rejection of duplicate converter types in one group, and same-key idempotency.

- [x] **Step 2: Confirm red**

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --filter FullyQualifiedName~ValueConversionRegistryTests

Expected: compilation fails because the converter API does not exist.

- [x] **Step 3: Implement the registry**

The generic wrapper validates a non-empty key and both delegates, and rejects a wrong boxed input type. RegisterRange validates every candidate before mutation: no nulls, no duplicate ValueType values in the incoming group, and no pre-existing different key. Same-key registration is a no-op.

Add a private readonly DataVoValueConverterRegistry to DataVoContext. Get<T>() looks up the name, returns an assignable built-in directly, otherwise dispatches to an exact registered converter. It must not use global state, dynamic, Unity, or reflection.

- [x] **Step 4: Verify green and portable compilation**

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --filter FullyQualifiedName~ValueConversionRegistryTests
    dotnet build DataVo.Core/DataVo.Core.csproj -p:DataVoEnablePortableTarget=true

Expected: focused tests pass and both Core frameworks build.

- [x] **Step 5: Commit Task 1**

    git add DataVo.Core/ValueConversion/IDataVoValueConverter.cs DataVo.Core/ValueConversion/DataVoValueConverter.cs DataVo.Core/ValueConversion/DataVoValueConverterRegistry.cs DataVo.Core/DataVoContext.cs DataVo.Tests/E2E/ValueConversionRegistryTests.cs
    git commit -m "feat: add context value conversion registry"

## Task 2: Add cached Map<T>() and typed Insert<T>()

**Files:**
- Create: DataVo.Core/ValueConversion/DataVoModelBinding.cs
- Modify: DataVo.Core/DataVoContext.cs
- Create: DataVo.Tests/E2E/AutomaticModelBindingTests.cs

**Produces:**

    public T Map<T>(IReadOnlyDictionary<string, object?> row);
    public long Insert<T>(string tableName, T entity);

- [x] **Step 1: Write failing model tests**

Use TestTransformRow with public Id, TestVector Position, and string Label properties; use ProjectionRow with TestVector Position and int NotSelected. Test that Map<TestTransformRow>() converts VECTOR(2), ignores NotSelected when it is absent from a projection, propagates a wrong-dimension converter error, and Insert("Transforms", entity) writes a row in Transforms (Id INT PRIMARY KEY, Position VECTOR(2), Label VARCHAR(20)). Test that a missing catalog-column property throws before any row is written.

- [x] **Step 2: Confirm red**

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --filter FullyQualifiedName~AutomaticModelBindingTests

Expected: compilation fails because Map<T>() and Insert<T>() do not exist.

- [x] **Step 3: Implement cached bindings**

Cache read bindings by model Type and write bindings by (model Type, database, table, catalog schema version). Ignore result fields without writable matching properties. Require public parameterless construction, writable non-indexer properties for reads, and a readable property for every catalog column for writes. Write bindings use catalog order, build a ReactiveRowSchema, convert every value through Task 1, construct all cells, and only then call InsertTyped.

- [x] **Step 4: Verify green with typed-insert regressions**

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~AutomaticModelBindingTests|FullyQualifiedName~InsertTypedTests"

Expected: model tests and existing typed inserts pass.

- [x] **Step 5: Commit Task 2**

    git add DataVo.Core/ValueConversion/DataVoModelBinding.cs DataVo.Core/DataVoContext.cs DataVo.Tests/E2E/AutomaticModelBindingTests.cs
    git commit -m "feat: map typed DataVo model values"

## Task 3: Bind BulkInsert and compiled DML without vector text conversion

**Files:**
- Create: DataVo.Core/ValueConversion/DataVoBoundValue.cs
- Modify: DataVo.Core/DataVoContext.cs
- Modify: DataVo.Core/Parser/DML/InsertRowService.cs
- Modify: DataVo.Core/StorageEngine/Serialization/RowSerializer.cs
- Modify: DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs
- Modify: DataVo.Tests/E2E/AutomaticModelBindingTests.cs
- Modify: DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs

- [x] **Step 1: Write failing boundary tests**

Register TestVectorConverter. Verify BulkInsert accepts TestVector for a VECTOR(2) column. Add a compiled insert plan:

    DataVoCompiledQueryPlan.Insert("Samples", ["Id", "Position"], ["id", "position"])

Pass TestVector as the position parameter and round-trip it with Get<TestVector>(). Add an equivalent compiled UPDATE assignment test.

- [x] **Step 2: Confirm red**

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~AutomaticModelBindingTests|FullyQualifiedName~CompiledQueryRuntimeTests"

Expected: legacy paths reject TestVector as unsupported VECTOR input.

- [x] **Step 3: Implement bound-cell transport**

DataVoBoundValue wraps a converted CellValue. While copying caller dictionaries, BulkInsert replaces registered values with this carrier. InsertRowService.TryNormalizeValue recognizes it before FormatColumnValue, validates its scalar/vector cell as InsertTyped does, and retains it. RowSerializer.WriteNonNullValue recognizes it and calls WriteTypedCell, so no string format/parse occurs. Vector-index insertion reads its AsVectorReadOnlySpan; all ordinary legacy values retain the VectorParser branch.

Add an internal context binder and call it from compiled INSERT and UPDATE before normal column coercion. Do not change raw parameter behavior or fixed-width compiled overloads.

- [x] **Step 4: Verify green and legacy regressions**

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~AutomaticModelBindingTests|FullyQualifiedName~CompiledQueryRuntimeTests|FullyQualifiedName~GameRuntimeBulkInsertTests"

Expected: model, dictionary, compiled INSERT, and compiled UPDATE work with registered vectors; existing bulk/vector-index tests pass.

- [x] **Step 5: Commit Task 3**

    git add DataVo.Core/ValueConversion/DataVoBoundValue.cs DataVo.Core/DataVoContext.cs DataVo.Core/Parser/DML/InsertRowService.cs DataVo.Core/StorageEngine/Serialization/RowSerializer.cs DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs DataVo.Tests/E2E/AutomaticModelBindingTests.cs DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs
    git commit -m "feat: bind registered values across DataVo writes"

## Task 4: Add the Unity UseUnityTypes() group

**Files:**
- Create: integrations/unity/com.datavo.unity.mathematics/Runtime/TypeBinding/DataVo.Unity.TypeBinding.asmdef
- Create: integrations/unity/com.datavo.unity.mathematics/Runtime/TypeBinding/UnityTypeBindingExtensions.cs
- Modify: scripts/test-unity-mathematics-package.sh
- Modify: integrations/unity/com.datavo.unity.mathematics/README.md
- Modify: integrations/unity/com.datavo.unity.mathematics/Documentation~/README.md

**Produces:**

    namespace DataVo.Unity;
    public static class UnityTypeBindingExtensions
    {
        public static void UseUnityTypes(this DataVoContext context);
    }

- [x] **Step 1: Add a failing static package contract**

Add a --type-binding mode to scripts/test-unity-mathematics-package.sh. Require an asmdef named DataVo.Unity.TypeBinding with Unity.Mathematics source reference, DataVo.Core.dll precompiled reference, overrideReferences true, and noEngineReferences false. Require the exact void extension signature and all eight Unity type names.

Run:

    bash scripts/test-unity-mathematics-package.sh --type-binding

Expected: failure because TypeBinding does not exist.

- [x] **Step 2: Implement all eight converters**

Create the asmdef. Add static DataVoValueConverter instances for Vector2, Vector3, Vector4, Quaternion, float2, float3, float4, and quaternion. Writers use stackalloc plus CellValue.FromVector; readers consume materialized float arrays, require exact dimensions, and construct values directly. Quaternion copies x/y/z/w without normalization. UseUnityTypes null-checks, calls RegisterValueConverters once with all eight, and returns void. It must not reference the two existing adapter assemblies or create an asmdef cycle.

- [x] **Step 3: Document and verify**

Document:

    context.UseUnityTypes();
    PlayerState player = context.Map<PlayerState>(row);
    Vector3 position = context.Get<Vector3>(row, "Position");
    context.Insert("Players", player);

Explain why VECTOR(4) cannot automatically choose Quaternion versus Vector4 and retain the existing low-level API guidance.

Run:

    bash scripts/test-unity-mathematics-package.sh --all

Expected: old adapter and new registration contracts pass.

- [x] **Step 4: Commit Task 4**

    git add integrations/unity/com.datavo.unity.mathematics/Runtime/TypeBinding integrations/unity/com.datavo.unity.mathematics/README.md integrations/unity/com.datavo.unity.mathematics/Documentation~/README.md scripts/test-unity-mathematics-package.sh
    git commit -m "feat: register automatic Unity type binding"

## Task 5: Prove automatic binding in Unity 6000.5.4f1

**Files:**
- Modify: tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/DataVo.UnitySmoke.EditorTests.asmdef
- Modify: tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/AdapterImportTests.cs
- Create: DataVo.Tests/Performance/ValueConversionAllocationTests.cs
- Modify: matching Unity meta files only if Unity changes them
- Modify: docs/superpowers/plans/2026-07-21-unity-automatic-type-binding.md

- [x] **Step 1: Write failing Unity tests**

Reference DataVo.Unity.TypeBinding. Add AutoBoundState with Vector3 Position, float3 Velocity, Quaternion Rotation, and quaternion SimulationRotation. Create a table, call UseUnityTypes(), insert the model, select the row, and assert Map<AutoBoundState>() and Get<Vector3>() reproduce values. Add Vector2/float2 coverage and a Get<Vector3>() from VECTOR(2) error mentioning VECTOR(3) and VECTOR(2). Compare quaternion components exactly.

- [x] **Step 2: Confirm red**

Run:

    bash scripts/run-unity-editor-tests.sh

Expected: Unity compile failure because the TypeBinding API/reference is missing.

- [x] **Step 3: Verify green in the real Editor**

Run:

    bash scripts/check-unity-smoke-project.sh --require-candidate
    bash scripts/run-unity-editor-tests.sh

Expected: Unity XML top-level result Passed, failed 0, with old and automatic-binding cases.

- [x] **Step 4: Add narrow allocation evidence**

Warm the fake converter and binding cache. In ValueConversionAllocationTests, measure GC.GetAllocatedBytesForCurrentThread around direct Insert<T>() and a Get<TestVector>() loop. Bound write allocation to its one owned vector/cell/model overhead and verify reads do not create a second vector array. State in assertion text that this is not a zero-allocation mapper or Unity player-performance claim.

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~ValueConversionAllocationTests|FullyQualifiedName~AutomaticModelBindingTests"

Expected: allocation and functional tests pass.

- [x] **Step 5: Run full verification, record evidence, and commit**

Run:

    dotnet test DataVo.Tests/DataVo.Tests.csproj --no-build --no-restore --settings scripts/xunit-serial.runsettings
    bash scripts/test-unity-mathematics-package.sh --all
    bash scripts/test-prepare-unity-candidate.sh
    bash scripts/check-unity-smoke-project.sh --require-candidate
    bash scripts/run-unity-editor-tests.sh
    bash scripts/check-unity-support-status.sh
    git diff --check HEAD

Record Unity revision, result count, lock SHA-256, allocation thresholds, and B2c/B2d deferrals in this plan. Commit:

    git add DataVo.Tests/Performance/ValueConversionAllocationTests.cs
    git add -f tests/unity/DataVo.UnitySmoke/Assets docs/superpowers/plans/2026-07-21-unity-automatic-type-binding.md
    git commit -m "test: prove Unity automatic type binding"

## Completion evidence — 2026-07-21

- Core slices committed as `166d290`, `e56c736`, and `87a387a`; Unity type registration as `0fad822`; Editor/allocations coverage as `e92e2ee`.
- `dotnet build DataVo.Core/DataVo.Core.csproj -p:DataVoEnablePortableTarget=true` passed for `netstandard2.1` and `net10.0`. The pre-existing `TopKReactiveQuery` nullable warnings remain outside this work.
- Focused Core coverage passed: registry/Get, Map/Insert, BulkInsert, compiled INSERT/UPDATE, HNSW update paths, and allocation guard. The allocation guard warms the binding/cache, caps one direct `Insert<T>` at 8,192 bytes, and caps 2,000 `Get<T>` reads at 1,024 bytes. It explicitly does not claim a zero-allocation mapper or Unity player performance.
- Pinned Editor: Unity `6000.5.4f1`, revision `d550df8bd089`. The real EditMode XML recorded `total=6`, `passed=6`, `failed=0`; package-lock SHA-256 was `58b87ce5a7c2adc18a4839708e215aae36b4b527592dacf1d1ed708c514e780d` before/after the run.
- `bash scripts/test-unity-mathematics-package.sh --all`, `bash scripts/test-prepare-unity-candidate.sh`, `bash scripts/check-unity-smoke-project.sh --require-candidate`, `bash scripts/run-unity-editor-tests.sh`, and `bash scripts/check-unity-support-status.sh` passed. Candidate provenance was built from `e92e2ee`.
- The requested full serial xUnit command was invoked but is not clean: unrelated existing Disk-mode tests share/corrupt `Catalog.xml` and then fail with `XmlException` (for example `ChangeCaptureIntegrationTests` and `ReactiveRecursiveCteTests`). The new focused tests pass; this plan does not change disk-test infrastructure.
- B2c/B2d remain deferred: no Burst/Jobs bridge, IL2CPP/player proof, persistent Unity save-data support, or broad gameplay benchmark claim is introduced by this slice.
