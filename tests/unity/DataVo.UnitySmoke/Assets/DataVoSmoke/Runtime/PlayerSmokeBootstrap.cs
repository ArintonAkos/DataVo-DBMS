#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;
using DataVo.Unity;
using Unity.Mathematics;
using UnityEngine;

namespace DataVo.UnitySmoke.Player
{
    internal static partial class PlayerSmokeBootstrap
    {
        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.AfterSceneLoad)]
        private static void Start()
        {
            string runId;

            try
            {
                runId = GetRequiredRunId();
            }
            catch (Exception exception)
            {
                Debug.LogError("DataVo Unity Player smoke startup failed: " + Bound(exception.Message));
                Application.Quit(1);
                return;
            }

            PlayerSmokeResult result;

            try
            {
                result = RunScenario();
            }
            catch (Exception exception)
            {
                result = PlayerSmokeResult.Failed(Bound(exception.Message));
            }

            int exitCode = result.status == "passed" ? 0 : 1;

            try
            {
                WriteResult(runId, result);
            }
            catch (Exception exception)
            {
                Debug.LogError("DataVo Unity Player smoke could not write its result: " + Bound(exception.Message));
                exitCode = 1;
            }

            Application.Quit(exitCode);
        }

        private static PlayerSmokeResult RunScenario()
        {
            using var context = CreateInMemoryContext();
            context.UseUnityTypes();
            context.Execute(
                "CREATE TABLE States (" +
                "Id INT PRIMARY KEY, " +
                "Position VECTOR(3), " +
                "Velocity VECTOR(3), " +
                "Rotation VECTOR(4), " +
                "SimulationRotation VECTOR(4), " +
                "ScreenPosition VECTOR(2), " +
                "Drift VECTOR(2), " +
                "Four VECTOR(4), " +
                "MathematicsFour VECTOR(4))");

            var source = new PlayerSmokeState
            {
                Id = 1,
                Position = new Vector3(1.25f, -2f, 3.5f),
                Velocity = new float3(-4f, 5.25f, 6f),
                Rotation = new Quaternion(2f, -3f, 4f, -5f),
                SimulationRotation = new quaternion(-6f, 7f, -8f, 9f),
                ScreenPosition = new Vector2(10f, -11f),
                Drift = new float2(-12f, 13f),
                Four = new Vector4(14f, -15f, 16f, -17f),
                MathematicsFour = new float4(-18f, 19f, -20f, 21f)
            };

            context.Insert("States", source);

            var row = context.Execute(
                "SELECT Id, Position, Velocity, Rotation, SimulationRotation, ScreenPosition, Drift, Four, MathematicsFour " +
                "FROM States")
                .Single()
                .Data
                .Single();
            PlayerSmokeState mapped = context.Map<PlayerSmokeState>(row);
            Vector3 position = context.Get<Vector3>(row, "Position");

            AssertEquivalent(source, mapped, position);
            AssertVectorLengthFailure(context);
            RunPackageScenarios();
            AssertSaveDataPersists(StorageMode.Lsm);
            AssertSaveDataPersists(StorageMode.Disk);

            return PlayerSmokeResult.Passed();
        }

        // Implemented only when verify-unity-upm-package.sh installs the real package.
        static partial void RunPackageScenarios();

        private static void AssertSaveDataPersists(StorageMode mode)
        {
            string path = Path.Combine(Application.persistentDataPath, "DataVoSmokeSave_" + mode + "_" + Guid.NewGuid().ToString("N"));
            var config = new DataVoConfig { StorageMode = mode, DiskStoragePath = path };

            try
            {
                using (var writer = new DataVoContext(config))
                {
                    Run(writer, "CREATE DATABASE Save");
                    Run(writer, "USE Save");
                    Run(writer, "CREATE TABLE Inventory (Id INT PRIMARY KEY, Qty INT, Item VARCHAR(30))");
                    Run(writer, "CREATE INDEX ix_qty ON Inventory (Qty)");

                    for (int id = 1; id <= 2000; id++)
                        Run(writer, "INSERT INTO Inventory (Id, Qty, Item) VALUES (" + id + ", " + (id % 20) + ", 'item" + id + "')");

                    Run(writer, "UPDATE Inventory SET Qty = 777 WHERE Id = 1");
                    Run(writer, "DELETE FROM Inventory WHERE Id = 2");
                }

                using var reader = new DataVoContext(config);
                Run(reader, "USE Save");
                Require(Run(reader, "SELECT Id FROM Inventory").Data.Count == 1999, mode + ": wrong row count after reopen.");
                Require(Equals(Run(reader, "SELECT Qty FROM Inventory WHERE Id = 1").Data.Single()["Qty"], 777), mode + ": update was lost.");
                Require(Run(reader, "SELECT Id FROM Inventory WHERE Id = 2").Data.Count == 0, mode + ": delete was lost.");
                Require(Run(reader, "SELECT Id FROM Inventory WHERE Qty = 3").Data.Count == 100, mode + ": index was wrong after reopen.");
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, recursive: true);
            }
        }

        private static DataVo.Core.Contracts.Results.QueryResult Run(DataVoContext context, string sql)
        {
            var result = context.Execute(sql).Last();
            Require(!result.IsError, sql + " failed: " + string.Join(" | ", result.Messages));
            return result;
        }

        private static DataVoContext CreateInMemoryContext()
        {
            var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            string databaseName = "UnityPlayerSmoke_" + Guid.NewGuid().ToString("N");
            context.Execute("CREATE DATABASE " + databaseName);
            context.Execute("USE " + databaseName);
            return context;
        }

        private static void AssertEquivalent(PlayerSmokeState expected, PlayerSmokeState actual, Vector3 position)
        {
            Require(actual.Id == expected.Id, "Mapped Id differs.");
            AssertVector2(expected.ScreenPosition, actual.ScreenPosition, "ScreenPosition");
            AssertVector3(expected.Position, actual.Position, "Position");
            AssertVector3(expected.Position, position, "Get<Vector3>(Position)");
            AssertFloat2(expected.Drift, actual.Drift, "Drift");
            AssertFloat3(expected.Velocity, actual.Velocity, "Velocity");
            AssertVector4(expected.Four, actual.Four, "Four");
            AssertFloat4(expected.MathematicsFour, actual.MathematicsFour, "MathematicsFour");
            AssertQuaternion(expected.Rotation, actual.Rotation, "Rotation");
            AssertQuaternion(expected.SimulationRotation, actual.SimulationRotation, "SimulationRotation");
        }

        private static void AssertVectorLengthFailure(DataVoContext context)
        {
            var row = new Dictionary<string, object?>
            {
                ["Position"] = new[] { 1f, 2f }
            };

            try
            {
                context.Get<Vector3>(row, "Position");
            }
            catch (ArgumentException exception)
            {
                Require(exception.Message.Contains("VECTOR(3)", StringComparison.Ordinal), "Dimension error did not name VECTOR(3).");
                Require(exception.Message.Contains("VECTOR(2)", StringComparison.Ordinal), "Dimension error did not name VECTOR(2).");
                return;
            }

            throw new InvalidOperationException("VECTOR(2) converted to Vector3 without an error.");
        }

        private static void WriteResult(string runId, PlayerSmokeResult result)
        {
            string path = Path.Combine(Application.persistentDataPath, "DataVoSmoke", runId, "result.json");
            string? directory = Path.GetDirectoryName(path);

            if (directory is null)
                throw new InvalidOperationException("Could not determine the Player smoke report directory.");

            Directory.CreateDirectory(directory);
            File.WriteAllText(path, JsonUtility.ToJson(result));
            Debug.Log("DATAVO_SMOKE_RESULT=" + path);
        }

        private static string GetRequiredRunId()
        {
            string[] arguments = Environment.GetCommandLineArgs();

            for (int index = 0; index < arguments.Length - 1; index++)
            {
                if (!string.Equals(arguments[index], "-datavoSmokeRunId", StringComparison.Ordinal))
                    continue;

                string runId = arguments[index + 1];

                if (IsSafeRunId(runId))
                    return runId;

                throw new ArgumentException("-datavoSmokeRunId must contain 1 to 128 letters, digits, underscores, or hyphens.");
            }

            throw new ArgumentException("Missing required -datavoSmokeRunId Player argument.");
        }

        private static bool IsSafeRunId(string value)
        {
            if (value.Length is < 1 or > 128)
                return false;

            foreach (char character in value)
            {
                if (!char.IsLetterOrDigit(character) && character is not '_' and not '-')
                    return false;
            }

            return true;
        }

        private static void AssertVector2(Vector2 expected, Vector2 actual, string name)
        {
            Require(expected.x == actual.x && expected.y == actual.y, name + " differs.");
        }

        private static void AssertVector3(Vector3 expected, Vector3 actual, string name)
        {
            Require(expected.x == actual.x && expected.y == actual.y && expected.z == actual.z, name + " differs.");
        }

        private static void AssertVector4(Vector4 expected, Vector4 actual, string name)
        {
            Require(expected.x == actual.x && expected.y == actual.y && expected.z == actual.z && expected.w == actual.w, name + " differs.");
        }

        private static void AssertFloat2(float2 expected, float2 actual, string name)
        {
            Require(expected.x == actual.x && expected.y == actual.y, name + " differs.");
        }

        private static void AssertFloat3(float3 expected, float3 actual, string name)
        {
            Require(expected.x == actual.x && expected.y == actual.y && expected.z == actual.z, name + " differs.");
        }

        private static void AssertFloat4(float4 expected, float4 actual, string name)
        {
            Require(expected.x == actual.x && expected.y == actual.y && expected.z == actual.z && expected.w == actual.w, name + " differs.");
        }

        private static void AssertQuaternion(Quaternion expected, Quaternion actual, string name)
        {
            Require(
                expected.x == actual.x && expected.y == actual.y && expected.z == actual.z && expected.w == actual.w,
                name + " differs.");
        }

        private static void AssertQuaternion(quaternion expected, quaternion actual, string name)
        {
            AssertFloat4(expected.value, actual.value, name);
        }

        private static void Require(bool condition, string message)
        {
            if (!condition)
                throw new InvalidOperationException(message);
        }

        private static string Bound(string message)
        {
            return message.Length <= 1024 ? message : message.Substring(0, 1024);
        }

        [Serializable]
        private sealed class PlayerSmokeResult
        {
            public int schemaVersion;
            public string status = string.Empty;
            public string unityVersion = string.Empty;
            public string platform = string.Empty;
            public string scriptingBackend = string.Empty;
            public int scenarioCount;
            public string error = string.Empty;

            public static PlayerSmokeResult Passed()
            {
                return Create("passed", string.Empty);
            }

            public static PlayerSmokeResult Failed(string error)
            {
                return Create("failed", error);
            }

            private static PlayerSmokeResult Create(string status, string error)
            {
                return new PlayerSmokeResult
                {
                    schemaVersion = 1,
                    status = status,
                    unityVersion = Application.unityVersion,
                    platform = Application.platform.ToString(),
                    scriptingBackend = ScriptingBackend,
                    scenarioCount = status == "passed" ? 3 : 0,
                    error = error
                };
            }

#if ENABLE_IL2CPP
            private const string ScriptingBackend = "IL2CPP";
#else
            private const string ScriptingBackend = "non-il2cpp";
#endif
        }
    }

    public sealed class PlayerSmokeState
    {
        public int Id { get; set; }
        public Vector3 Position { get; set; }
        public float3 Velocity { get; set; }
        public Quaternion Rotation { get; set; }
        public quaternion SimulationRotation { get; set; }
        public Vector2 ScreenPosition { get; set; }
        public float2 Drift { get; set; }
        public Vector4 Four { get; set; }
        public float4 MathematicsFour { get; set; }
    }
}
