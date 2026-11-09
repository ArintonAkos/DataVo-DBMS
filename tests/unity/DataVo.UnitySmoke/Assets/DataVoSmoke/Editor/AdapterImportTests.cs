#nullable enable

using System;
using System.Linq;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;
using DataVo.Unity;
using DataVo.Unity.EngineTypes;
using DataVo.Unity.Mathematics;
using NUnit.Framework;
using Unity.Mathematics;
using UnityEngine;

namespace DataVo.UnitySmoke.EditorTests
{
    public sealed class AdapterImportTests
    {
        [TestCase(StorageMode.Lsm)]
        [TestCase(StorageMode.Disk)]
        public void SaveData_Survives_Closing_And_Reopening(StorageMode mode)
        {
            string path = System.IO.Path.Combine(Application.temporaryCachePath, "DataVoSave_" + mode + "_" + Guid.NewGuid().ToString("N"));
            var config = new DataVoConfig { StorageMode = mode, DiskStoragePath = path };

            try
            {
                using (var writer = new DataVoContext(config))
                {
                    Run(writer, "CREATE DATABASE Save");
                    Run(writer, "USE Save");
                    Run(writer, "CREATE TABLE Inventory (Id INT PRIMARY KEY, Qty INT)");

                    for (int id = 1; id <= 500; id++)
                        Run(writer, "INSERT INTO Inventory (Id, Qty) VALUES (" + id + ", " + id * 2 + ")");

                    Run(writer, "UPDATE Inventory SET Qty = 777 WHERE Id = 1");
                    Run(writer, "DELETE FROM Inventory WHERE Id = 2");
                }

                using var reader = new DataVoContext(config);
                Run(reader, "USE Save");

                Assert.That(Run(reader, "SELECT Id FROM Inventory").Data.Count, Is.EqualTo(499));
                Assert.That(Run(reader, "SELECT Qty FROM Inventory WHERE Id = 1").Data.Single()["Qty"], Is.EqualTo(777));
            }
            finally
            {
                if (System.IO.Directory.Exists(path))
                    System.IO.Directory.Delete(path, recursive: true);
            }
        }

        private static DataVo.Core.Contracts.Results.QueryResult Run(DataVoContext context, string sql)
        {
            var result = context.Execute(sql).Last();
            Assert.That(result.IsError, Is.False, sql + " failed: " + string.Join(" | ", result.Messages));
            return result;
        }

        [Test]
        public void MathematicsFloat3_RoundTrips_XYZ()
        {
            var source = new float3(1.25f, -2f, 3.5f);

            Assert.That(source.ToDataVoCell().AsFloat3(), Is.EqualTo(source));
        }

        [Test]
        public void EngineVector3_RoundTrips_XYZ()
        {
            var source = new Vector3(-4f, 5.25f, 6f);

            Assert.That(source.ToDataVoCell().AsVector3(), Is.EqualTo(source));
        }

        [Test]
        public void MathematicsQuaternion_Retains_All_Four_Components_Without_Normalization()
        {
            var source = new quaternion(2f, -3f, 4f, -5f);
            var roundTrip = UnityMathematicsCellExtensions.AsQuaternion(source.ToDataVoCell());

            Assert.That(roundTrip.value, Is.EqualTo(source.value));
        }

        [Test]
        public void Wrong_Vector_Length_Reports_Expected_And_Actual_Dimensions()
        {
            var exception = Assert.Throws<ArgumentException>(() => new float2(1f, 2f).ToDataVoCell().AsFloat3());

            Assert.That(exception!.Message, Does.Contain("Expected VECTOR(3) but received VECTOR(2)."));
        }

        [Test]
        public void UseUnityTypes_Inserts_Maps_And_Gets_All_Initial_Unity_Vector_Types()
        {
            using var context = CreateContext();
            context.UseUnityTypes();
            context.UseUnityTypes();
            context.Execute(
                "CREATE TABLE States (Id INT PRIMARY KEY, Position VECTOR(3), Velocity VECTOR(3), Rotation VECTOR(4), " +
                "SimulationRotation VECTOR(4), ScreenPosition VECTOR(2), Drift VECTOR(2))");

            var source = new AutoBoundState
            {
                Id = 1,
                Position = new Vector3(1.25f, -2f, 3.5f),
                Velocity = new float3(-4f, 5.25f, 6f),
                Rotation = new Quaternion(2f, -3f, 4f, -5f),
                SimulationRotation = new quaternion(-6f, 7f, -8f, 9f),
                ScreenPosition = new Vector2(10f, -11f),
                Drift = new float2(-12f, 13f)
            };

            context.Insert("States", source);

            var row = context.Execute(
                "SELECT Id, Position, Velocity, Rotation, SimulationRotation, ScreenPosition, Drift FROM States")
                .Single()
                .Data
                .Single();
            AutoBoundState mapped = context.Map<AutoBoundState>(row);
            Vector3 position = context.Get<Vector3>(row, "Position");

            Assert.That(mapped.Id, Is.EqualTo(source.Id));
            Assert.That(mapped.Position, Is.EqualTo(source.Position));
            Assert.That(mapped.Velocity, Is.EqualTo(source.Velocity));
            Assert.That(mapped.ScreenPosition, Is.EqualTo(source.ScreenPosition));
            Assert.That(mapped.Drift, Is.EqualTo(source.Drift));
            Assert.That(position, Is.EqualTo(source.Position));
            AssertQuaternion(mapped.Rotation, source.Rotation);
            Assert.That(mapped.SimulationRotation.value, Is.EqualTo(source.SimulationRotation.value));
        }

        [Test]
        public void Get_When_Vector_Length_Does_Not_Match_Target_Reports_Both_Dimensions()
        {
            using var context = CreateContext();
            context.UseUnityTypes();

            var row = new System.Collections.Generic.Dictionary<string, object?>
            {
                ["Position"] = new[] { 1f, 2f }
            };

            var exception = Assert.Throws<ArgumentException>(() => context.Get<Vector3>(row, "Position"));

            Assert.That(exception!.Message, Does.Contain("VECTOR(3)"));
            Assert.That(exception.Message, Does.Contain("VECTOR(2)"));
        }

        private static DataVoContext CreateContext()
        {
            var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            string databaseName = "UnityBinding_" + Guid.NewGuid().ToString("N");
            context.Execute("CREATE DATABASE " + databaseName);
            context.Execute("USE " + databaseName);
            return context;
        }

        private static void AssertQuaternion(Quaternion actual, Quaternion expected)
        {
            Assert.That(actual.x, Is.EqualTo(expected.x));
            Assert.That(actual.y, Is.EqualTo(expected.y));
            Assert.That(actual.z, Is.EqualTo(expected.z));
            Assert.That(actual.w, Is.EqualTo(expected.w));
        }

        public sealed class AutoBoundState
        {
            public int Id { get; set; }
            public Vector3 Position { get; set; }
            public float3 Velocity { get; set; }
            public Quaternion Rotation { get; set; }
            public quaternion SimulationRotation { get; set; }
            public Vector2 ScreenPosition { get; set; }
            public float2 Drift { get; set; }
        }
    }
}
