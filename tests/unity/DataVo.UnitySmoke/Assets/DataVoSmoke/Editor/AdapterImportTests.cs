#nullable enable

using System;
using DataVo.Unity.EngineTypes;
using DataVo.Unity.Mathematics;
using NUnit.Framework;
using Unity.Mathematics;
using UnityEngine;

namespace DataVo.UnitySmoke.EditorTests
{
    public sealed class AdapterImportTests
    {
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
    }
}
