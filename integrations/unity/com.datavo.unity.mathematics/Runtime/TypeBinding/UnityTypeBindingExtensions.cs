#nullable enable

using System;
using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.ValueConversion;
using Unity.Mathematics;
using UnityEngine;

namespace DataVo.Unity
{
    /// <summary>Registers the UnityEngine and Unity.Mathematics VECTOR mappings for one DataVo context.</summary>
    public static class UnityTypeBindingExtensions
    {
        private static readonly IDataVoValueConverter[] Converters =
        {
            new DataVoValueConverter<Vector2>("DataVo.Unity.Vector2.v1", ToCell, ReadVector2),
            new DataVoValueConverter<Vector3>("DataVo.Unity.Vector3.v1", ToCell, ReadVector3),
            new DataVoValueConverter<Vector4>("DataVo.Unity.Vector4.v1", ToCell, ReadVector4),
            new DataVoValueConverter<Quaternion>("DataVo.Unity.Quaternion.v1", ToCell, ReadQuaternion),
            new DataVoValueConverter<float2>("DataVo.Unity.float2.v1", ToCell, ReadFloat2),
            new DataVoValueConverter<float3>("DataVo.Unity.float3.v1", ToCell, ReadFloat3),
            new DataVoValueConverter<float4>("DataVo.Unity.float4.v1", ToCell, ReadFloat4),
            new DataVoValueConverter<quaternion>("DataVo.Unity.quaternion.v1", ToCell, ReadMathQuaternion),
        };

        /// <summary>
        /// Registers automatic read/write conversion for Vector2/3/4, Quaternion, float2/3/4, and quaternion.
        /// Calling this method again is safe and does not create a new builder or context.
        /// </summary>
        public static void UseUnityTypes(this DataVoContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            context.RegisterValueConverters(Converters);
        }

        private static CellValue ToCell(Vector2 value)
        {
            Span<float> components = stackalloc float[2];
            components[0] = value.x;
            components[1] = value.y;
            return CellValue.FromVector(components);
        }

        private static CellValue ToCell(Vector3 value)
        {
            Span<float> components = stackalloc float[3];
            components[0] = value.x;
            components[1] = value.y;
            components[2] = value.z;
            return CellValue.FromVector(components);
        }

        private static CellValue ToCell(Vector4 value)
        {
            Span<float> components = stackalloc float[4];
            components[0] = value.x;
            components[1] = value.y;
            components[2] = value.z;
            components[3] = value.w;
            return CellValue.FromVector(components);
        }

        private static CellValue ToCell(Quaternion value)
        {
            Span<float> components = stackalloc float[4];
            components[0] = value.x;
            components[1] = value.y;
            components[2] = value.z;
            components[3] = value.w;
            return CellValue.FromVector(components);
        }

        private static CellValue ToCell(float2 value)
        {
            Span<float> components = stackalloc float[2];
            components[0] = value.x;
            components[1] = value.y;
            return CellValue.FromVector(components);
        }

        private static CellValue ToCell(float3 value)
        {
            Span<float> components = stackalloc float[3];
            components[0] = value.x;
            components[1] = value.y;
            components[2] = value.z;
            return CellValue.FromVector(components);
        }

        private static CellValue ToCell(float4 value)
        {
            Span<float> components = stackalloc float[4];
            components[0] = value.x;
            components[1] = value.y;
            components[2] = value.z;
            components[3] = value.w;
            return CellValue.FromVector(components);
        }

        private static CellValue ToCell(quaternion value)
        {
            Span<float> components = stackalloc float[4];
            components[0] = value.value.x;
            components[1] = value.value.y;
            components[2] = value.value.z;
            components[3] = value.value.w;
            return CellValue.FromVector(components);
        }

        private static Vector2 ReadVector2(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 2);
            return new Vector2(vector[0], vector[1]);
        }

        private static Vector3 ReadVector3(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 3);
            return new Vector3(vector[0], vector[1], vector[2]);
        }

        private static Vector4 ReadVector4(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 4);
            return new Vector4(vector[0], vector[1], vector[2], vector[3]);
        }

        private static Quaternion ReadQuaternion(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 4);
            return new Quaternion(vector[0], vector[1], vector[2], vector[3]);
        }

        private static float2 ReadFloat2(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 2);
            return new float2(vector[0], vector[1]);
        }

        private static float3 ReadFloat3(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 3);
            return new float3(vector[0], vector[1], vector[2]);
        }

        private static float4 ReadFloat4(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 4);
            return new float4(vector[0], vector[1], vector[2], vector[3]);
        }

        private static quaternion ReadMathQuaternion(object? value)
        {
            ReadOnlySpan<float> vector = RequireVector(value, 4);
            return new quaternion(vector[0], vector[1], vector[2], vector[3]);
        }

        private static ReadOnlySpan<float> RequireVector(object? value, int expectedLength)
        {
            if (value is not float[] vector)
            {
                throw new ArgumentException("Expected a materialized VECTOR(" + expectedLength + ").", nameof(value));
            }

            if (vector.Length != expectedLength)
            {
                throw new ArgumentException(
                    "Expected VECTOR(" + expectedLength + ") but received VECTOR(" + vector.Length + ").",
                    nameof(value));
            }

            return vector;
        }
    }
}
