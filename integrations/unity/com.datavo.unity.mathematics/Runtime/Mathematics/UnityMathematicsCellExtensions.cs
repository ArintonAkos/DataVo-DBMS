using System;
using System.Collections.Generic;
using DataVo.Core;
using Unity.Mathematics;

namespace DataVo.Unity.Mathematics
{
    /// <summary>Converts common Unity Mathematics values to and from DataVo VECTOR cells.</summary>
    public static class UnityMathematicsCellExtensions
    {
        /// <summary>Stores <paramref name="value"/> as VECTOR(2) in x,y order.</summary>
        public static CellValue ToDataVoCell(this float2 value)
        {
            Span<float> components = stackalloc float[2];
            components[0] = value.x;
            components[1] = value.y;
            return CellValue.FromVector(components);
        }

        /// <summary>Stores <paramref name="value"/> as VECTOR(3) in x,y,z order.</summary>
        public static CellValue ToDataVoCell(this float3 value)
        {
            Span<float> components = stackalloc float[3];
            components[0] = value.x;
            components[1] = value.y;
            components[2] = value.z;
            return CellValue.FromVector(components);
        }

        /// <summary>Stores <paramref name="value"/> as VECTOR(4) in x,y,z,w order.</summary>
        public static CellValue ToDataVoCell(this float4 value)
        {
            Span<float> components = stackalloc float[4];
            components[0] = value.x;
            components[1] = value.y;
            components[2] = value.z;
            components[3] = value.w;
            return CellValue.FromVector(components);
        }

        /// <summary>Stores <paramref name="value"/> as VECTOR(4) in x,y,z,w order without rotation normalization.</summary>
        public static CellValue ToDataVoCell(this quaternion value)
        {
            Span<float> components = stackalloc float[4];
            components[0] = value.value.x;
            components[1] = value.value.y;
            components[2] = value.value.z;
            components[3] = value.value.w;
            return CellValue.FromVector(components);
        }

        /// <summary>Reads a VECTOR(2) cell as a Unity Mathematics float2.</summary>
        public static float2 AsFloat2(this CellValue value)
        {
            Span<float> components = stackalloc float[2];
            CopyExact(value, components);
            return new float2(components[0], components[1]);
        }

        /// <summary>Reads a VECTOR(3) cell as a Unity Mathematics float3.</summary>
        public static float3 AsFloat3(this CellValue value)
        {
            Span<float> components = stackalloc float[3];
            CopyExact(value, components);
            return new float3(components[0], components[1], components[2]);
        }

        /// <summary>Reads a VECTOR(4) cell as a Unity Mathematics float4.</summary>
        public static float4 AsFloat4(this CellValue value)
        {
            Span<float> components = stackalloc float[4];
            CopyExact(value, components);
            return new float4(components[0], components[1], components[2], components[3]);
        }

        /// <summary>Reads a VECTOR(4) cell as a quaternion without rotation normalization.</summary>
        public static quaternion AsQuaternion(this CellValue value)
        {
            Span<float> components = stackalloc float[4];
            CopyExact(value, components);
            return new quaternion(components[0], components[1], components[2], components[3]);
        }

        /// <summary>Runs ordinary VECTOR(2) nearest-neighbor search for <paramref name="query"/>.</summary>
        public static List<Dictionary<string, object?>> SearchNearest(this DataVoContext context, string tableName, string indexName, float2 query, int topK = 10)
        {
            Span<float> components = stackalloc float[2];
            components[0] = query.x;
            components[1] = query.y;
            return Search(context, tableName, indexName, components, topK);
        }

        /// <summary>Runs ordinary VECTOR(3) nearest-neighbor search for <paramref name="query"/>.</summary>
        public static List<Dictionary<string, object?>> SearchNearest(this DataVoContext context, string tableName, string indexName, float3 query, int topK = 10)
        {
            Span<float> components = stackalloc float[3];
            components[0] = query.x;
            components[1] = query.y;
            components[2] = query.z;
            return Search(context, tableName, indexName, components, topK);
        }

        /// <summary>Runs ordinary VECTOR(4) nearest-neighbor search for <paramref name="query"/>.</summary>
        public static List<Dictionary<string, object?>> SearchNearest(this DataVoContext context, string tableName, string indexName, float4 query, int topK = 10)
        {
            Span<float> components = stackalloc float[4];
            components[0] = query.x;
            components[1] = query.y;
            components[2] = query.z;
            components[3] = query.w;
            return Search(context, tableName, indexName, components, topK);
        }

        private static void CopyExact(CellValue value, Span<float> destination)
        {
            int actual = value.VectorLength;
            int expected = destination.Length;
            if (actual != expected)
            {
                throw new ArgumentException(
                    "Expected VECTOR(" + expected + ") but received VECTOR(" + actual + ").",
                    nameof(value));
            }

            value.CopyVectorTo(destination);
        }

        private static List<Dictionary<string, object?>> Search(
            DataVoContext context,
            string tableName,
            string indexName,
            ReadOnlySpan<float> query,
            int topK)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            return context.SearchNearest(tableName, indexName, query, topK);
        }
    }
}
