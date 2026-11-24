#if !NET8_0_OR_GREATER
#pragma warning disable CS1591

// Compiler and trimming attributes the netstandard2.1 reference assemblies lack; the C# compiler and the trim
// analyzer bind to them by name. DataVo.Core keeps its own internal copies.
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit;
}

namespace System.Diagnostics.CodeAnalysis
{
    [Flags]
    internal enum DynamicallyAccessedMemberTypes
    {
        None = 0,
        PublicFields = 0x0010,
        PublicProperties = 0x0200,
    }

    [AttributeUsage(AttributeTargets.GenericParameter | AttributeTargets.Field | AttributeTargets.ReturnValue | AttributeTargets.Parameter | AttributeTargets.Method, Inherited = false)]
    internal sealed class DynamicallyAccessedMembersAttribute : Attribute
    {
        public DynamicallyAccessedMembersAttribute(DynamicallyAccessedMemberTypes memberTypes) => MemberTypes = memberTypes;

        public DynamicallyAccessedMemberTypes MemberTypes { get; }
    }

    [AttributeUsage(AttributeTargets.All, Inherited = false, AllowMultiple = true)]
    internal sealed class UnconditionalSuppressMessageAttribute : Attribute
    {
        public UnconditionalSuppressMessageAttribute(string category, string checkId)
        {
            Category = category;
            CheckId = checkId;
        }

        public string Category { get; }

        public string CheckId { get; }

        public string? Justification { get; set; }
    }
}
#pragma warning restore CS1591
#endif
