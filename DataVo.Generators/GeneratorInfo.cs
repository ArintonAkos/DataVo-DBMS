using System.Reflection;

namespace DataVo.Generators;

/// <summary>Facts about this generator assembly that generated code records.</summary>
internal static class GeneratorInfo
{
    /// <summary>
    /// The package version (from the assembly's informational version, without build metadata), written into
    /// <c>[GeneratedCode]</c> attributes so generated code names the generator release that produced it.
    /// </summary>
    public static readonly string Version = ResolveVersion();

    private static string ResolveVersion()
    {
        string? informational = typeof(GeneratorInfo).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (string.IsNullOrWhiteSpace(informational))
        {
            return typeof(GeneratorInfo).Assembly.GetName().Version?.ToString(3) ?? "0.0.0";
        }

        int metadata = informational!.IndexOf('+');
        return metadata > 0 ? informational.Substring(0, metadata) : informational;
    }
}
