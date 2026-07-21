using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Security.Cryptography;
using System.Text.Json;

return CandidateInspector.Run(args);

internal static class CandidateInspector
{
    public static int Run(string[] args)
    {
        try
        {
            Options options = Options.Parse(args);
            List<AssemblyRecord> records = options.AssemblyPaths
                .Select(path => Inspect(path, options.RootDirectory))
                .OrderBy(record => record.File, StringComparer.Ordinal)
                .ToList();

            ValidateNoDuplicateIdentities(records);
            ValidateNoDuplicatePublicSystemTypes(records);

            string? outputDirectory = Path.GetDirectoryName(options.OutputPath);
            if (!string.IsNullOrEmpty(outputDirectory))
            {
                Directory.CreateDirectory(outputDirectory);
            }

            var inventory = new AssemblyInventory(1, records);
            File.WriteAllText(
                options.OutputPath,
                JsonSerializer.Serialize(
                    inventory,
                    new JsonSerializerOptions { WriteIndented = true, PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));

            return 0;
        }
        catch (ArgumentException exception)
        {
            Console.Error.WriteLine(exception.Message);
            return 64;
        }
        catch (InvalidOperationException exception)
        {
            Console.Error.WriteLine(exception.Message);
            return 1;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine(exception);
            return 1;
        }
    }

    private static AssemblyRecord Inspect(string assemblyPath, string rootDirectory)
    {
        string fullPath = Path.GetFullPath(assemblyPath);
        if (!File.Exists(fullPath))
        {
            throw new ArgumentException($"Assembly does not exist: {fullPath}", nameof(assemblyPath));
        }

        string relativePath = Path.GetRelativePath(rootDirectory, fullPath);
        if (relativePath == ".." ||
            relativePath.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal) ||
            Path.IsPathRooted(relativePath))
        {
            throw new ArgumentException($"Assembly must be inside --root: {fullPath}", nameof(assemblyPath));
        }

        AssemblyName assemblyName = AssemblyName.GetAssemblyName(fullPath);
        using FileStream stream = File.OpenRead(fullPath);
        using var peReader = new PEReader(stream);
        if (!peReader.HasMetadata)
        {
            throw new InvalidOperationException($"Assembly has no managed metadata: {fullPath}");
        }

        MetadataReader metadata = peReader.GetMetadataReader();
        string[] publicSystemTypes = metadata.TypeDefinitions
            .Select(handle => metadata.GetTypeDefinition(handle))
            .Where(type => (type.Attributes & TypeAttributes.VisibilityMask) == TypeAttributes.Public)
            .Select(type => (Namespace: metadata.GetString(type.Namespace), Name: metadata.GetString(type.Name)))
            .Where(type => type.Namespace == "System" || type.Namespace.StartsWith("System.", StringComparison.Ordinal))
            .Select(type => type.Namespace + "." + type.Name)
            .OrderBy(type => type, StringComparer.Ordinal)
            .ToArray();

        return new AssemblyRecord(
            relativePath.Replace(Path.DirectorySeparatorChar, '/'),
            ComputeSha256(fullPath),
            assemblyName.FullName ?? assemblyName.Name ?? "<unnamed>",
            publicSystemTypes);
    }

    private static void ValidateNoDuplicateIdentities(IEnumerable<AssemblyRecord> records)
    {
        var owners = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (AssemblyRecord record in records)
        {
            if (owners.TryGetValue(record.Identity, out string? owner))
            {
                throw new InvalidOperationException(
                    $"Duplicate assembly identity '{record.Identity}' in '{owner}' and '{record.File}'.");
            }

            owners.Add(record.Identity, record.File);
        }
    }

    private static void ValidateNoDuplicatePublicSystemTypes(IEnumerable<AssemblyRecord> records)
    {
        var owners = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (AssemblyRecord record in records)
        {
            foreach (string type in record.PublicSystemTypes)
            {
                if (owners.TryGetValue(type, out string? owner))
                {
                    throw new InvalidOperationException(
                        $"Duplicate public System type '{type}' in '{owner}' and '{record.File}'.");
                }

                owners.Add(type, record.File);
            }
        }
    }

    private static string ComputeSha256(string path)
    {
        using FileStream stream = File.OpenRead(path);
        using SHA256 hash = SHA256.Create();
        return Convert.ToHexString(hash.ComputeHash(stream)).ToLowerInvariant();
    }

    private sealed record Options(string RootDirectory, string OutputPath, IReadOnlyList<string> AssemblyPaths)
    {
        public static Options Parse(string[] args)
        {
            string? rootDirectory = null;
            string? outputPath = null;
            var assemblyPaths = new List<string>();

            for (int index = 0; index < args.Length; index++)
            {
                switch (args[index])
                {
                    case "--root":
                        rootDirectory = ReadValue(args, ref index, "--root");
                        break;
                    case "--output":
                        outputPath = ReadValue(args, ref index, "--output");
                        break;
                    default:
                        if (args[index].StartsWith("-", StringComparison.Ordinal))
                        {
                            throw new ArgumentException($"Unknown option: {args[index]}");
                        }

                        assemblyPaths.Add(args[index]);
                        break;
                }
            }

            if (string.IsNullOrWhiteSpace(rootDirectory))
            {
                throw new ArgumentException("Missing required --root path.");
            }

            if (!Directory.Exists(rootDirectory))
            {
                throw new ArgumentException($"Root directory does not exist: {rootDirectory}");
            }

            if (string.IsNullOrWhiteSpace(outputPath))
            {
                throw new ArgumentException("Missing required --output path.");
            }

            if (assemblyPaths.Count == 0)
            {
                throw new ArgumentException("Provide at least one assembly path.");
            }

            return new Options(
                Path.GetFullPath(rootDirectory),
                Path.GetFullPath(outputPath),
                assemblyPaths);
        }

        private static string ReadValue(string[] args, ref int index, string option)
        {
            if (index + 1 >= args.Length || args[index + 1].StartsWith("-", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Missing value for {option}.");
            }

            index++;
            return args[index];
        }
    }

    private sealed record AssemblyInventory(int SchemaVersion, IReadOnlyList<AssemblyRecord> Assemblies);

    private sealed record AssemblyRecord(
        string File,
        string Sha256,
        string Identity,
        IReadOnlyList<string> PublicSystemTypes);
}
