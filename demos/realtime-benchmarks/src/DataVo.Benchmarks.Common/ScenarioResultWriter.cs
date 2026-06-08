namespace DataVo.Benchmarks.Common;

public static class ScenarioResultWriter
{
    public static void Write(ScenarioRunResult result, string? outputPath)
    {
        string json = result.ToJson();
        Console.WriteLine(json);

        if (string.IsNullOrWhiteSpace(outputPath))
        {
            return;
        }

        string fullPath = Path.GetFullPath(outputPath);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        File.WriteAllText(fullPath, json + Environment.NewLine);
    }
}
