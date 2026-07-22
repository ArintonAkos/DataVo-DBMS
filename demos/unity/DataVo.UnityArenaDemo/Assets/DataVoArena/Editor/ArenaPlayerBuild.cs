#nullable enable

using System;
using System.IO;
using System.Linq;
using UnityEditor;
using UnityEditor.Build;
using UnityEditor.Build.Reporting;
using UnityEditor.SceneManagement;
using UnityEngine.SceneManagement;

namespace DataVo.UnityArena.Editor
{
    public static class ArenaPlayerBuild
    {
        private const string GeneratedScenePath = "Assets/DataVoArena/.generated/ArenaHeadless.unity";

        public static void BuildMacOSIl2Cpp()
        {
            RequireMacIl2CppModule();

            string outputPath = RequireArgument("-datavoArenaBuildPath");
            string scenePath = EnsureGeneratedEmptyScene();
            var target = NamedBuildTarget.Standalone;
            ScriptingImplementation oldBackend = PlayerSettings.GetScriptingBackend(target);
            ManagedStrippingLevel oldStripping = PlayerSettings.GetManagedStrippingLevel(target);

            try
            {
                PlayerSettings.SetScriptingBackend(target, ScriptingImplementation.IL2CPP);
                PlayerSettings.SetManagedStrippingLevel(target, ManagedStrippingLevel.Medium);

                BuildReport report = BuildPipeline.BuildPlayer(new BuildPlayerOptions
                {
                    scenes = new[] { scenePath },
                    locationPathName = outputPath,
                    target = BuildTarget.StandaloneOSX,
                    options = BuildOptions.None
                });

                if (report.summary.result != BuildResult.Succeeded)
                {
                    throw new BuildFailedException(
                        "DataVo Unity arena macOS IL2CPP Player build failed: " + report.summary.result + ".");
                }
            }
            finally
            {
                PlayerSettings.SetScriptingBackend(target, oldBackend);
                PlayerSettings.SetManagedStrippingLevel(target, oldStripping);
            }
        }

        private static void RequireMacIl2CppModule()
        {
            string variationsPath = Path.Combine(GetEditorRoot(), "PlaybackEngines", "MacStandaloneSupport", "Variations");

            bool hasIl2CppVariation = Directory.Exists(variationsPath) &&
                Directory.EnumerateDirectories(variationsPath)
                    .Any(path => Path.GetFileName(path).EndsWith("_il2cpp", StringComparison.OrdinalIgnoreCase));

            if (!hasIl2CppVariation)
            {
                throw new InvalidOperationException(
                    "The pinned Unity 6000.5.4f1 Editor is missing Unity Hub module mac-il2cpp " +
                    "(Mac Build Support (IL2CPP)). Install that exact module; a Mono Player is not an acceptable fallback.");
            }
        }

        private static string GetEditorRoot()
        {
            var contents = new DirectoryInfo(EditorApplication.applicationContentsPath);
            DirectoryInfo? root = contents.Parent?.Parent;

            if (root is null)
            {
                throw new InvalidOperationException("Could not locate the pinned Unity Editor root.");
            }

            return root.FullName;
        }

        private static string RequireArgument(string name)
        {
            string[] arguments = Environment.GetCommandLineArgs();

            for (int index = 0; index < arguments.Length - 1; index++)
            {
                if (string.Equals(arguments[index], name, StringComparison.Ordinal))
                {
                    return Path.GetFullPath(arguments[index + 1]);
                }
            }

            throw new ArgumentException("Missing required Unity batch argument " + name + ".");
        }

        private static string EnsureGeneratedEmptyScene()
        {
            if (File.Exists(GeneratedScenePath))
            {
                return GeneratedScenePath;
            }

            string? directory = Path.GetDirectoryName(GeneratedScenePath);

            if (directory is null)
            {
                throw new InvalidOperationException("Could not determine the generated arena Player scene directory.");
            }

            Directory.CreateDirectory(directory);
            // ArenaHeadlessBootstrap is retained by RuntimeInitializeOnLoadMethod, so this
            // deliberately empty scene avoids coupling the Player proof to visual content.
            Scene scene = EditorSceneManager.NewScene(NewSceneSetup.EmptyScene, NewSceneMode.Single);

            if (!EditorSceneManager.SaveScene(scene, GeneratedScenePath, saveAsCopy: false))
            {
                throw new InvalidOperationException("Could not create the generated arena Player scene.");
            }

            return GeneratedScenePath;
        }
    }
}
