#nullable enable

using System;
using System.Globalization;
using System.IO;
using Unity.Profiling;
using UnityEngine;

namespace DataVo.UnityArena
{
    public sealed record ArenaHeadlessInvocation(
        ArenaDeliveryMode mode,
        string runId,
        int measuredTicks);

    public static class ArenaHeadlessBootstrap
    {
        private const int DefaultMeasuredTicks = 300;
        private const int MaximumMeasuredTicks = 1_000_000;

        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.AfterSceneLoad)]
        private static void Start()
        {
            string[] arguments = Environment.GetCommandLineArgs();

            if (!HasModeArgument(arguments))
            {
                return;
            }

            ArenaHeadlessInvocation? invocation = null;
            ArenaRunResult result;
            int exitCode;

            try
            {
                invocation = ParseInvocation(arguments);
                ProfilerRecorder allocationRecorder = default;
                bool hasAllocationRecorder = TryStartManagedAllocationRecorder(out allocationRecorder);

                try
                {
                    result = ArenaBenchmark.Run(new ArenaRunOptions
                    {
                        Mode = invocation.mode,
                        MeasuredTicks = invocation.measuredTicks
                    });
                    PopulateRuntimeMetadata(result, allocationRecorder, hasAllocationRecorder);
                }
                finally
                {
                    if (hasAllocationRecorder)
                    {
                        allocationRecorder.Dispose();
                    }
                }

                RequireInternallyConsistent(result, invocation);
                exitCode = 0;
            }
            catch (Exception exception)
            {
                result = FailedResult(invocation, exception.Message);
                exitCode = 1;
            }

            try
            {
                WriteResult(invocation?.runId ?? SafeFallbackRunId(arguments), result);
            }
            catch (Exception exception)
            {
                Debug.LogError("DataVo arena benchmark could not write its result: " + Bound(exception.Message));
                exitCode = 1;
            }

            Application.Quit(exitCode);
        }

        public static ArenaHeadlessInvocation ParseInvocation(string[] arguments)
        {
            if (arguments is null)
            {
                throw new ArgumentNullException(nameof(arguments));
            }

            string? modeText = null;
            string? runId = null;
            int measuredTicks = DefaultMeasuredTicks;

            for (int index = 0; index < arguments.Length; index++)
            {
                string argument = arguments[index];

                if (argument == "-datavoArenaMode")
                {
                    modeText = ReadValue(arguments, ref index, argument);
                }
                else if (argument == "-datavoArenaRunId")
                {
                    runId = ReadValue(arguments, ref index, argument);
                }
                else if (argument == "-datavoArenaTicks")
                {
                    string ticksText = ReadValue(arguments, ref index, argument);

                    if (!int.TryParse(ticksText, NumberStyles.None, CultureInfo.InvariantCulture, out measuredTicks) ||
                        measuredTicks <= 0 || measuredTicks > MaximumMeasuredTicks)
                    {
                        throw new ArgumentException("-datavoArenaTicks must be an integer from 1 to " + MaximumMeasuredTicks + ".");
                    }
                }
            }

            ArenaDeliveryMode mode = modeText switch
            {
                "reactive" => ArenaDeliveryMode.Reactive,
                "polling" => ArenaDeliveryMode.Polling,
                null => throw new ArgumentException("Missing required -datavoArenaMode reactive|polling Player argument."),
                _ => throw new ArgumentException("-datavoArenaMode must be reactive or polling.")
            };

            if (runId is null || !IsSafeRunId(runId))
            {
                throw new ArgumentException("-datavoArenaRunId must contain 1 to 128 letters, digits, underscores, or hyphens.");
            }

            return new ArenaHeadlessInvocation(mode, runId, measuredTicks);
        }

        private static void PopulateRuntimeMetadata(
            ArenaRunResult result,
            ProfilerRecorder allocationRecorder,
            bool hasAllocationRecorder)
        {
            result.unityVersion = Application.unityVersion;
            result.platform = Application.platform.ToString();
            result.scriptingBackend = ScriptingBackendName();

            if (hasAllocationRecorder && allocationRecorder.Valid)
            {
                result.hasManagedAllocationBytes = true;
                result.managedAllocationBytes = allocationRecorder.LastValue;
            }
        }

        private static bool TryStartManagedAllocationRecorder(out ProfilerRecorder recorder)
        {
            try
            {
                recorder = ProfilerRecorder.StartNew(ProfilerCategory.Memory, "GC Allocated In Frame");
                return recorder.Valid;
            }
            catch (ArgumentException)
            {
                recorder = default;
                return false;
            }
        }

        private static void RequireInternallyConsistent(ArenaRunResult result, ArenaHeadlessInvocation invocation)
        {
            if (result.schemaVersion != 1 || result.status != "passed" || result.mode != ModeName(invocation.mode) ||
                result.measuredTicks != invocation.measuredTicks ||
                result.tick.count != invocation.measuredTicks || result.leaderboardChecksum.Length != 64 ||
                result.aggregateChecksum.Length != 64 || !string.IsNullOrEmpty(result.error))
            {
                throw new InvalidOperationException("Arena benchmark result did not satisfy its reporting contract.");
            }
        }

        private static ArenaRunResult FailedResult(ArenaHeadlessInvocation? invocation, string error)
        {
            var result = new ArenaRunResult
            {
                schemaVersion = 1,
                status = "failed",
                mode = ModeName(invocation?.mode ?? ArenaDeliveryMode.Reactive),
                seed = 20260721,
                measuredTicks = invocation?.measuredTicks ?? 0,
                unityVersion = Application.unityVersion,
                platform = Application.platform.ToString(),
                scriptingBackend = ScriptingBackendName(),
                error = Bound(error)
            };

            return result;
        }

        private static string ModeName(ArenaDeliveryMode mode)
        {
            return mode == ArenaDeliveryMode.Polling ? "polling" : "reactive";
        }

        private static void WriteResult(string runId, ArenaRunResult result)
        {
            string path = Path.Combine(Application.persistentDataPath, "DataVoArena", runId, "result.json");
            string? directory = Path.GetDirectoryName(path);

            if (directory is null)
            {
                throw new InvalidOperationException("Could not determine the arena benchmark result directory.");
            }

            Directory.CreateDirectory(directory);
            File.WriteAllText(path, JsonUtility.ToJson(result));
            Debug.Log("DATAVO_ARENA_RESULT=" + path);
        }

        private static bool HasModeArgument(string[] arguments)
        {
            foreach (string argument in arguments)
            {
                if (argument == "-datavoArenaMode")
                {
                    return true;
                }
            }

            return false;
        }

        private static string ReadValue(string[] arguments, ref int index, string option)
        {
            if (index + 1 >= arguments.Length || string.IsNullOrWhiteSpace(arguments[index + 1]))
            {
                throw new ArgumentException(option + " requires a value.");
            }

            index++;
            return arguments[index];
        }

        private static bool IsSafeRunId(string value)
        {
            if (value.Length is < 1 or > 128)
            {
                return false;
            }

            foreach (char character in value)
            {
                if (!char.IsLetterOrDigit(character) && character != '_' && character != '-')
                {
                    return false;
                }
            }

            return true;
        }

        private static string SafeFallbackRunId(string[] arguments)
        {
            for (int index = 0; index < arguments.Length - 1; index++)
            {
                if (arguments[index] == "-datavoArenaRunId" && IsSafeRunId(arguments[index + 1]))
                {
                    return arguments[index + 1];
                }
            }

            return "invalid-run";
        }

        private static string ScriptingBackendName()
        {
#if ENABLE_IL2CPP
            return "IL2CPP";
#elif ENABLE_MONO
            return "Mono";
#else
            return "Unknown";
#endif
        }

        private static string Bound(string value)
        {
            const int maximumLength = 1_024;
            string singleLine = value.Replace('\r', ' ').Replace('\n', ' ');
            return singleLine.Length <= maximumLength ? singleLine : singleLine.Substring(0, maximumLength);
        }
    }
}
