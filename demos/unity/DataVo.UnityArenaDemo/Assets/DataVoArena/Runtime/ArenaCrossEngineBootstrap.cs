#nullable enable

using System;
using System.Globalization;
using System.IO;
using UnityEngine;

namespace DataVo.UnityArena
{
    public sealed record ArenaCrossEngineInvocation(
        ArenaCrossEngineKind engine,
        ArenaCrossEngineProfile profile,
        string runId,
        int repetitions);

    public static class ArenaCrossEngineBootstrap
    {
        private const int DefaultRepetitions = 1;
        private const int MaximumRepetitions = 7;

        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.AfterSceneLoad)]
        private static void Start()
        {
            string[] arguments = Environment.GetCommandLineArgs();
            if (!HasEngineArgument(arguments))
            {
                return;
            }

            ArenaCrossEngineInvocation? invocation = null;
            ArenaCrossEngineResult result;
            int exitCode;

            try
            {
                invocation = ParseInvocation(arguments);
                result = ArenaCrossEngineRunner.Run(
                    invocation.profile,
                    invocation.engine,
                    invocation.repetitions);
                PopulateRuntimeMetadata(result);
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
                Debug.LogError("DataVo cross-engine arena benchmark could not write its result: " + Bound(exception.Message));
                exitCode = 1;
            }

            Application.Quit(exitCode);
        }

        public static ArenaCrossEngineInvocation ParseInvocation(string[] arguments)
        {
            if (arguments is null)
            {
                throw new ArgumentNullException(nameof(arguments));
            }

            string? engineText = null;
            string? profileText = null;
            string? runId = null;
            int repetitions = DefaultRepetitions;
            bool repetitionsSpecified = false;

            for (int index = 0; index < arguments.Length; index++)
            {
                string argument = arguments[index];
                switch (argument)
                {
                    case "-datavoArenaEngine":
                        engineText = ReadUniqueValue(arguments, ref index, argument, engineText);
                        break;
                    case "-datavoArenaProfile":
                        profileText = ReadUniqueValue(arguments, ref index, argument, profileText);
                        break;
                    case "-datavoArenaRunId":
                        runId = ReadUniqueValue(arguments, ref index, argument, runId);
                        break;
                    case "-datavoArenaRepetitions":
                        if (repetitionsSpecified)
                        {
                            throw new ArgumentException(argument + " may be specified only once.");
                        }

                        string repetitionsText = ReadValue(arguments, ref index, argument);
                        if (!int.TryParse(repetitionsText, NumberStyles.None, CultureInfo.InvariantCulture, out repetitions) ||
                            repetitions is < 1 or > MaximumRepetitions)
                        {
                            throw new ArgumentException(
                                "-datavoArenaRepetitions must be an integer from 1 to " + MaximumRepetitions + ".");
                        }

                        repetitionsSpecified = true;

                        break;
                }
            }

            ArenaCrossEngineKind engine = engineText switch
            {
                "datavo-reactive" => ArenaCrossEngineKind.DataVoReactive,
                "datavo-polling" => ArenaCrossEngineKind.DataVoPolling,
                "sqlite" => ArenaCrossEngineKind.SqlitePolling,
                null => throw new ArgumentException(
                    "Missing required -datavoArenaEngine datavo-reactive|datavo-polling|sqlite Player argument."),
                _ => throw new ArgumentException(
                    "-datavoArenaEngine must be datavo-reactive, datavo-polling, or sqlite.")
            };

            if (profileText is null)
            {
                throw new ArgumentException("Missing required -datavoArenaProfile gameplay|ivm-stress Player argument.");
            }

            ArenaCrossEngineProfile profile = ArenaCrossEngineProfile.Parse(profileText);
            if (runId is null || !IsSafeRunId(runId))
            {
                throw new ArgumentException(
                    "-datavoArenaRunId must contain 1 to 128 letters, digits, underscores, or hyphens.");
            }

            return new ArenaCrossEngineInvocation(engine, profile, runId, repetitions);
        }

        private static void PopulateRuntimeMetadata(ArenaCrossEngineResult result)
        {
            result.unityVersion = Application.unityVersion;
            result.platform = Application.platform.ToString();
            result.scriptingBackend = ScriptingBackendName();
        }

        private static void RequireInternallyConsistent(
            ArenaCrossEngineResult result,
            ArenaCrossEngineInvocation invocation)
        {
            if (result.schemaVersion != 1 ||
                result.schemaRevision != ArenaCrossEngineWorkload.SchemaRevision ||
                result.status != "passed" ||
                result.engine != EngineName(invocation.engine) ||
                result.profile != invocation.profile.id ||
                result.seed != invocation.profile.seed ||
                result.measuredBatches != invocation.profile.measuredBatches ||
                result.repetitions is null ||
                result.repetitions.Length != invocation.repetitions ||
                !IsLowercaseHash(result.leaderboardChecksum) ||
                !IsLowercaseHash(result.aggregateChecksum) ||
                !IsLowercaseHash(result.zoneInventoryChecksum) ||
                !string.IsNullOrEmpty(result.error) ||
                (invocation.engine == ArenaCrossEngineKind.SqlitePolling
                    ? string.IsNullOrWhiteSpace(result.sqliteVersion)
                    : !string.IsNullOrEmpty(result.sqliteVersion)))
            {
                throw new InvalidOperationException("Cross-engine arena result did not satisfy its reporting contract.");
            }

            for (int index = 0; index < result.repetitions.Length; index++)
            {
                ArenaCrossEngineRepetition repetition = result.repetitions[index];
                if (repetition is null || repetition.index != index + 1 ||
                    repetition.batch.count != invocation.profile.measuredBatches ||
                    repetition.leaderboard.count != invocation.profile.measuredBatches ||
                    repetition.aggregate.count != invocation.profile.measuredBatches ||
                    repetition.zoneInventory.count != invocation.profile.measuredBatches)
                {
                    throw new InvalidOperationException("Cross-engine arena repetition did not satisfy its reporting contract.");
                }
            }
        }

        private static ArenaCrossEngineResult FailedResult(ArenaCrossEngineInvocation? invocation, string error)
        {
            ArenaCrossEngineProfile? profile = invocation?.profile;
            return new ArenaCrossEngineResult
            {
                schemaVersion = 1,
                schemaRevision = ArenaCrossEngineWorkload.SchemaRevision,
                status = "failed",
                engine = EngineName(invocation?.engine ?? ArenaCrossEngineKind.DataVoReactive),
                profile = profile?.id ?? string.Empty,
                seed = profile?.seed ?? 0,
                measuredBatches = profile?.measuredBatches ?? 0,
                repetitions = Array.Empty<ArenaCrossEngineRepetition>(),
                unityVersion = Application.unityVersion,
                platform = Application.platform.ToString(),
                scriptingBackend = ScriptingBackendName(),
                sqliteVersion = string.Empty,
                error = Bound(error)
            };
        }

        private static void WriteResult(string runId, ArenaCrossEngineResult result)
        {
            string path = Path.GetFullPath(Path.Combine(
                Application.persistentDataPath,
                "DataVoArenaCrossEngine",
                runId,
                "result.json"));
            string? directory = Path.GetDirectoryName(path);
            if (directory is null)
            {
                throw new InvalidOperationException("Could not determine the cross-engine arena result directory.");
            }

            Directory.CreateDirectory(directory);
            File.WriteAllText(path, JsonUtility.ToJson(result));
            Debug.Log("DATAVO_ARENA_CROSS_ENGINE_RESULT=" + path);
        }

        private static bool HasEngineArgument(string[] arguments)
        {
            foreach (string argument in arguments)
            {
                if (argument == "-datavoArenaEngine")
                {
                    return true;
                }
            }

            return false;
        }

        private static string ReadUniqueValue(string[] arguments, ref int index, string option, string? existingValue)
        {
            if (existingValue is not null)
            {
                throw new ArgumentException(option + " may be specified only once.");
            }

            return ReadValue(arguments, ref index, option);
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
                bool isAsciiLetter = (character >= 'A' && character <= 'Z') ||
                    (character >= 'a' && character <= 'z');
                bool isAsciiDigit = character >= '0' && character <= '9';
                if (!isAsciiLetter && !isAsciiDigit && character != '_' && character != '-')
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

        private static bool IsLowercaseHash(string value)
        {
            if (value is null || value.Length != 64)
            {
                return false;
            }

            foreach (char character in value)
            {
                if ((character < '0' || character > '9') && (character < 'a' || character > 'f'))
                {
                    return false;
                }
            }

            return true;
        }

        private static string EngineName(ArenaCrossEngineKind engine) => engine switch
        {
            ArenaCrossEngineKind.DataVoReactive => "datavo-reactive",
            ArenaCrossEngineKind.DataVoPolling => "datavo-polling",
            ArenaCrossEngineKind.SqlitePolling => "sqlite",
            _ => "unknown"
        };

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
