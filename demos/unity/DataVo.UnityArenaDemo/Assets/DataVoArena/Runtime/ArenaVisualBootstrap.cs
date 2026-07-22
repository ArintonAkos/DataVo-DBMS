#nullable enable

using System;
using UnityEngine;

namespace DataVo.UnityArena
{
    internal static class ArenaVisualBootstrap
    {
        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.AfterSceneLoad)]
        private static void Start()
        {
            if (IsHeadlessArenaInvocation())
            {
                return;
            }

            var root = new GameObject("DataVo Unity Arena");
            root.AddComponent<ArenaVisualController>();
        }

        private static bool IsHeadlessArenaInvocation()
        {
            string[] arguments = Environment.GetCommandLineArgs();

            foreach (string argument in arguments)
            {
                if (string.Equals(argument, "-datavoArenaMode", StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
