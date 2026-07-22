#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using UnityEngine;

namespace DataVo.UnityArena
{
    public sealed class ArenaVisualController : MonoBehaviour
    {
        private const float SimulationIntervalSeconds = 0.12f;
        private readonly Dictionary<int, NpcMarker> npcMarkers = new();

        private ArenaDeliveryRunner? runner;
        private ArenaFrameSnapshot snapshot = ArenaFrameSnapshot.Empty;
        private float nextSimulationAt;
        private string selectedBehaviour = "patrol";

        private void Start()
        {
            runner = new ArenaDeliveryRunner(new ArenaRunOptions { Mode = ArenaDeliveryMode.Reactive });
            runner.Warmup();
            CreateScene(runner.Options.VisibleNpcCount);
            RenderSnapshot(runner.CurrentSnapshot);
            nextSimulationAt = Time.unscaledTime + SimulationIntervalSeconds;
        }

        private void Update()
        {
            if (runner is null || Time.unscaledTime < nextSimulationAt)
            {
                return;
            }

            nextSimulationAt = Time.unscaledTime + SimulationIntervalSeconds;
            RenderSnapshot(runner.Step());
        }

        private void OnDestroy()
        {
            runner?.Dispose();
            runner = null;
        }

        public void RenderSnapshot(ArenaFrameSnapshot frame)
        {
            snapshot = frame ?? throw new ArgumentNullException(nameof(frame));
            string tag = snapshot.selectedMemoryMatches.Count == 0
                ? "objective"
                : snapshot.selectedMemoryMatches[0].tag;
            selectedBehaviour = BehaviourForTag(tag);

            foreach (ArenaNpcSeed npc in snapshot.visibleNpcs)
            {
                if (!npcMarkers.TryGetValue(npc.id, out NpcMarker? marker))
                {
                    continue;
                }

                marker.transform.position = npc.position + Vector3.up;
                marker.renderer.material.color = npc.id == snapshot.selectedNpcId
                    ? ColourFor(selectedBehaviour)
                    : ColourFor(BehaviourForTag(ArenaSeed.MemoryTag(npc.id - 1)));
            }
        }

        public static Color ColourFor(string behaviour)
        {
            return behaviour switch
            {
                "flee" => Color.red,
                "assist" => Color.green,
                "pursue" => Color.yellow,
                _ => Color.cyan
            };
        }

        private void CreateScene(int visibleNpcCount)
        {
            var cameraObject = new GameObject("Arena Camera");
            Camera arenaCamera = cameraObject.AddComponent<Camera>();
            arenaCamera.transform.position = new Vector3(0f, 25f, -18f);
            arenaCamera.transform.LookAt(new Vector3(0f, 0f, 0f));
            arenaCamera.clearFlags = CameraClearFlags.SolidColor;
            arenaCamera.backgroundColor = new Color(0.035f, 0.05f, 0.09f);

            var lightObject = new GameObject("Arena Light");
            Light light = lightObject.AddComponent<Light>();
            light.type = LightType.Directional;
            light.intensity = 1.2f;
            lightObject.transform.rotation = Quaternion.Euler(50f, -30f, 0f);

            GameObject ground = GameObject.CreatePrimitive(PrimitiveType.Plane);
            ground.name = "Arena Ground";
            ground.transform.localScale = new Vector3(3f, 1f, 2f);
            ground.GetComponent<Renderer>().material.color = new Color(0.08f, 0.1f, 0.15f);

            GameObject player = GameObject.CreatePrimitive(PrimitiveType.Sphere);
            player.name = "Player";
            player.transform.position = new Vector3(0f, 1.2f, 0f);
            player.transform.localScale = Vector3.one * 1.4f;
            player.GetComponent<Renderer>().material.color = Color.white;

            for (int npcId = 1; npcId <= visibleNpcCount; npcId++)
            {
                GameObject marker = GameObject.CreatePrimitive(PrimitiveType.Capsule);
                marker.name = "NPC " + npcId.ToString(CultureInfo.InvariantCulture);
                marker.transform.localScale = new Vector3(0.65f, 0.85f, 0.65f);
                Renderer renderer = marker.GetComponent<Renderer>();
                npcMarkers.Add(npcId, new NpcMarker(marker.transform, renderer));
            }
        }

        private void OnGUI()
        {
            ArenaFrameDiagnostics diagnostics = snapshot.diagnostics;
            string work = diagnostics.mode == ArenaDeliveryMode.Reactive
                ? "Reactive changes: " + diagnostics.reactiveChanges.ToString(CultureInfo.InvariantCulture)
                : "Polling rows: " + diagnostics.pollingRows.ToString(CultureInfo.InvariantCulture);

            GUILayout.BeginArea(new Rect(16f, 16f, 390f, Screen.height - 32f), GUI.skin.box);
            GUILayout.Label("DataVo NPC Memory Arena");
            GUILayout.Label("Mode: " + diagnostics.mode + " | Tick " + snapshot.tick.ToString(CultureInfo.InvariantCulture));
            GUILayout.Label(work);
            GUILayout.Label("P99 ms — tick: " + FormatMilliseconds(diagnostics.tick.p99Milliseconds) +
                            " | view: " + FormatMilliseconds(diagnostics.leaderboard.p99Milliseconds) +
                            " | vector: " + FormatMilliseconds(diagnostics.vectorSearch.p99Milliseconds));
            GUILayout.Space(6f);
            GUILayout.Label("Selected NPC " + snapshot.selectedNpcId.ToString(CultureInfo.InvariantCulture) +
                            " — " + selectedBehaviour.ToUpperInvariant());
            GUILayout.Label("Nearest memories: " + string.Join(", ", snapshot.selectedMemoryMatches.Take(3).Select(match => match.tag)));
            GUILayout.Space(6f);
            GUILayout.Label("Leaderboard");

            foreach (ArenaLeaderboardEntry entry in snapshot.leaderboard.Take(20))
            {
                GUILayout.Label("#" + entry.id.ToString(CultureInfo.InvariantCulture) +
                                "  score " + entry.score.ToString(CultureInfo.InvariantCulture));
            }

            GUILayout.Space(6f);
            GUILayout.Label("Alive by team: " + string.Join(", ", snapshot.aliveByTeam.Select(
                entry => entry.team + " " + entry.alive.ToString(CultureInfo.InvariantCulture))));
            GUILayout.EndArea();
        }

        private static string BehaviourForTag(string tag)
        {
            return tag switch
            {
                "danger" => "flee",
                "ally" => "assist",
                "loot" => "pursue",
                _ => "patrol"
            };
        }

        private static string FormatMilliseconds(double value) => value.ToString("0.000", CultureInfo.InvariantCulture);

        private sealed class NpcMarker
        {
            public NpcMarker(Transform transform, Renderer renderer)
            {
                this.transform = transform;
                this.renderer = renderer;
            }

            public Transform transform { get; }
            public Renderer renderer { get; }
        }
    }
}
