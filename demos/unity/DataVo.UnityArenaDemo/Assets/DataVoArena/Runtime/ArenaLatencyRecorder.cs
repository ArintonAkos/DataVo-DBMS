#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace DataVo.UnityArena
{
    public sealed class ArenaLatencyRecorder
    {
        private readonly List<double> samplesMilliseconds = new();

        public int Count => samplesMilliseconds.Count;

        public long Start() => Stopwatch.GetTimestamp();

        public void AddElapsed(long startTimestamp)
        {
            long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
            AddMilliseconds(elapsed * 1000d / Stopwatch.Frequency);
        }

        public void AddMilliseconds(double milliseconds)
        {
            if (double.IsNaN(milliseconds) || double.IsInfinity(milliseconds) || milliseconds < 0d)
            {
                throw new ArgumentOutOfRangeException(nameof(milliseconds), "Latency samples must be finite and non-negative.");
            }

            samplesMilliseconds.Add(milliseconds);
        }

        public double RateAboveMilliseconds(double thresholdMilliseconds)
        {
            if (double.IsNaN(thresholdMilliseconds) || double.IsInfinity(thresholdMilliseconds))
            {
                throw new ArgumentOutOfRangeException(nameof(thresholdMilliseconds), "Threshold must be finite.");
            }

            if (samplesMilliseconds.Count == 0)
            {
                return 0d;
            }

            int above = 0;

            foreach (double sample in samplesMilliseconds)
            {
                if (sample > thresholdMilliseconds)
                {
                    above++;
                }
            }

            return above / (double)samplesMilliseconds.Count;
        }

        public ArenaLatencySummary Snapshot()
        {
            if (samplesMilliseconds.Count == 0)
            {
                return new ArenaLatencySummary();
            }

            double[] sorted = samplesMilliseconds.ToArray();
            Array.Sort(sorted);
            double total = 0d;

            foreach (double sample in sorted)
            {
                total += sample;
            }

            return new ArenaLatencySummary
            {
                count = sorted.Length,
                p50Milliseconds = NearestRank(sorted, 50d),
                p95Milliseconds = NearestRank(sorted, 95d),
                p99Milliseconds = NearestRank(sorted, 99d),
                maxMilliseconds = sorted[sorted.Length - 1],
                meanMilliseconds = total / sorted.Length
            };
        }

        private static double NearestRank(double[] sorted, double percentile)
        {
            int index = (int)Math.Ceiling(percentile / 100d * sorted.Length) - 1;
            index = Math.Max(0, Math.Min(index, sorted.Length - 1));
            return sorted[index];
        }
    }
}
