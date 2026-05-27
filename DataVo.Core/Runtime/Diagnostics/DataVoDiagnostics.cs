namespace DataVo.Core.Runtime.Diagnostics;

/// <summary>
/// Opt-in runtime query diagnostics collector for a single <see cref="Runtime.DataVoEngine"/> instance.
/// </summary>
public sealed class DataVoDiagnostics
{
    private readonly object _sync = new();
    private readonly Queue<RuntimeQueryStats> _recent = new();
    private readonly Queue<RuntimeQueryStats> _slow = new();
    private RuntimeQueryStats? _lastQuery;
    private int _recentQueryCapacity = 128;
    private int _slowQueryCapacity = 128;

    /// <summary>Gets or sets whether runtime diagnostics are recorded.</summary>
    public bool Enabled { get; set; }

    /// <summary>Gets or sets the elapsed threshold used to retain entries in the slow-query ring.</summary>
    public TimeSpan SlowQueryThreshold { get; set; } = TimeSpan.FromMilliseconds(16);

    /// <summary>Gets or sets the bounded capacity for recent queries.</summary>
    public int RecentQueryCapacity
    {
        get => _recentQueryCapacity;
        set
        {
            lock (_sync)
            {
                _recentQueryCapacity = Math.Max(0, value);
                TrimToCapacity(_recent, _recentQueryCapacity);
            }
        }
    }

    /// <summary>Gets or sets the bounded capacity for slow queries.</summary>
    public int SlowQueryCapacity
    {
        get => _slowQueryCapacity;
        set
        {
            lock (_sync)
            {
                _slowQueryCapacity = Math.Max(0, value);
                TrimToCapacity(_slow, _slowQueryCapacity);
            }
        }
    }

    /// <summary>Gets the most recently recorded query, if any.</summary>
    public RuntimeQueryStats? LastQuery
    {
        get
        {
            lock (_sync)
            {
                return _lastQuery;
            }
        }
    }

    internal void Record(RuntimeQueryStats stats)
    {
        if (!Enabled)
        {
            return;
        }

        lock (_sync)
        {
            _lastQuery = stats;
            EnqueueBounded(_recent, stats, _recentQueryCapacity);
            if (stats.Elapsed >= SlowQueryThreshold)
            {
                EnqueueBounded(_slow, stats, _slowQueryCapacity);
            }
        }
    }

    /// <summary>Returns a snapshot of the recent-query ring in oldest-to-newest order.</summary>
    public IReadOnlyList<RuntimeQueryStats> GetRecentQueries()
    {
        lock (_sync)
        {
            return _recent.ToArray();
        }
    }

    /// <summary>Returns a snapshot of the slow-query ring in oldest-to-newest order.</summary>
    public IReadOnlyList<RuntimeQueryStats> GetSlowQueries()
    {
        lock (_sync)
        {
            return _slow.ToArray();
        }
    }

    /// <summary>Clears the recorded query history and last-query reference.</summary>
    public void Clear()
    {
        lock (_sync)
        {
            _lastQuery = null;
            _recent.Clear();
            _slow.Clear();
        }
    }

    private static void EnqueueBounded(Queue<RuntimeQueryStats> queue, RuntimeQueryStats stats, int capacity)
    {
        if (capacity <= 0)
        {
            return;
        }

        queue.Enqueue(stats);
        TrimToCapacity(queue, capacity);
    }

    private static void TrimToCapacity(Queue<RuntimeQueryStats> queue, int capacity)
    {
        while (queue.Count > capacity)
        {
            queue.Dequeue();
        }
    }
}
