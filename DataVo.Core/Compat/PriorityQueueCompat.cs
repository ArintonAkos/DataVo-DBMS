#if NETSTANDARD2_1
namespace System.Collections.Generic;

internal sealed class PriorityQueue<TElement, TPriority>
{
    private readonly List<(TElement Element, TPriority Priority)> _items = [];
    private readonly IComparer<TPriority> _comparer;

    public PriorityQueue()
        : this(null)
    {
    }

    public PriorityQueue(IComparer<TPriority>? comparer)
    {
        _comparer = comparer ?? Comparer<TPriority>.Default;
    }

    public int Count => _items.Count;

    public void Enqueue(TElement element, TPriority priority)
    {
        int index = _items.BinarySearch((element, priority), EntryComparer.Instance(_comparer));
        if (index < 0)
        {
            index = ~index;
        }

        _items.Insert(index, (element, priority));
    }

    public TElement Dequeue()
    {
        if (_items.Count == 0)
        {
            throw new InvalidOperationException("The priority queue is empty.");
        }

        TElement element = _items[0].Element;
        _items.RemoveAt(0);
        return element;
    }

    public bool TryDequeue(out TElement element, out TPriority priority)
    {
        if (_items.Count == 0)
        {
            element = default!;
            priority = default!;
            return false;
        }

        (element, priority) = _items[0];
        _items.RemoveAt(0);
        return true;
    }

    public TElement Peek()
    {
        if (_items.Count == 0)
        {
            throw new InvalidOperationException("The priority queue is empty.");
        }

        return _items[0].Element;
    }

    public bool TryPeek(out TElement element, out TPriority priority)
    {
        if (_items.Count == 0)
        {
            element = default!;
            priority = default!;
            return false;
        }

        (element, priority) = _items[0];
        return true;
    }

    public void Clear() => _items.Clear();

    private sealed class EntryComparer : IComparer<(TElement Element, TPriority Priority)>
    {
        private readonly IComparer<TPriority> _priorityComparer;

        private EntryComparer(IComparer<TPriority> priorityComparer)
        {
            _priorityComparer = priorityComparer;
        }

        public static EntryComparer Instance(IComparer<TPriority> priorityComparer) => new(priorityComparer);

        public int Compare((TElement Element, TPriority Priority) x, (TElement Element, TPriority Priority) y) =>
            _priorityComparer.Compare(x.Priority, y.Priority);
    }
}
#endif
