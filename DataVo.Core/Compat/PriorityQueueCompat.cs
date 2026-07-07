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
        _items.Add((element, priority));
        SiftUp(_items.Count - 1);
    }

    public TElement Dequeue()
    {
        if (_items.Count == 0)
        {
            throw new InvalidOperationException("The priority queue is empty.");
        }

        TElement element = _items[0].Element;
        RemoveRoot();
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
        RemoveRoot();
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

    private void RemoveRoot()
    {
        int last = _items.Count - 1;
        if (last == 0)
        {
            _items.RemoveAt(0);
            return;
        }

        _items[0] = _items[last];
        _items.RemoveAt(last);
        SiftDown(0);
    }

    private void SiftUp(int index)
    {
        (TElement Element, TPriority Priority) item = _items[index];
        while (index > 0)
        {
            int parent = (index - 1) >> 1;
            if (_comparer.Compare(item.Priority, _items[parent].Priority) >= 0)
            {
                break;
            }

            _items[index] = _items[parent];
            index = parent;
        }

        _items[index] = item;
    }

    private void SiftDown(int index)
    {
        (TElement Element, TPriority Priority) item = _items[index];
        int half = _items.Count >> 1;

        while (index < half)
        {
            int child = (index << 1) + 1;
            int right = child + 1;

            if (right < _items.Count && _comparer.Compare(_items[right].Priority, _items[child].Priority) < 0)
            {
                child = right;
            }

            if (_comparer.Compare(_items[child].Priority, item.Priority) >= 0)
            {
                break;
            }

            _items[index] = _items[child];
            index = child;
        }

        _items[index] = item;
    }
}
#endif
