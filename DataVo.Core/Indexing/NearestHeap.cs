namespace DataVo.Core.Indexing;

/// <summary>
/// Keeps the nearest items offered to it in caller-supplied buffers: a bounded max-heap on distance, so the
/// farthest kept item is at the root and one comparison rejects most offers. Equal distances keep the smaller
/// item. It allocates nothing.
/// </summary>
/// <example>
/// <code>
/// var heap = new NearestHeap(distances.AsSpan(0, k), ordinals.AsSpan(0, k));
/// foreach (...) heap.Offer(distance, ordinal);
/// int count = heap.SortAscending(); // distances[0..count) and ordinals[0..count), nearest first
/// </code>
/// </example>
internal ref struct NearestHeap
{
    private readonly Span<float> _distances;
    private readonly Span<int> _items;
    private int _count;

    /// <summary>Creates a heap that keeps at most as many items as the buffers hold.</summary>
    public NearestHeap(Span<float> distances, Span<int> items)
    {
        _distances = distances;
        _items = items[..distances.Length];
        _count = 0;
    }

    /// <summary>Offers an item; it is kept when the heap is not full or it is nearer than the farthest kept one.</summary>
    public void Offer(float distance, int item)
    {
        if (_count < _distances.Length)
        {
            int slot = _count++;
            _distances[slot] = distance;
            _items[slot] = item;
            SiftUp(slot);
        }
        else if (_count > 0 && IsFarther(_distances[0], _items[0], distance, item))
        {
            _distances[0] = distance;
            _items[0] = item;
            SiftDown(0, _count);
        }
    }

    /// <summary>Sorts the kept items nearest first in the buffers and returns how many there are. The heap is then spent.</summary>
    public int SortAscending()
    {
        for (int end = _count - 1; end > 0; end--)
        {
            (_distances[0], _distances[end]) = (_distances[end], _distances[0]);
            (_items[0], _items[end]) = (_items[end], _items[0]);
            SiftDown(0, end);
        }

        return _count;
    }

    private static bool IsFarther(float distance, int item, float otherDistance, int otherItem) =>
        distance > otherDistance || (distance == otherDistance && item > otherItem);

    private void SiftUp(int slot)
    {
        while (slot > 0)
        {
            int parent = (slot - 1) >> 1;
            if (!IsFarther(_distances[slot], _items[slot], _distances[parent], _items[parent]))
            {
                return;
            }

            Swap(slot, parent);
            slot = parent;
        }
    }

    private void SiftDown(int slot, int count)
    {
        while (true)
        {
            int child = (slot << 1) + 1;
            if (child >= count)
            {
                return;
            }

            if (child + 1 < count && IsFarther(_distances[child + 1], _items[child + 1], _distances[child], _items[child]))
            {
                child++;
            }

            if (!IsFarther(_distances[child], _items[child], _distances[slot], _items[slot]))
            {
                return;
            }

            Swap(slot, child);
            slot = child;
        }
    }

    private void Swap(int left, int right)
    {
        (_distances[left], _distances[right]) = (_distances[right], _distances[left]);
        (_items[left], _items[right]) = (_items[right], _items[left]);
    }
}
