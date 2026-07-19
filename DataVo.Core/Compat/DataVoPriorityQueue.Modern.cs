#if NET10_0_OR_GREATER
namespace DataVo.Core.Compat;

internal sealed class DataVoPriorityQueue<TElement, TPriority>
{
    private readonly PriorityQueue<TElement, TPriority> _queue;

    public DataVoPriorityQueue()
        : this(null)
    {
    }

    public DataVoPriorityQueue(IComparer<TPriority>? comparer)
    {
        _queue = new PriorityQueue<TElement, TPriority>(comparer);
    }

    public int Count => _queue.Count;

    public void Enqueue(TElement element, TPriority priority) => _queue.Enqueue(element, priority);

    public TElement Dequeue() => _queue.Dequeue();

    public bool TryDequeue(out TElement element, out TPriority priority) =>
        _queue.TryDequeue(out element!, out priority!);

    public TElement Peek() => _queue.Peek();

    public bool TryPeek(out TElement element, out TPriority priority) =>
        _queue.TryPeek(out element!, out priority!);

    public void Clear() => _queue.Clear();
}
#endif
