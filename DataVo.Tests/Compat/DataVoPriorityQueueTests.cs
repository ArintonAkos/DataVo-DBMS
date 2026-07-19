using DataVo.Core.Compat;

namespace DataVo.Tests.Compat;

public sealed class DataVoPriorityQueueTests
{
    [Fact]
    public void Queue_DequeuesElementsInAscendingPriorityOrder()
    {
        var queue = new DataVoPriorityQueue<string, int>();
        queue.Enqueue("third", 30);
        queue.Enqueue("first", 10);
        queue.Enqueue("second", 20);

        Assert.Equal(3, queue.Count);
        Assert.Equal("first", queue.Dequeue());
        Assert.True(queue.TryDequeue(out string? second, out int secondPriority));
        Assert.Equal("second", second);
        Assert.Equal(20, secondPriority);
        Assert.True(queue.TryDequeue(out string? third, out int thirdPriority));
        Assert.Equal("third", third);
        Assert.Equal(30, thirdPriority);
        Assert.Equal(0, queue.Count);
    }

    [Fact]
    public void Queue_UsesCustomPriorityComparer()
    {
        var queue = new DataVoPriorityQueue<string, int>(Comparer<int>.Create((left, right) => right.CompareTo(left)));
        queue.Enqueue("low", 1);
        queue.Enqueue("high", 3);
        queue.Enqueue("middle", 2);

        Assert.True(queue.TryPeek(out string? element, out int priority));
        Assert.Equal("high", element);
        Assert.Equal(3, priority);
        Assert.Equal("high", queue.Peek());
    }

    [Fact]
    public void Queue_EmptyOperationsMatchPriorityQueueContract()
    {
        var queue = new DataVoPriorityQueue<string, int>();

        Assert.False(queue.TryPeek(out string? peeked, out int peekPriority));
        Assert.Null(peeked);
        Assert.Equal(default, peekPriority);
        Assert.False(queue.TryDequeue(out string? dequeued, out int dequeuePriority));
        Assert.Null(dequeued);
        Assert.Equal(default, dequeuePriority);
        Assert.Throws<InvalidOperationException>(() => { queue.Peek(); });
        Assert.Throws<InvalidOperationException>(() => { queue.Dequeue(); });
    }

    [Fact]
    public void Clear_RemovesAllElements()
    {
        var queue = new DataVoPriorityQueue<string, int>();
        queue.Enqueue("one", 1);
        queue.Enqueue("two", 2);

        queue.Clear();

        Assert.Equal(0, queue.Count);
        Assert.False(queue.TryPeek(out _, out _));
    }
}
