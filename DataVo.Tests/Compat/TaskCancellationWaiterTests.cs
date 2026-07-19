using DataVo.Core.Compat;

namespace DataVo.Tests.Compat;

public sealed class TaskCancellationWaiterTests
{
    [Fact]
    public async Task WaitAsync_UnderlyingTaskCompletes_First()
    {
        using var cancellation = new CancellationTokenSource();
        Task completed = Task.CompletedTask;

        await TaskCancellationWaiter.WaitAsync(completed, cancellation.Token);

        Assert.False(cancellation.IsCancellationRequested);
    }

    [Fact]
    public async Task WaitAsync_CancellationWins_WithoutCompletingUnderlyingTask()
    {
        var completion = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        Task wait = TaskCancellationWaiter.WaitAsync(completion.Task, cancellation.Token);

        cancellation.Cancel();

        OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await wait);
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.False(completion.Task.IsCompleted);

        completion.SetResult(null);
        await completion.Task;
    }

    [Fact]
    public async Task WaitAsync_PropagatesUnderlyingFailure()
    {
        var expected = new InvalidOperationException("expected failure");
        Task faulted = Task.FromException(expected);

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await TaskCancellationWaiter.WaitAsync(faulted, CancellationToken.None));

        Assert.Same(expected, actual);
    }
}
