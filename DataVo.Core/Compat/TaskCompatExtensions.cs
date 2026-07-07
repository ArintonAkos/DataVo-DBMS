#if NETSTANDARD2_1
namespace System.Threading.Tasks;

internal static class TaskCompatExtensions
{
    public static async Task WaitAsync(this Task task, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            await task.ConfigureAwait(false);
            return;
        }

        var cancellation = new TaskCompletionSource<object?>();
        using (cancellationToken.Register(static state => ((TaskCompletionSource<object?>)state!).TrySetResult(null), cancellation))
        {
            Task completed = await Task.WhenAny(task, cancellation.Task).ConfigureAwait(false);
            if (ReferenceEquals(completed, cancellation.Task))
            {
                throw new OperationCanceledException(cancellationToken);
            }
        }

        await task.ConfigureAwait(false);
    }
}
#endif
