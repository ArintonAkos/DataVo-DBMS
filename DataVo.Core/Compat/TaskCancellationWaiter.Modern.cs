#if NET10_0_OR_GREATER
namespace DataVo.Core.Compat;

internal static class TaskCancellationWaiter
{
    public static Task WaitAsync(Task task, CancellationToken cancellationToken) =>
        task.WaitAsync(cancellationToken);
}
#endif
