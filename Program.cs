// See https://aka.ms/new-console-template for more information

using System.Threading.Channels;

namespace CustomActionBlock.App;

public class ActionBlock<T> : IDisposable
{
    private readonly Channel<T> _queue;
    private readonly Task _consumer;

    public ActionBlock(Func<T, Task> action, int maxParallel = 1)
    {
        _queue = Channel.CreateUnbounded<T>();
        _consumer = Task.Run(async () =>
        {
            var buffer = new Queue<T>();
            await foreach (var item in _queue.Reader.ReadAllAsync())
            {
                buffer.Enqueue(item);
                if (buffer.Count == maxParallel)
                {
                    var parallelItems = buffer.ToArray();
                    buffer.Clear();
                    var parallelTasks = parallelItems.Select(action);
                    await Task.WhenAll(parallelTasks);
                }
            }
        });
    }

    public async Task PostAsync(T item, CancellationToken cancellationToken = default)
    {
        await _queue.Writer.WriteAsync(item, cancellationToken);
    }

    public void Dispose()
    {
        _consumer.Dispose();
    }
}

public class GroupPipeline<TItem>
{
    private static readonly object _lockObj = new object();
    private readonly Dictionary<int, ActionBlock<TItem>> _pool;

    public GroupPipeline()
    {
        _pool = new Dictionary<int, ActionBlock<TItem>>();
    }

    public async Task PostAsync(TItem item, int group)
    {
        if (!_pool.ContainsKey(group))
        {
            lock (_lockObj)
            {
                if (_pool.ContainsKey(group))
                {
                    return;
                }

                _pool.Add(group, new ActionBlock<TItem>(async d =>
                {
                    Console.WriteLine("hi");
                    await Task.Delay(1000);
                }, maxParallel: 5));
            }
        }

        _pool.TryGetValue(group, out var actionBlock);

        await actionBlock.PostAsync(item);
    }
}

internal class Program
{
    public static async Task Main(string[] args)
    {
        var ab = new GroupPipeline<int>();

        var items = Enumerable.Range(1, 10);
        var parallel = items.Select(t => ab.PostAsync(t, 1)).ToList();
        await Task.WhenAll(parallel);

        Console.ReadLine();
    }
}
