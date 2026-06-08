using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using DataVo.Benchmarks.Common;
using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<LiveArenaEngine>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LiveArenaEngine>());

var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();
app.UseWebSockets();

app.MapGet("/api/metrics", (LiveArenaEngine engine) => engine.Snapshot());
app.MapGet("/api/queries", () => LiveQueries.All);
app.MapPost("/api/reset", (LiveArenaEngine engine) =>
{
    engine.Reset();
    return Results.Ok(engine.Snapshot());
});

app.Map("/ws", async (HttpContext context, LiveArenaEngine engine) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    using WebSocket socket = await context.WebSockets.AcceptWebSocketAsync();
    await engine.Attach(socket, context.RequestAborted);
});

app.Run();

internal sealed class LiveArenaEngine : BackgroundService
{
    private readonly object _gate = new();
    private readonly List<LiveClient> _clients = [];
    private readonly Random _random = new(20260620);
    private DataVoContext _context = CreateContext();
    private long _tick;
    private long _mutations;
    private long _deltaRows;
    private bool _running = true;

    public LiveArenaEngine()
    {
        Seed(_context);
    }

    public async Task Attach(WebSocket socket, CancellationToken cancellationToken)
    {
        var client = new LiveClient(socket);
        lock (_gate)
        {
            _clients.Add(client);
        }

        await client.Enqueue(new
        {
            type = "hello",
            metrics = Snapshot(),
            queries = LiveQueries.All
        }, cancellationToken);

        Task sender = client.SendLoop(cancellationToken);
        Task receiver = ReceiveLoop(client, cancellationToken);

        await Task.WhenAny(sender, receiver);
        client.Dispose();

        lock (_gate)
        {
            _clients.Remove(client);
        }
    }

    public object Snapshot()
    {
        lock (_gate)
        {
            return new
            {
                running = _running,
                tick = _tick,
                mutations = _mutations,
                deltaRows = _deltaRows,
                clients = _clients.Count,
                subscriptions = _clients.Sum(c => c.SubscriptionCount),
                architecture = "DataVo Subscribe -> QueryChange -> WebSocket -> browser UI"
            };
        }
    }

    public void Reset()
    {
        lock (_gate)
        {
            foreach (LiveClient client in _clients)
            {
                client.ClearSubscriptions();
            }

            _context.Dispose();
            _context = CreateContext();
            Seed(_context);
            _tick = 0;
            _mutations = 0;
            _deltaRows = 0;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(120));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            lock (_gate)
            {
                if (!_running)
                {
                    continue;
                }

                Mutate(_context, _random, _tick);
                _context.DispatchPendingNotifications();
                _tick++;
                _mutations += 8;
            }

            await BroadcastMetrics(stoppingToken);
        }
    }

    private async Task ReceiveLoop(LiveClient client, CancellationToken cancellationToken)
    {
        var buffer = new byte[16 * 1024];
        while (!cancellationToken.IsCancellationRequested && client.Socket.State == WebSocketState.Open)
        {
            WebSocketReceiveResult result = await client.Socket.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                break;
            }

            string json = Encoding.UTF8.GetString(buffer, 0, result.Count);
            ClientCommand? command = JsonSerializer.Deserialize<ClientCommand>(json, JsonOptions);
            if (command is null)
            {
                continue;
            }

            await HandleCommand(client, command, cancellationToken);
        }
    }

    private async Task HandleCommand(LiveClient client, ClientCommand command, CancellationToken cancellationToken)
    {
        if (command.Type.Equals("pause", StringComparison.OrdinalIgnoreCase))
        {
            lock (_gate)
            {
                _running = false;
            }

            await BroadcastMetrics(cancellationToken);
            return;
        }

        if (command.Type.Equals("resume", StringComparison.OrdinalIgnoreCase))
        {
            lock (_gate)
            {
                _running = true;
            }

            await BroadcastMetrics(cancellationToken);
            return;
        }

        if (command.Type.Equals("unsubscribe", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(command.Id))
        {
            client.Unsubscribe(command.Id);
            return;
        }

        if (!command.Type.Equals("subscribe", StringComparison.OrdinalIgnoreCase)
            || string.IsNullOrWhiteSpace(command.Id)
            || string.IsNullOrWhiteSpace(command.Sql))
        {
            return;
        }

        lock (_gate)
        {
            IDisposable handle = _context.Subscribe(command.Sql, change =>
            {
                _deltaRows += change.Added.Count + change.Removed.Count + change.Updated.Count;
                _ = client.Enqueue(new
                {
                    type = "change",
                    id = command.Id,
                    sql = command.Sql,
                    tick = _tick,
                    added = change.Added,
                    removed = change.Removed,
                    updatedBefore = change.UpdatedBefore,
                    updated = change.Updated
                }, CancellationToken.None);
            });

            client.Replace(command.Id, handle);
        }

        await client.Enqueue(new { type = "subscribed", id = command.Id, sql = command.Sql }, cancellationToken);
    }

    private async Task BroadcastMetrics(CancellationToken cancellationToken)
    {
        LiveClient[] clients;
        object metrics = Snapshot();
        lock (_gate)
        {
            clients = [.. _clients];
        }

        foreach (LiveClient client in clients)
        {
            await client.Enqueue(new { type = "metrics", metrics }, cancellationToken);
        }
    }

    private static DataVoContext CreateContext() => new(new DataVoConfig { StorageMode = StorageMode.InMemory });

    private static void Seed(DataVoContext context)
    {
        context.ExecuteOk("CREATE DATABASE LiveArena");
        context.ExecuteOk("USE LiveArena");
        context.ExecuteOk("CREATE TABLE Players (Id INT PRIMARY KEY, Zone VARCHAR(20), Team VARCHAR(10), X INT, Y INT, Health INT, Score INT)");
        context.ExecuteOk("CREATE TABLE Inventory (InventoryId INT PRIMARY KEY, PlayerId INT, ItemId INT, Slot INT)");

        for (int id = 1; id <= 750; id++)
        {
            string zone = id % 8 == 0 ? "arena-7" : $"arena-{id % 8}";
            string team = id % 2 == 0 ? "blue" : "red";
            context.ExecuteOk($"INSERT INTO Players VALUES ({id}, '{zone}', '{team}', {id % 900}, {(id * 5) % 900}, {50 + (id % 51)}, {id % 2500})");

            if (id % 4 == 0)
            {
                context.ExecuteOk($"INSERT INTO Inventory VALUES ({id}, {id}, {9000 + id}, {id % 8})");
            }
        }
    }

    private static void Mutate(DataVoContext context, Random random, long tick)
    {
        for (int i = 0; i < 8; i++)
        {
            int id = random.Next(1, 751);
            string zone = (tick + id + i) % 14 == 0 ? "arena-7" : $"arena-{id % 8}";
            context.ExecuteOk($"UPDATE Players SET X = {random.Next(0, 1000)}, Y = {random.Next(0, 1000)}, Health = {random.Next(0, 101)}, Score = {(tick * 23 + id + i) % 100000}, Zone = '{zone}' WHERE Id = {id}");
        }
    }

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
}

internal sealed class LiveClient(WebSocket socket) : IDisposable
{
    private readonly Channel<string> _outbox = Channel.CreateUnbounded<string>();
    private readonly ConcurrentDictionary<string, IDisposable> _subscriptions = new(StringComparer.Ordinal);

    public WebSocket Socket { get; } = socket;
    public int SubscriptionCount => _subscriptions.Count;

    public async Task Enqueue(object payload, CancellationToken cancellationToken)
    {
        string json = JsonSerializer.Serialize(payload, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        await _outbox.Writer.WriteAsync(json, cancellationToken);
    }

    public async Task SendLoop(CancellationToken cancellationToken)
    {
        await foreach (string message in _outbox.Reader.ReadAllAsync(cancellationToken))
        {
            if (Socket.State != WebSocketState.Open)
            {
                break;
            }

            byte[] bytes = Encoding.UTF8.GetBytes(message);
            await Socket.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken);
        }
    }

    public void Replace(string id, IDisposable handle)
    {
        Unsubscribe(id);
        _subscriptions[id] = handle;
    }

    public void Unsubscribe(string id)
    {
        if (_subscriptions.TryRemove(id, out IDisposable? existing))
        {
            existing.Dispose();
        }
    }

    public void ClearSubscriptions()
    {
        foreach (string id in _subscriptions.Keys)
        {
            Unsubscribe(id);
        }
    }

    public void Dispose()
    {
        ClearSubscriptions();
        _outbox.Writer.TryComplete();
    }
}

internal sealed record ClientCommand(string Type, string? Id, string? Sql);

internal static class LiveQueries
{
    public static readonly object[] All =
    [
        new
        {
            id = "arena-players",
            label = "Arena players",
            sql = "SELECT Id, X, Y, Health FROM Players WHERE Zone = 'arena-7'"
        },
        new
        {
            id = "team-alive",
            label = "Team alive",
            sql = "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team"
        },
        new
        {
            id = "leaderboard",
            label = "Leaderboard",
            sql = "SELECT Id, Score FROM Players ORDER BY Score DESC LIMIT 10"
        },
        new
        {
            id = "inventory",
            label = "Zone inventory",
            sql = "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7'"
        }
    ];
}
