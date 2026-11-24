# Live Reactive Leaderboard (DBSP Z-set incremental views)

A game backend writes a burst of match events every tick. Three standing SQL views stay current from the deltas of
those writes alone, through DataVo's Z-set incremental view maintenance. No view ever re-reads its base tables.

| View | SQL | Operator |
| --- | --- | --- |
| Team totals | `SELECT Team, COUNT(*), SUM(Points) FROM events GROUP BY Team` | incremental aggregate |
| Top-10 | `SELECT PlayerId, Name, Score FROM standings ORDER BY Score DESC LIMIT 10` | incremental top-K |
| Big plays | `SELECT e.Id, p.Name, e.Points FROM events e INNER JOIN players p ON e.PlayerId = p.Id WHERE e.Points >= 95` | incremental two-table join |

All three use `SubscribeZeroAlloc`: callbacks read borrowed `RowRef` spans and nothing is materialized as
dictionaries. Changes are pull-drained once per tick with `DispatchPendingNotifications()`, on the game-loop thread.

## Run it

```bash
dotnet run -c Release                                 # InMemory, 200 ticks x 1,000 events
dotnet run -c Release -- --lsm                        # the durable LSM engine
dotnet run -c Release -- --ticks 500 --events 2000
dotnet run -c Release -- --only teams                 # keep one view (teams | top | feed | none) to see its cost
```

Each tick writes one `INSERT` into `events` and one `UPDATE standings SET Score = Score + @points` per event, in one
transaction, then dispatches. The program prints the leaderboard four times. At the end it checks that every
incrementally maintained view equals a from-scratch query of the base tables.

## What it measured (Apple M4 Pro, InMemory, 1,000 events per tick)

| | |
| --- | --- |
| Keeping all 3 views current | **6.0 ms per tick** |
| Re-running the same 3 queries once | 68.9 ms per tick |
| Views correct against a full re-query | yes (top-10 and team totals verified every run) |

Cost of each view alone (`--only <view>`, 60 ticks, per input row delta):

| View | Upkeep per input row delta | Allocated per tick |
| --- | --- | --- |
| `GROUP BY` team totals | **~300 ns** | ~480 B |
| Top-10 (`ORDER BY ... LIMIT`) | ~1.7 µs | ~0.8 MB |
| `JOIN` big-plays feed | ~2.3 µs | ~5.5 MB |

The aggregate view is incremental in the strict sense: each event updates one group in place, in well under a
microsecond, with almost no allocation. The top-K and join operators are already about 11 times cheaper than
re-querying, but they still keep dictionary-shaped rows inside their arrangements, which is where their per-row
cost and allocations come from. Typed arrangements for those two operators are the next engine improvement.

Writes run at about 25,000 to 33,000 events per second (two statements per event); the views add no write-side
cost, because change capture only appends deltas and the work happens at dispatch.
