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

| | Before typed arrangements | Now |
| --- | --- | --- |
| Keeping all 3 views current (200 ticks) | 6.0 ms per tick | **2.5 ms per tick** |
| Re-running the same 3 queries once | 68.9 ms per tick | 73.0 ms per tick |
| Views correct against a full re-query | yes | yes (top-10 and team totals verified every run) |

Cost of each view alone (`--only <view>`, 60 ticks, per input row delta):

| View | Upkeep per input row delta | Allocated per tick |
| --- | --- | --- |
| `GROUP BY` team totals | **~230 ns** | ~480 B |
| Top-10 (`ORDER BY ... LIMIT`) | 1.68 µs → **1.04 µs** | 804 KB → **37 KB** |
| `JOIN` big-plays feed | 2.35 µs → **1.22 µs** | 5.5 MB → **0.83 MB** |

The top-K and join operators used to keep dictionary-shaped rows in their arrangements: every change was
copied into a `Dictionary<string, object?>`, identified by a rendered string key, projected into a second
dictionary, and boxed again on the way out. Both now keep typed arrangements. Rows are stored as unboxed
`CellValue` cells, identified by a packed primary key (inline for a single-column key) and indexed by a packed
join key. The join's delivered state keeps references to the side rows instead of projected copies. Typed
insert images are adopted without copying. Top-K entries are pooled.

What is left is not garbage per row:

- **Top-10:** a sorted-set node per updated player, plus the sorted-set enumerator once per dispatch. That is
  about 200 B per dispatch in the unit benchmark, down from about 900 B.
- **Feed:** the join's arrangement grows by one stored row per event, because a later change to a player
  must be able to re-join every one of that player's events. Its steady-state cost per re-applied row went
  from about 3.2 KB to 256 B in the unit benchmark.

Writes run at about 25,000 to 35,000 events per second (two statements per event). The views add no write-side
cost, because change capture only appends deltas and the work happens at dispatch.
