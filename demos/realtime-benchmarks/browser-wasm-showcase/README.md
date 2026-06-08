# Browser / WASM Showcase Plan

This folder documents how the realtime benchmark demos should be surfaced in the existing
WebAssembly architecture.

## Current Browser Architecture

The repo already has a browser runtime:

- `DataVo.Browser` targets `net10.0` with `RuntimeIdentifier=browser-wasm`.
- `DataVo.Browser/Program.cs` exports:
  - `Initialize()`
  - `ExecuteSql(sql)`
  - `ResetStorage()`
  - `RuntimeCapabilities()`
  - `DiagnoseLexer(sql)`
- `DataVo.Browser/datavo.interop.js` provides browser storage through OPFS/worker/localStorage
  fallbacks.
- `docs/tests/browser` contains Playwright parity tests for the browser runtime.

`Frontend` is a Windows Forms desktop frontend (`net6.0-windows`), so it is not the right target for
the zero-friction browser demo. The browser demo should build on `DataVo.Browser` and the docs/Vite
site instead.

## Compatibility Check

Based on the checked-in browser parity artifacts:

- last generated overall WASM summary: `434` passed, `0` failed
- generated SQL scenario summary: `245` passed, `0` failed
- browser-specific E2E summary: `36` passed, `0` failed
- runtime-needs-specific report: `0` outstanding items

Those artifacts are useful but not fresh proof for the latest reactive work. They were generated on
2026-03-30, while reactive subscriptions were added later. Treat them as evidence that the core SQL,
storage, vector, MVCC, and transaction paths have had broad browser coverage, not as evidence that
the reactive subscription API is already exported to JavaScript.

## What Works In WASM Today

Available through `ExecuteSql(sql)`:

- DDL and DML
- SELECT, joins, aggregates, distinct, union, subqueries
- transactions covered by browser parity scenarios
- vector column and HNSW scenarios covered by browser-specific tests
- browser persistence through the custom browser storage backend

## Missing For The Realtime Browser Demo

The reactive API exists in .NET:

```csharp
IDisposable Subscribe(string sql, Action<QueryChange> onChanged);
void DispatchPendingNotifications();
```

But `DataVo.Browser` does not yet export JavaScript bindings for it. A true browser realtime demo
needs these new JS exports:

```csharp
[JSExport]
public static string SubscribeSql(string subscriptionId, string sql)

[JSExport]
public static string DispatchPendingNotifications()

[JSExport]
public static string DrainReactiveChanges()

[JSExport]
public static string Unsubscribe(string subscriptionId)
```

Recommended browser-side flow:

```text
UI calls SubscribeSql("arena-scoreboard", "... GROUP BY Team")
simulation loop calls ExecuteSql("UPDATE ...")
simulation loop calls DispatchPendingNotifications()
UI calls DrainReactiveChanges()
UI applies added/removed/updated/updatedBefore rows
```

No WebSocket server is required. The page can run the simulation, database, reactive subscriptions,
and UI in one browser process.

## Minimal UI Shape

Implement this in the docs/Vite app after the JS exports exist:

- scenario selector: Game Arena / Trading
- mode selector: reactive / polling
- controls: start, pause, reset, rows, ticks/sec, mutations/tick
- live panels:
  - p50/p95/p99 tick latency
  - frame-budget miss rate
  - GC/heap estimate where browser APIs allow it
  - delta rows emitted vs polling rows returned
  - current leaderboard/top-of-book table

## Pros / Cons Versus Server WebSocket Demo

Pros:

- zero backend setup
- best possible "open a page and run the database" showcase
- demonstrates DataVo's local-first/browser-native story
- avoids confusing DataVo reactive deltas with network transport concerns

Cons:

- browser timers and GC are noisier than native benchmarks
- browser storage backend constraints differ from native in-memory and disk modes
- reactive JS exports still need to be implemented before this can be interactive
