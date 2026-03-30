# MVCC Multi-Phase Epic

## Objective

Deliver production-grade MVCC end-to-end: snapshot-visible reads, versioned writes, write-conflict detection, reduced reader locking, and lifecycle maintenance (vacuum/GC).

## Scope

- Integrate snapshot visibility in read paths (`SELECT` and shared table reads).
- Integrate version lifecycle in write paths (`INSERT`, `UPDATE`, `DELETE`, and `COMMIT` replay).
- Detect write conflicts using version metadata.
- Relax row read locks when snapshot reads are active.
- Add vacuum utilities for obsolete version metadata cleanup.

## Phases

### Phase 2A: Snapshot Read Integration

- Add ambient MVCC execution scope for the current snapshot.
- Apply visibility filtering in storage read APIs.
- Ensure legacy rows without version metadata are initialized safely.

### Phase 2B: Versioned DML

- `INSERT`: allocate version (`xmin = txId`).
- `UPDATE`: mark old version obsolete (`xmax = txId`), insert new version, link chain.
- `DELETE`: mark version obsolete (`xmax = txId`).
- Apply same logic for explicit transaction commit replay.

### Phase 2C: Conflict Detection

- Validate target rows before update/delete:
  - reject already-obsoleted rows,
  - reject rows not visible to transaction snapshot.
- Re-check conflicts at commit replay boundary.

### Phase 2D: Lock Relaxation

- Skip row read locks when snapshot MVCC reads are active.
- Keep write locks and row write locks unchanged.

### Phase 2E: Maintenance and Performance

- Add lightweight vacuum for version metadata tied to non-existent rows.
- Expose utility entrypoint for periodic cleanup in engine maintenance flows.

## Verification

- Unit tests: coordinator logic, visibility filtering, conflict checks.
- Integration tests: snapshot reads, concurrent update/delete conflicts, non-blocking readers.
- Regression tests: existing DML/DQL suites.

## Rollout Notes

- Backward compatible with legacy rows via automatic version bootstrap (`xmin = 0`).
- No schema migration required for initial adoption.
