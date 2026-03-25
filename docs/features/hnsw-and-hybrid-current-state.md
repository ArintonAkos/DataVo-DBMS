# HNSW + Hybrid Planner: Current State and What Happens Internally

> Last updated: 2026-03-24 (late)
> Audience: implementation and architecture tracking
> Status: Active implementation (hybrid planner optimization in progress)

## Why this document exists

This file answers the practical question:

- What is already implemented?
- What is still a roadmap item?
- What exactly happens when a mixed query (vector + lexical + join) runs?

## Quick reality check

At this moment, the optimizer/planner side is significantly advanced, while the index core is in transition.

- Implemented now:
  - Hybrid route selection (accept/reject buckets)
  - Candidate-first execution for vector queries
  - Adaptive topK sizing and expansion passes
  - Per-query and periodic telemetry snapshots
- Implemented now in HNSW core:
  - Layered graph connectivity with persisted tuning parameters
  - Multi-seed search and diversity-aware neighbor selection
  - Adaptive efSearch and adaptive efConstruction
  - Insertion candidate expansion (fixed + adaptive density-driven mode)
  - Insertion-time local neighborhood pruning (high-degree pressure)
  - Delete-time local graph repair

## End-to-end execution flow

```mermaid
flowchart TD
    A[SQL Query: vector + lexical + join] --> B{Planner Route Check}
    B -->|reject| C[Fallback Legacy or Volcano plan]
    B -->|accept| D[Vector Candidate Fetch]
    D --> E[Initial topK sizing]
    E -->|baseline| F[topK = LIMIT+OFFSET]
    E -->|adaptive| G[topK ~= LIMIT / selectivity]
    F --> H[SearchVector on index]
    G --> H
    H --> I{Enough rows after lexical filter?}
    I -->|no| J[Expansion pass: topK *= factor]
    J --> H
    I -->|yes| K[Seed rows ready]
    K --> L{Join needed?}
    L -->|no| M[Return filtered rows]
    L -->|yes| N[Volcano join on seed set]
    N --> O[ORDER BY + LIMIT final output]
    C --> O
```

## Sequence of a mixed query

```mermaid
sequenceDiagram
    participant Q as Query
    participant S as Select Planner
    participant I as IndexManager
    participant H as HNSWIndex
    participant V as Volcano Join

    Q->>S: SELECT ... WHERE lexical ORDER BY vector LIMIT K
    S->>S: Evaluate hybrid route eligibility
    alt Hybrid route accepted
        S->>S: Compute initial topK (baseline/adaptive)
        loop Expansion until enough rows or max passes
            S->>I: SearchVector(queryVector, topK)
            I->>H: SearchTopK(queryVector, topK)
            H-->>I: candidate rowIds
            I-->>S: candidate rowIds
            S->>S: Apply lexical predicate on candidates
        end
        alt Join present
            S->>V: Execute join only on seed rows
            V-->>S: joined rows
        end
        S-->>Q: final rows
    else Hybrid route rejected
        S->>V: Full fallback plan
        V-->>S: rows
        S-->>Q: final rows
    end
```

## Example 1: ORDER BY vector + lexical filter

SQL:

```sql
SELECT Id, Emb <=> '[1,0,0]' AS rank
FROM Embeddings
WHERE Status = 'active'
ORDER BY rank ASC
LIMIT 5;
```

Internal behavior (accepted hybrid route):

1. Planner checks route eligibility.
2. Computes initial topK:
   - Baseline mode: topK = 5
   - Adaptive mode: topK is inflated from selectivity estimate (for example 50).
3. Performs vector candidate fetch.
4. Applies lexical predicate on candidates.
5. If filtered rows < 5, expansion pass increases topK and retries.
6. Returns final rows.

## Example 2: Mixed query with join

SQL:

```sql
SELECT p.Id, p.Name, e.Emb <=> '[0.95,0.05,0]' AS rank
FROM Embeddings e
JOIN Products p ON p.Id = e.ProductId
WHERE e.Status = 'active'
ORDER BY rank ASC
LIMIT 3;
```

Internal behavior:

1. Candidate row IDs are fetched from vector path first.
2. Embedding-table lexical filter is applied on seed rows.
3. Volcano join executes on this reduced seed set.
4. Final ordering/limit is applied.

This is the key optimization: do not join full source tables when a smaller candidate set can be safely produced first.

## Telemetry that now exists

Per-query telemetry (when enabled):

- route decision
- requested topK and initial topK
- selectivity estimate
- expansion pass count
- query elapsed time and result rows

Periodic snapshot telemetry (query-interval based):

- total processed queries
- number of hybrid-used queries
- average expansion passes
- top reason bucket counters

Typical bucket names:

- `hybrid.orderby.accept`
- `hybrid.orderby.reject.topk_ge_total_rows`
- `hybrid.orderby.reject.complexity_gate`
- `hybrid.orderby.initial_topk.baseline`
- `hybrid.orderby.initial_topk.adaptive`

## New HNSW insertion behavior (latest)

Insertion path now has two additional controls:

- Adaptive insertion expansion policy:
  - Expands insertion candidate budget based on local graph density near insertion seeds.
  - Sparser regions receive broader candidate budgets, denser regions stay tighter.
- Canonical-style local pruning after insert:
  - After connecting a node, touched neighborhoods can be rebuilt from local candidate pools.
  - This reduces degree-pressure artifacts under insert/delete churn.

Key tuning parameters now persisted in index snapshots:

- `EnableInsertionCandidateExpansion`
- `InsertionCandidateExpansionFactor`
- `EnableAdaptiveInsertionCandidateExpansion`
- `AdaptiveInsertionExpansionMinFactor`
- `AdaptiveInsertionExpansionMaxFactor`
- `EnableInsertionNeighborhoodPruning`
- `InsertionNeighborhoodPruningThreshold`
- `InsertionNeighborhoodPruneHops`

## What benchmark-backed test shows

A representative mixed workload test was added.

Observed in test snapshots:

- Baseline initial topK: avg expansion passes about 7.0
- Adaptive initial topK: avg expansion passes about 4.0

Interpretation: adaptive initial topK reduced expansion retries by about 42.9% on that workload shape.

Additional benchmark coverage now includes:

- Insertion expansion recall trend (fixed-factor on/off comparison)
- Adaptive insertion expansion policy stability checks
- Churn matrix recall+latency bounds across churn ratios
- Multi-run churn soak variance guardrails (mean + stddev constraints)

## Phase status (high-level)

- Phase 5 (telemetry + expansion tuning): complete
- Phase 6 (hybrid planner optimization): near complete
- Phase 7 (HNSW production hardening): near complete, final tuning and long-soak confidence passes ongoing

## Practical takeaway

Today, the biggest wins are from planner-level intelligence:

- reduce scanned/joined rows early
- adapt candidate sizes
- observe behavior with telemetry

Index-core ANN sophistication is the next continuation track, while keeping current behavior stable and measurable.
