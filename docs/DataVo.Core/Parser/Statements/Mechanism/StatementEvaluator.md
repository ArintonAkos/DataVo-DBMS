# StatementEvaluator.cs

The `StatementEvaluator.cs` component translates logic trees originating from the query parser, evaluating constraints across the target table and its JOIN contexts natively.

## Implementation Details & Methodologies

| Feature | Supported | Description |
| :--- | :---: | :--- |
| **Boolean Algebra Operations** | Yes | Supports complex nested operations (`AND`, `OR`, `EQUALS`, `NOT_EQUALS`, `LESS_THAN`, etc) efficiently mapping constraints across sequences. |
| **Hash-Map Output Clustering** | Yes | Outputs logical maps seamlessly wrapping rows in `HashedTable` structures gracefully bridging the storage engine and execution plan. |
| **Index-Accelerated Filtering** | Yes | Checks if conditional lookups map to available B-Tree sequences. Converts conditions properly into `IndexManager` filters to proactively bypass linear full-scans safely. |
| **Null Verification** | Yes | Natively identifies and isolates `IS NULL` and `IS NOT NULL` conditions across extracted table bounds cleanly. |

### Evaluation Delegation Sub-Routines

```mermaid
graph TD
    A[Start Evaluation] --> B{Node Operator Type}
    
    B -- AND --> C[Left Result Set]
    B -- AND --> D[Right Result Set]
    C --> E(Execute AND Intersection)
    D --> E
    
    B -- OR --> F[Left Result Set]
    B -- OR --> G[Right Result Set]
    F --> H(Execute OR Union)
    G --> H
    
    B -- COMPARISON --> I{Is Column Indexed?}
    I -- Yes --> J[HandleIndexableStatement]
    J --> K[O(1) Index Lookup -> B-Tree]
    I -- No --> L[HandleNonIndexableStatement]
    L --> M[Table Scan O(n)]
    
    K --> Output[Return HashedTable]
    M --> Output
    E --> Output
    H --> Output
```

### Critical Implementation specifics
- **Index Jump Detection (`HandleIndexableStatement`):** When executing direct equality limits linked to explicitly defined index schemas or the table's Primary Key, the evaluator natively uses `IndexManager.Instance.FilterUsingIndex` rapidly bypassing raw sequential memory iterations.
- **Set Operations:** The evaluator processes binary mappings structurally intercepting results internally as mappings mapping gracefully and safely. `AND` triggers an internal dictionary `Intersect`, while `OR` forces an internally resolved `Union`.
