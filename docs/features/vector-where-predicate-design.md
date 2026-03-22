# Expanded Planner Eligibility: WHERE Predicate Extraction

> Design Document for nearest-neighbor queries with WHERE filters on the embedding table
> Last updated: 2026-03-22
> Status: Design Phase

## Goal

Expand the automatic query planner to support nearest-neighbor queries with simple WHERE predicates on the embedding table, enabling three-phase execution:

1. **HNSW Fetch**: Use index to get top-k row IDs (ignoring WHERE for now)
2. **Predicate Filter**: Apply WHERE predicates to seed rows (filtering embedding table only)
3. **Join + Limit**: Join filtered results to other tables and apply ORDER BY/LIMIT

### Example Query

```sql
SELECT p.Name, p.Id, a.Emb <=> '[0.95,0.05,0]' AS rank
FROM p_embeddings a
WHERE a.status = 'active'
  AND a.created_date > '2026-01-01'
JOIN products p ON a.product_id = p.Id
ORDER BY rank ASC
LIMIT 1
```

Three-phase execution:
```
HNSW_SEARCH('[0.95,0.05,0]', topK=100) 
  -> fetch rows [1, 5, 7, 23, ...]

FILTER(rows, 'a.status = "active" AND a.created_date > "2026-01-01"')
  -> [5, 7, ...]

JOIN(filtered_rows, products ON a.product_id = p.Id)
  -> Apply ORDER BY distance, LIMIT 1
```

## Design Constraints

### Conservative Eligibility Criteria

The planner will only apply WHERE predicate extraction if:

1. **Single embedding table reference**: All WHERE predicates reference columns from the embedding table only
2. **No joined table references**: WHERE predicates must NOT reference columns from tables in JOIN clauses
3. **No subqueries**: Subqueries in WHERE are not supported initially
4. **Supported operators**: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IS NULL`, `IS NOT NULL`
5. **Safe connectives**: 
   - AND: Can push down individual AND-connected clauses (conservative optimization)
   - OR: **Not supported initially** (would requires evaluating all rows or complex logic)

### Not Supported (Future Work)

- OR predicates (requires brute-force evaluation of all candidates)
- Subqueries in WHERE clause
- Window functions in WHERE
- Aggregate functions in WHERE predicates
- Complex expressions like `WHERE (a.status = 'active' OR a.status = 'pending')`

## Architecture

### 1. Predicate Extraction Phase

**New Method**: `ExpressionExtractor.ExtractEmbeddingTablePredicates(ExpressionNode where, string embeddingTableName)`

```csharp
public class ExpressionExtractor
{
    /// <summary>
    /// Extracts WHERE predicates that reference only the embedding table.
    /// Returns null if extraction is impossible or would be unsafe.
    /// </summary>
    public static ExpressionNode? TryExtractEmbeddingOnlyPredicates(
        ExpressionNode? expression,
        string embeddingTableName,
        TableService tableService);

    /// <summary>
    /// Checks if an expression tree references only a single table.
    /// </summary>
    private static bool ReferencesOnlyTable(ExpressionNode expression, string tableName, TableService tableService);

    /// <summary>
    /// Checks if an expression would be safe to evaluate eagerly on seed rows.
    /// </summary>
    private static bool IsSafeEmbeddingFilter(ExpressionNode expression);
}
```

**Extraction Algorithm**:

1. Walk the WHERE expression tree recursively
2. For each BinaryExpressionNode with AND operator:
   - Extract left side if it references only embedding table
   - Extract right side if it references only embedding table
   - Combine extracted predicates with AND
3. Reject OR expressions (not supported initially)
4. Return combined predicate or null if not extractable

### 2. Predicate Evaluation Phase

**Modified Method**: `Select.EvaluateJoinFromSeed(TableData seedRows, ExpressionNode? embeddingFilter)`

Apply extracted WHERE predicates to seed rows **before** joining:

```csharp
private ListedTable EvaluateJoinFromSeed(TableData seedRows, ExpressionNode? embeddingFilter)
{
    // Phase 2: Apply embedding table predicates to filter seed rows
    if (embeddingFilter != null)
    {
        seedRows = FilterRowsByPredicate(seedRows, embeddingFilter, _model.FromTable);
    }

    // Phase 3: Original join execution on filtered seed set
    HashedTable groupedInitialTable = PrepareHashedTable(seedRows, _model.FromTable.TableName);
    return _model.JoinStatement.Evaluate(groupedInitialTable, _model.FromTable.TableName).ToListedTable();
}
```

### 3. Planner Eligibility Check

**Modified Method**: `Select.IsNearestJoinTwoPhaseEligible(out ExpressionNode? extractedFilter)`

```csharp
private bool IsNearestJoinTwoPhaseEligible(out ExpressionNode? embeddingFilter)
{
    embeddingFilter = null;

    // Try to extract WHERE predicates on embedding table only
    ExpressionNode? whereExpression = _model.WhereStatement.GetExpression();
    
    embeddingFilter = ExpressionExtractor.TryExtractEmbeddingOnlyPredicates(
        whereExpression,
        _model.FromTable.TableName,
        _model.TableService);

    // Check if extracted predicates exist OR no WHERE clause at all
    bool hasValidFilter = embeddingFilter != null || IsAllTrueExpression(whereExpression);
    
    if (!hasValidFilter)
    {
        return false; // WHERE references joined tables or uses unsupported patterns
    }

    // Verify all joins are INNER
    return _model.JoinStatement.Model.JoinConditions
        .All(condition => condition.JoinType.Equals(JoinTypes.INNER, StringComparison.OrdinalIgnoreCase));
}
```

## Implementation Phases

### Phase 1: Core Predicate Extraction

- [ ] Implement `ExpressionExtractor` utility class
- [ ] Add `TryExtractEmbeddingOnlyPredicates()` for simple AND predicates
- [ ] Add table reference validation
- [ ] Add operator validation (only support safe operators)

### Phase 2: Planner Integration

- [ ] Modify `IsNearestJoinTwoPhaseEligible()` to call extraction
- [ ] Modify `EvaluateJoinFromSeed()` to accept and apply embedding filter
- [ ] Update `TryEvaluateNearestNeighborUsingVectorIndex()` to pass filter through

### Phase 3: Testing

- [ ] Add tests for simple WHERE predicates (a.status = 'active')
- [ ] Add tests for multiple AND predicates
- [ ] Add tests for WHERE + nearest + join combinations
- [ ] Add negative tests (OR predicates, joined table references, etc.)
- [ ] Add tests for both in-memory and disk storage

### Phase 4: Documentation & Future Work

- [ ] Document supported WHERE patterns
- [ ] Plan OR predicate support (would require different strategy)
- [ ] Plan subquery support (future enhancement)

## Safety Guarantees

### Correctness Preservation

1. **Table isolation**: Predicates must reference embedding table only
2. **Logical equivalence**: `HNSW(K) + FILTER(where) + JOIN` = `normal WHERE + JOIN + HNSW(K)`
   - The planner sees more rows from HNSW than the final WHERE would show, but filters them before joining
   - Result is identical to running full query without optimization

3. **Fallback path**: If extraction fails, query runs normally without optimization

### Performance Guarantees

- Early filtering on seed rows reduces join cardinality
- No additional passes required (single HNSW search + single filter + single join)

## Test Matrix

```
Embedding Table Filter Scenarios:
  [x] No WHERE clause (current behavior)
  [ ] Simple equality: a.status = 'active'
  [ ] Simple comparison: a.score > 0.5
  [ ] LIKE literal: a.name LIKE 'Ch%'
  [ ] IS NULL: a.deleted_at IS NULL
  [ ] Multiple AND: a.status = 'active' AND a.score > 0.5
  
Invalid/Unsupported Scenarios (should fallback to normal execution):
  [ ] OR predicate: a.status = 'active' OR a.status = 'pending'
  [ ] Joined table ref: a.status = 'active' AND p.category_id = 5
  [ ] Subquery: a.id IN (SELECT ...)
  [ ] Complex expression: a.score + 10 > 20
  
Storage  Modes:
  [x] InMemory
  [x] Disk
```

## Example Data Flow

### Query

```sql
SELECT p.Name, a.Emb <=> '[0.9,0.1,0]' AS rank
FROM p_embeddings a
WHERE a.status = 'active' AND a.created_date >= '2026-01-01'
JOIN products p ON a.product_id = p.Id
ORDER BY rank ASC
LIMIT 5
```

### Extraction Result

```
Input WHERE:     (a.status = 'active') AND (a.created_date >= '2026-01-01')
Output Filter:   (a.status = 'active') AND (a.created_date >= '2026-01-01')  ✓ all clauses safe
Table Refs:      a only  ✓ no joined table references
Operator:        AND  ✓ supported
```

### Execution Plan

```
Step 1: HNSW Search
  SearchVector('[0.9,0.1,0]', topK=5+buffer) 
  -> RowIds [1, 5, 7, 23, 44, ...]  (6 seed rows)

Step 2: Apply Embedding Filter
  FilterRowsByPredicate(seeds, 'a.status = "active" AND a.created_date >= "2026-01-01"')
  -> RowIds [5, 23, 44]  (3 rows pass filter)

Step 3: Join & Project
  JOIN filtered_rows to products ON a.product_id = p.Id
  ORDER BY rank ASC
  LIMIT 5
  -> Final result

vs. Without optimization:
  Full table scan + filter WHERE + join + order + limit  (much slower)
```

## Code Structure

### New Files

- `DataVo.Core/Parser/Statements/ExpressionExtractor.cs`: Predicate extraction logic

### Modified Files

- `DataVo.Core/Parser/DQL/Select.cs`: Use extracted predicates in fast path
- `DataVo.Core/Parser/Statements/Mechanism/ExpressionEvaluator.cs`: Already supports evaluation

### No Changes Needed

- `DataVoContext.cs`: API remains the same
- `Catalog`: Already has metadata
- `IndexManager`: Already has HNSW search

## Future Enhancements

### OR Predicate Support (Roadmap)

Would require one of:
1. Brute-force approach: fetch all HNSW results and filter (defeats purpose)
2. Multiple HNSW queries: search for each OR branch separately and union (complex)
3. Probabilistic approach: estimate selectivity and adjust topK (unpredictable)

### Subquery Support (Roadmap)

Would require materializing subquery results and matching against seed rows. Deferred pending broader query planning improvements.

### Cost-Based Optimization (Roadmap)

Estimate selectivity of WHERE predicates to determine if HNSW path is worth it.

---

## Next Steps

1. Implement `ExpressionExtractor` with safe predicate extraction
2. Modify planner to use extraction in eligibility check
3. Update `EvaluateJoinFromSeed` to filter after HNSW fetch
4. Add comprehensive test coverage
5. Document behavior and limitations

