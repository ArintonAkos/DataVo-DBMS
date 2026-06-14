# Aggregations (Parser) Overview

The `Aggregations` module implements SQL aggregate functions used in SELECT projections and `GROUP BY` queries.

## Supported functions

- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

## How it works

Each aggregate is an `Aggregation` subclass that reduces a set of rows (typically a `ListedTable`) to a single scalar value.

Instances are created through `AggregationService`, which maps the function name to the corresponding implementation.

## Components

| Component (File) | Purpose |
|------------------|---------|
| `Aggregation.cs` | Base type and shared helpers. |
| `Avg.cs`, `Count.cs`, `Max.cs`, `Min.cs`, `Sum.cs` | Concrete aggregate implementations. |
