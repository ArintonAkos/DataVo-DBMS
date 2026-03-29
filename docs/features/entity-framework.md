# Entity Framework Integration

This page describes the DataVo integration path for Entity Framework users.

## Current status

DataVo includes an Entity Framework integration package and helper APIs in the repository.

Current posture:

- usable for early integration and evaluation
- suitable for controlled environments and iterative adoption
- further production-hardening and broader provider ergonomics remain active roadmap work

## Package intent

The Entity Framework layer is intended to help teams:

- map EF models to DataVo schema workflows
- generate and apply DataVo-compatible create statements
- keep model-first development practical while using DataVo runtime execution

## Planned public package flow

When published to public feeds:

```bash
dotnet add package DataVo.EntityFrameworkCore
```

## Current local package flow

```bash
dotnet pack DataVo.sln -c Release
dotnet add package DataVo.EntityFrameworkCore --source ./artifacts/packages
```

## Suggested adoption pattern

1. Start with direct DataVo embedding for core runtime confidence.
2. Introduce EF integration in bounded slices.
3. Validate schema and query behavior in CI with representative workloads.
4. Expand coverage as provider capabilities mature.

## Query modes in DataVo EF

DataVo EF currently exposes two practical query modes:

- Standard LINQ for non-vector projections/filters (`ctx.Items.Where(...).Select(...)`)
- Guarded/native bridge via `QueryFromDataVo` for DataVo-native translation and safe fallback

For vector queries, use `DataVoVectorDbFunctions` inside LINQ expressions:

```csharp
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

float[] q = [1f, 0f, 0f];

var nearest = ctx.QueryFromDataVo<ItemEmbedding>(s => s
	.Where(x => DataVoVectorDbFunctions.CosineDistance(EF.Functions, x.Vector, q) < 0.3)
	.OrderBy(x => DataVoVectorDbFunctions.CosineDistance(EF.Functions, x.Vector, q))
	.Take(5));
```

Supported vector LINQ function shims:

- `DataVoVectorDbFunctions.CosineDistance(EF.Functions, left, right)`
- `DataVoVectorDbFunctions.L2Distance(EF.Functions, left, right)`

Current native translation preview status:

- `CosineDistance`: translated to DataVo vector-distance SQL
- `L2Distance`: API surface exists, but native LINQ translation is not enabled yet

## Why this differs from PostgreSQL EF providers

PostgreSQL providers (for example Npgsql with pgvector) already ship mature translation layers for many custom operators/functions.
DataVo is adding this translation surface incrementally. The `DataVoVectorDbFunctions` API is the provider-native LINQ bridge for vector expressions while translation coverage continues to expand.

## Related pages

- [Setup and Packaging](./setup-and-packaging.md)
- [Roadmap and Integrations](./roadmap-and-integrations.md)
- [Getting Started](./getting-started.md)
- [Vector Queries Guide](./vector-queries-guide.md)
