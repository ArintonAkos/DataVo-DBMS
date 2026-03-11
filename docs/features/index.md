---
layout: home

hero:
  name: "DataVo Feature Guide"
  text: "SQL behavior, examples, and integration notes"
  tagline: "Read this like product documentation: supported SQL, example inputs, result tables, execution notes, and current limitations"
  actions:
    - theme: brand
      text: Start Here
      link: /features/getting-started
    - theme: alt
      text: Query Features
      link: /features/select-and-querying
    - theme: alt
      text: Setup and Packaging
      link: /features/setup-and-packaging

features:
  - title: Getting Started
    details: Create a database, insert rows, run a query, and understand the result shape returned by the engine.
    link: /features/getting-started
  - title: SELECT and Querying
    details: Browse filtering, joins, grouping, ordering, limits, predicates, set operations, and subquery support.
    link: /features/select-and-querying
  - title: Data Modification
    details: Learn how INSERT, UPDATE, DELETE, and VACUUM behave with constraints, rewrites, and physical cleanup.
    link: /features/data-modification
  - title: Schema and DDL
    details: Review CREATE TABLE, CREATE INDEX, and ALTER TABLE support with current guardrails and examples.
    link: /features/schema-and-ddl
  - title: Transactions
    details: Understand BEGIN, COMMIT, and ROLLBACK with session-scoped behavior and storage-level considerations.
    link: /features/transactions
  - title: Roadmap and Integrations
    details: See where ADO.NET, EF, vectors, WASM, and package distribution fit into the next documentation and product slices.
    link: /features/roadmap-and-integrations
---

# Feature Documentation

This section is the developer-facing feature guide for `DataVo`.

It is separate from the `DataVo.Core` module reference on purpose:

- **Feature docs** explain what the engine supports, how to use it, and what results to expect.
- **Module docs** explain how the codebase is organized internally for contributors.

## Recommended reading order

1. [Setup and Packaging](./setup-and-packaging.md)
2. [Getting Started](./getting-started.md)
3. [SELECT and Query Features](./select-and-querying.md)
4. [INSERT, UPDATE, DELETE, and VACUUM](./data-modification.md)
5. [DDL and Schema Changes](./schema-and-ddl.md)
6. [Transactions](./transactions.md)
7. [Roadmap and Integrations](./roadmap-and-integrations.md)

## Audience

This section is written for:

- developers embedding `DataVo` in applications
- contributors extending SQL support
- maintainers reviewing feature behavior and current limitations

## Current scope

The docs below reflect the currently implemented feature set in the engine, including:

- `SELECT`, filtering, ordering, grouping, and joins
- `INSERT`, `UPDATE`, `DELETE`
- `UNION` and `UNION ALL`
- `IN`, `BETWEEN`, `LIKE`
- uncorrelated subqueries: `IN`, `EXISTS`, scalar subqueries
- `ALTER TABLE ADD COLUMN`, `DROP COLUMN`, `MODIFY COLUMN`
- explicit transaction commands

## Fast paths

### I want to embed DataVo in a .NET app

Read:

1. [Setup and Packaging](./setup-and-packaging.md)
2. [Getting Started](./getting-started.md)
3. [Transactions](./transactions.md)

### I want to know what SQL is supported today

Read:

1. [SELECT and Query Features](./select-and-querying.md)
2. [Data Modification](./data-modification.md)
3. [Schema and DDL](./schema-and-ddl.md)

### I want to know what is coming next

Read:

1. [Roadmap and Integrations](./roadmap-and-integrations.md)
2. [DataVo.Core module reference](../DataVo.Core/index.md)

## What this section tries to answer

Each feature page aims to answer four practical questions:

1. **What SQL is supported?**
2. **What does the engine actually do with that SQL?**
3. **What does the input table look like before the query runs?**
4. **What result shape should a developer expect?**

## Current packaging status

Today, `DataVo` is packaged **locally** from the repository and is **not yet published to NuGet**.

Current local packages:

- `DataVo.Core`
- `DataVo.Data`

NuGet publication is planned for a later packaging and distribution slice.

## Read this together with

- [DataVo.Core module reference](../DataVo.Core/index.md)
- [Architecture docs](../architecture/index-manager.md)
