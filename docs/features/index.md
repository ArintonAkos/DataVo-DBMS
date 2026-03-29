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
      text: Packaging and Install
      link: /features/setup-and-packaging
    - theme: alt
      text: WebAssembly and npm
      link: /features/wasm-and-npm

features:
  - title: Getting Started
    details: Create a database, insert rows, run a query, and understand the result shape returned by the engine.
    link: /features/getting-started
  - title: SELECT and Querying
    details: Browse filtering, joins, grouping, ordering, limits, predicates, set operations, and subquery support.
    link: /features/select-and-querying
    - title: Vector Queries Guide
      details: Create and search vector columns with distance metrics, HNSW indexing, and hybrid queries.
      link: /features/vector-queries-guide
  - title: WebAssembly and npm
    details: Learn browser runtime support today and customer-ready npm package integration planning.
    link: /features/wasm-and-npm
  - title: Unity and Godot
    details: Apply DataVo in game development workflows for local persistence and deterministic SQL behavior.
    link: /features/unity-and-godot
  - title: Entity Framework
    details: Understand the EF integration path and current adoption posture.
    link: /features/entity-framework
  - title: Volcano Planner and Execution
    details: Understand how the planner chooses Volcano vs legacy paths, with operator-pipeline diagrams and fallback rules.
    link: /features/volcano-planner-and-execution
  - title: Data Modification
    details: Learn how INSERT, UPDATE, DELETE, and VACUUM behave with constraints, rewrites, and physical cleanup.
    link: /features/data-modification
  - title: Security and Authentication
    details: Learn principal management, grants, login/logout session behavior, and SHOW introspection commands.
    link: /features/security-and-authentication
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

This section is the end-user and product-facing guide for DataVo.

It is separate from the DataVo.Core module reference on purpose:

- Feature docs explain integration, behavior, and customer adoption paths.
- Module docs explain internal implementation details for contributors.

## Recommended reading order

1. [Setup and Packaging](./setup-and-packaging.md)
2. [Getting Started](./getting-started.md)
3. [WebAssembly and npm](./wasm-and-npm.md)
4. [Unity and Godot](./unity-and-godot.md)
5. [Entity Framework Integration](./entity-framework.md)
6. [SELECT and Query Features](./select-and-querying.md)
   @@7. [Vector Queries Guide](./vector-queries-guide.md)
   @@8. [Security and Authentication](./security-and-authentication.md)
   @@9. [Volcano Planner and Execution](./volcano-planner-and-execution.md)
   @@10. [INSERT, UPDATE, DELETE, and VACUUM](./data-modification.md)
   @@11. [DDL and Schema Changes](./schema-and-ddl.md)
   @@12. [Transactions](./transactions.md)
   @@13. [Roadmap and Integrations](./roadmap-and-integrations.md)

## Audience

This section is written for:

- developers embedding `DataVo` in applications
- product teams planning NuGet and npm rollout
- Unity and Godot developers building local-first game data flows
- teams evaluating Entity Framework integration paths
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
- principal and grant commands (`CREATE USER`, `CREATE ROLE`, `GRANT`, `REVOKE`)
- auth session commands (`LOGIN`, `LOGOUT`)
- introspection commands (`SHOW USERS`, `SHOW ROLES`, `SHOW GRANTS`, `SHOW GRANTS FOR USER`, `SHOW GRANTS FOR ROLE`)
- browser WebAssembly runtime deployment flow
- customer-facing NuGet and npm publication guidance

## Fast paths

### I want to embed DataVo in a .NET app

Read:

1. [Setup and Packaging](./setup-and-packaging.md)
2. [Getting Started](./getting-started.md)
3. [Transactions](./transactions.md)

### I want to ship to Unity or Godot

Read:

1. [Unity and Godot](./unity-and-godot.md)
2. [Setup and Packaging](./setup-and-packaging.md)
3. [Security and Authentication](./security-and-authentication.md)

### I want browser and npm adoption guidance

Read:

1. [WebAssembly and npm](./wasm-and-npm.md)
2. [Setup and Packaging](./setup-and-packaging.md)
3. [Roadmap and Integrations](./roadmap-and-integrations.md)

### I want to know what SQL is supported today

Read:

1. [SELECT and Query Features](./select-and-querying.md)
2. [Security and Authentication](./security-and-authentication.md)
3. [Data Modification](./data-modification.md)
4. [Schema and DDL](./schema-and-ddl.md)

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

Today, DataVo is packaged locally from the repository and public feed publication is in progress.

Current local packages:

- `DataVo.Core`
- `DataVo.Data`

NuGet and npm publication are part of the active packaging and distribution rollout.

## Read this together with

- [DataVo.Core module reference](../DataVo.Core/index.md)
- [Architecture docs](../architecture/index-manager.md)
