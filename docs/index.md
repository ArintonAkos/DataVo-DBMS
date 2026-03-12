---
layout: home

hero:
  name: "DataVo DBMS"
  text: "Documentation"
  tagline: "Modern docs for embedding, extending, and understanding a custom C# relational database engine"
  actions:
    - theme: brand
      text: Get Started
      link: /features/getting-started
    - theme: alt
      text: Explore Features
      link: /features/
    - theme: alt
      text: Core Modules
      link: /DataVo.Core/
    - theme: alt
      text: Packaging Guide
      link: /features/setup-and-packaging

features:
  - title: Querying Guide
    details: Learn the supported SELECT surface with examples for joins, grouping, predicates, ordering, set operations, and subqueries.
    link: /features/select-and-querying
  - title: Data Changes
    details: Understand INSERT, UPDATE, DELETE, and VACUUM with before-and-after table examples.
    link: /features/data-modification
  - title: Schema Evolution
    details: Review CREATE TABLE, CREATE INDEX, and guarded ALTER TABLE support for adding, dropping, and modifying columns.
    link: /features/schema-and-ddl
  - title: Packaging and Embedding
    details: Start with local packages today, embed DataVo in a .NET app, and prepare for future publication workflows.
    link: /features/setup-and-packaging
  - title: Runtime Architecture
    details: Move from feature docs into parser, runtime, storage, and indexing internals when contributing to the engine.
    link: /DataVo.Core/
  - title: Roadmap
    details: Track planned ADO.NET, Entity Framework, vector, WebAssembly, and future NuGet distribution work.
    link: /features/roadmap-and-integrations
---

<SqlEditor />

## Choose a documentation path

- **Feature Documentation**: use this when you want to know what SQL works, what the engine returns, and how behavior looks from the outside.
- **DataVo.Core Modules**: use this when you want to understand implementation details, extension points, or subsystem responsibilities.
- **Architecture**: use this when you want design-level context, storage/indexing rationale, and internal structure notes.

## What makes these docs different now

This site is being shaped more like a product documentation portal rather than a raw project wiki.

- quick entry points instead of long unstructured pages
- feature-first guides before internal code reference
- examples with input tables and output tables
- roadmap visibility for what exists now versus what is planned

## Start here

If you are new to the project, the best sequence is:

1. [Getting Started](./features/getting-started.md)
2. [SELECT and Query Features](./features/select-and-querying.md)
3. [Setup and Packaging](./features/setup-and-packaging.md)
4. [Roadmap and Integrations](./features/roadmap-and-integrations.md)
