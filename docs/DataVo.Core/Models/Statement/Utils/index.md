# Utils (Statement Models) Overview
The `Utils` subpackage contained within `Models/Statement` encapsulates lightweight structural abstractions that exist transiently during query compilation and execution. Rather than modeling the database schema statically, they track the dynamic representation of columns and tabular subsets while actively applying combinations or filters in memory.

## Core Responsibilities
* **Execution Schemas:** Track intermediate column declarations or combined `TableDetails` generated when a `JOIN` produces a virtual, temporary table structure.
* **Row Abstraction:** Formulate dictionaries serving as transient `Records` shuttling logic between evaluators independently of the persistent disk schema.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `Column.cs` | Models an abstract expression column dynamically targeted within a `SELECT` statement, managing potential aliases independently. |
| `Record.cs` | Encapsulates an actively evaluated row containing string-mapped values mapped directly during expression calculation phases. |
| `TableData.cs` | Functions as an intermediate array binding tracking dynamic execution records and internal table namespaces. |
| `TableDetails.cs` | Wraps the target table name and explicit runtime schema declarations to ensure bindings function consistently across alias names inside joins. |

## Dependencies & Interactions
Primarily instantiated inside the `QueryEngine` or dynamically through various `JoinStrategies`, these objects are ephemeral and do not correlate with standard DDL declarations found in `Models/Catalog`. They serve as a temporary bridge enabling the `Evaluator` layer to dynamically interpret ad-hoc query structures.
