# DDL (Parser Actions) Overview
The `DDL` execution module interprets Data Definition Language (DDL) AST models and performs physical destructive or constructive actions against the relational topology tracked by the `Catalog`. It physically alters the landscape of the database.

## Core Responsibilities
* **Schema Materialization:** Coordinates the allocation of new directories, metadata JSON mapping files, and binary file representations mapped directly to the Storage layer.
* **Topological Destruction:** Executing `DROP` actions safely removing table metadata whilst cleanly obliterating associative B-Tree maps.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `CreateDatabase.cs` | Binds active directory IO configuring root database folder creation operations. |
| `CreateIndex.cs` | Configures and instructs the `IndexManager` to proactively scan an existing table and materialize an entirely new `BPlus` or `Binary` persistent clustered index topology mapping assigned fields. |
| `CreateTable.cs` | Validates initial schema blueprints resolving unique index configurations and dynamically registering Foreign Key constraints locally. |
| `DropDatabase.cs, DropIndex.cs, DropTable.cs` | Explicit teardown mechanisms sequentially cascading destructive cleanups avoiding zombie persistence layers dynamically tracking constraint relationships preventing orphaned children rows. |

## Dependencies & Interactions
Highly coupled to the `TableService` orchestrating safety protocols checking identifier collisions across the `Catalog` before executing directory mutations. Outputs standard `QueryResult` elements confirming destructive resolution.

## Implementation Specifics
* **Topological Checks:** Relies inherently on algorithm validation paths (`TopologicalSort.cs`) ensuring tables aren't dropped if they are the primary foreign keys maintaining constraints on external references.
