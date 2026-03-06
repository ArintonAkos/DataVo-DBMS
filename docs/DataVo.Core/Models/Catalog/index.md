# Catalog (Models) Overview
The `Catalog` module is the beating heart of schema definition in `DataVo.Core`. It houses the physical representation of the database file structures as they exist logically within memory. It defines exactly what tables exist, their columns, their dependencies, and any foreign constraints binding them together.

## Core Responsibilities
* **Schema Definition:** Tracks the absolute state of Database topologies, capturing structural hierarchies (`Database` contains `Tables`, `Tables` contain `Columns` and `Indices`).
* **Constraint Specifications:** Maintains foreign key mappings, unique key declarations, and definitions mapped to underlying disk B-Tree files.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `Catalog.cs` | The central schema registry. Usually persisted as a root JSON or metadata file within the database directory pointing to the active table definitions. |
| `Database.cs` | Represents high-level context of a distinct database tracking a localized collection of related tables. |
| `Table.cs` | Defines a single table entity, listing its column datatypes, primary key compositions, and foreign dependencies. |
| `Column.cs, Field.cs` | Define atomic structural requirements (Type constraints, Nullability, Size limits) belonging to rows within tables. |
| `ForeignKey.cs, Reference.cs` | Define referential constraints linking child tables to parent entities utilized to enforce `ON DELETE CASCADE` or `RESTRICT` behaviors. |
| `IndexFile.cs` | Metadata pointing to the dedicated `_PK`, `_UK`, or `_FK` clustered B-Tree files on disk serving the table. |

## Dependencies & Interactions
As the single source of truth for schema validation, virtually all `DataVo.Core` subsystems reference the `Catalog`. When `Parser/Binding` resolves an identifier like `Users.Id`, it looks it up in `Catalog`. When `StorageEngine` writes a row, it strictly enforces types mapped within the corresponding `Table.cs` definition.

## Implementation Specifics
* **Supported Features:** Robust primary/unique key modeling, composite indices definitions, and dynamic table drops cascading to index teardown. Foreign Key metadata specifically tracks `CASCADE` or `RESTRICT` states for enforcement during DML modifications.
