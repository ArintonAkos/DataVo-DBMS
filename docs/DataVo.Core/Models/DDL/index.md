# DDL (Models) Overview
The `DDL` (Data Definition Language) models embody the structural definitions and targets provided by commands altering the database schema (`CREATE`, `DROP`). They serve as the passive data transfer objects transporting instructions from the syntactic `Parser` to the logical execution `Services`.

## Core Responsibilities
* **Action Transport:** Encapsulate all necessary parameters (table name, column maps, index columns) required to create or dismantle database entities.
* **Separation of Concerns:** De-couples the raw Abstract Syntax Tree node from the actual `CreateTableModel` containing cleanly parsed primitive arrays.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `CreateDatabaseModel.cs` | Wraps the target identifier required to provision a new root directory tracking a distinct database space. |
| `CreateTableModel.cs` | Encapsulates the table definition including its columns, primitive types, nullability boundaries, and explicitly parsed foreign keys mapped by the user. |
| `CreateIndexModel.cs` | Holds target specifications detailing which table explicitly receives a new secondary B-Tree indexing structure across defined columns. |
| `DropDatabaseModel.cs, DropTableModel.cs, DropIndexModel.cs` | Provide the simple identifier wrappers utilized by the Table/Catalog Services to locate and obliterate database artifacts. |

## Dependencies & Interactions
These models are constructed exclusively by the specific components residing within `DataVo.Core/Parser/DDL`. After instantiation, they are consumed by mapping services like `TableService.cs` or the `DataVoContext` implementation logic fulfilling the physical file creation requests.

## Implementation Specifics
* **Constraints Tracked:** `CreateTableModel` explicitly defines if a column constitutes a Primary Key, Unique Key, or references external Foreign Keys prior to schema instantiation.
