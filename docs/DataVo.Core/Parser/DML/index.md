# DML (Parser Actions) Overview
The `DML` module within the `Parser` acts as the execution bridge physically mutating raw records belonging to an established schema. It translates analytical statements (`INSERT`, `UPDATE`, `DELETE`) into transactional updates leveraging the disk storage layer and associated index arrays.

## Core Responsibilities
* **Persistence Writing:** Calculates raw data inserts dispatching instructions to dynamically update physical `.table` files alongside cascading insertions mapping secondary B-Tree elements.
* **Constraint Validation Checks:** Acts as the primary enforcement layer asserting unique keys, primary keys, and complex `CASCADE/RESTRICT` foreign key integrity boundaries during mutations.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `DeleteFrom.cs` | Determines matched row ID arrays passing identifiers explicitly to storage contexts removing indices bounds. |
| `InsertInto.cs` | Analyzes batches executing parallel scalar binding inserting payloads translating literals into internal native byte alignments. |
| `Update.cs` | Executes an out-of-place record replacement replacing legacy values and allocating dynamic B-tree modifications explicitly generating batch constraints preventing intra-query duplication. |
| `Vacuum.cs` | Orchestrates a physical defragmentation system sweeping tombstoned nodes and physically clustering live binary chunks into continuous IO alignments. |

## Dependencies & Interactions
Interacts intimately with `StorageContext.cs` passing execution vectors resolving array manipulations evaluating complex expression sets derived tightly from `WhereModel.cs` filters mapping results down linearly.

## Implementation Specifics
* **Delete Methodologies:** Employs rigorous Foreign Key constraint resolution. If a child foreign key specifies `RESTRICT`, the deletion fails. If `CASCADE`, the deletion triggers recursive `ExecuteDelete` mechanisms cascading down child tables. Deletion in storage marks nodes typically via zeroed-out flag mechanisms (Tombstones), cleaned physically later by `VACUUM`.
* **Update Methodologies:** Utilizes out-of-place updating patterns (Delete previous record -> Insert entirely new record) reducing the physical complexity of reallocating variable length strings whilst maintaining continuous memory blocks dynamically.
