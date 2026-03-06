# Commands (Parser) Overview
The `Commands` parsing actions manage execution vectors responsible for interactive terminal directives and high-level database state mutations. These commands abstract interactions typical to CLI consoles rather than strict algebraic SQL projections.

## Core Responsibilities
* **Interactive TTY Actions:** Provide system-level diagnostic actions exposing database topology mappings (`SHOW TABLES`, `DESCRIBE`).
* **State Control:** Control the active session memory pointers routing operations dynamically between distinct databases (`USE`).

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `Describe.cs` | Retrieves internal table blueprint configurations rendering the types directly to the user display. |
| `Go.cs` | Represents an arbitrary terminal execution break point parsing boundary. |
| `ShowDatabases.cs, ShowTables.cs` | List operations scanning absolute directories retrieving available root artifacts. |
| `Use.cs` | Establishes the volatile system lock binding session variables resolving the target `Database` namespace constraint. |

## Dependencies & Interactions
As standalone implementation subsets extending `BaseDbAction`, they interface exclusively with standard `DataVoContext` memory endpoints manipulating session states rather than interacting directly with traditional `StorageEngine` file manipulations.
