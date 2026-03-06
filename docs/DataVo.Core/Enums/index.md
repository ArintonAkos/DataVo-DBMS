# Enums Overview
The `Enums` module contains strongly-typed enumerations that categorize and standardize internal states, operational flags, and primitive variants across the `DataVo.Core` engine. By defining these constrained values, the system avoids loose string comparisons and enforces compile-time safety when evaluating expressions or routing execution paths.

## Core Responsibilities
* **Type Safety:** Replaces primitive integers or strings representing internal states with strongly-typed named constants.
* **Execution Routing:** Defines the variants of join strategies, data types, and logical operators used to govern branching logic within the execution and parsing engines.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `DataTypes.cs` | Defines the supported primitive SQL data types (e.g., Integer, Varchar, Boolean) recognized by the storage and expression engines. |
| `JoinTypes.cs` | Enumerates the varieties of SQL `JOIN` clauses (e.g., Inner, Left, Cross) determining the chosen join evaluation strategy. |
| `Operators.cs` | Lists the supported mathematical, logical, and comparative operators parsed from SQL syntax used during expression evaluation. |
| `Status.cs` | Represents high-level success or error flags returned dynamically by execution components to indicate transactional outcomes. |

## Dependencies & Interactions
As primitive type definitions, enums are a universal dependency utilized across every single layer of `DataVo.Core`. The `Parser` resolves strings to these enums, the `Models` strictly type their properties to these enums, and the `Services` consume them to execute specific behavioral switches.
