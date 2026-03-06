# Contracts Overview
The `Contracts` module formalizes the interface boundaries and shared protocol definitions for the broader `DataVo.Core` ecosystem. By relying on an abstraction layer rather than concrete classes, it enables loose coupling and inversion of control across separated system components like the logical query evaluators and the physical storage engines.

## Core Responsibilities
* **Type Abstraction:** Defines the fundamental interfaces representing executable database actions and physical target schemas.
* **Response Standardization:** Enforces structured result formats indicating the success, failure, and payload format of any evaluated query component.

## Component Breakdown

| Component (File/Dir) | Architectural Role |
|----------------------|--------------------|
| `Results/` | Confines abstract wrappers dictating the uniform shape of successful executions or thrown parsing errors. |
| `IColumn.cs` | Defines the generic interface contract encompassing the required metadata and parsing behavior for table columns. |
| `IDbAction.cs` | Represents an executable database operation mandated to support an asynchronous execution protocol. |

## Dependencies & Interactions
The `Contracts` module interacts comprehensively across `Models`, `Services`, `Parser`, and the `StorageEngine`. The parser generates concrete instances of `IDbAction`, which are strictly typed definitions capable of returning standardized `Results`. These interfaces provide the bedrock on which the broader `Server` can route operations without relying on low-level engine specifics.
