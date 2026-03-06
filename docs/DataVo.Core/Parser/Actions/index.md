# Actions (Parser) Overview
The `Actions` module serves as the primary execution wrapper within the `Parser` architecture encompassing evaluated models and implementing the standard database execution contract. An `Action` represents the final, executable state instantiated by the parser reflecting the user's intended target query.

## Core Responsibilities
* **Interface Resolution:** Establishes the common parent class implementing `IDbAction` that universally executes an operation passing system messages up to the top-level handler.
* **Execution Telemetry:** Tracks diagnostic console messages (`Rows affected: X`) resulting directly from an evaluation attempt.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `BaseDbAction.cs` | The abstract parent class offering standardized hooks exposing execution telemetry arrays, output `Results`, and declaring the virtual `PerformAction(Guid session)` endpoint. |

## Dependencies & Interactions
The `Parser` outputs concrete definitions extending this abstract class (like `Parser/DML/InsertInto.cs` or `Parser/DQL/Select.cs`). The external `Server` then accepts an instance constrained by `IDbAction`, invokes `PerformAction`, and intercepts the final `Messages` array to send back over the pipeline.
