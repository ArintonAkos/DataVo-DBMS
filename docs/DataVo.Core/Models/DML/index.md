# DML (Models) Overview
The `DML` (Data Manipulation Language) models capture the instructions provided by queries responsible for mutating rows mapping specifically to the active schema. They wrap conditions and payload maps translating user intents like `INSERT` or `UPDATE` into execution units.

## Core Responsibilities
* **Data Mutation Maps:** Store the dictionary layouts mapping specific column targets to new scalar values or dynamically evaluated expressions.
* **Target Scoping:** Contain the logical `WhereExpression` nodes directing the storage engine toward the exact rows necessitating an update or obliteration.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `DeleteFromModel.cs` | Tracks the target `TableName` and encapsulates a `WhereExpression` node utilized to filter which elements require deletion. |
| `InsertIntoModel.cs` | Defines the columns targeted by the insertion query and a robust memory list mapping `List<Dictionary<string, dynamic>>` representing the raw payload rows to serialize. |
| `UpdateModel.cs` | Embeds the `TableName`, target `SetExpressions` applying logic sequentially over bounded rows, and the `WhereExpression` bounding the modification scope. |

## Dependencies & Interactions
Built by the recursive functions found within `Parser/DML`, these instances are passed downward into the concrete `DeleteFrom.cs`, `InsertInto.cs`, or `Update.cs` evaluation blocks. They act exclusively as passive payload transporters determining what rows in a table require active mutating.

## Implementation Specifics
* **Supported Capabilities:** `InsertIntoModel` natively supports multi-row batched payloads seamlessly reducing parse-time latency for large migrations. 
* **Evaluation Nuance:** The models rely on the `Evaluator` layer natively processing complex mathematical expressions (e.g., `UPDATE X SET Value = Value + 10`) dynamically mapping the calculations specified in `SetExpressions`.
