# DQL (Parser Actions) Overview
The `DQL` component acts as the high-level orchestrator conducting the retrieval and complex filtering required to execute projection logic typically originating from a `SELECT` statement mapping parsed criteria into relational calculus.

## Core Responsibilities
* **Select Sequencing:** Defines the algorithmic procedure evaluating complex SQL mapping loops sequentially structuring filtering paths, executing Joins, processing aggregations, and culminating bound grouping lists.
* **Expression Evaluation:** Unifies external math models resolving nested filters extracting records mapped cleanly onto transient memory payloads.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `Select.cs` | The standalone orchestrator dynamically generating temporary internal cross-sections mapping `QueryResult` datasets utilizing distinct evaluation mechanisms mapping nested loop paths dynamically. |

## Dependencies & Interactions
The `Select` operator directly links `DataVo.Core/Parser/Statements/Mechanism` applying distinct join paths validating aliases mapping actively across `SelectBinder.cs` instances. Produces exactly formatted `QueryResult` buffers for active routing out to client consoles.

## Implementation Specifics
* **Execution Pathways:** Dynamically branches execution logic choosing either `StatementEvaluator.cs` or `StatementEvaluatorWOJoin.cs` depending purely stringently on topological mapping parameters resolving complex data fetches optimizing loop thresholds seamlessly across the Engine cache system.
