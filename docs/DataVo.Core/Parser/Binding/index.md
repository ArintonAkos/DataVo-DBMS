# Binding (Parser) Overview
The `Binding` module bridges the gap between syntactic correctness (parsed strings) and semantic correctness (actual database objects). It validates that the column references and table identifiers generated during the parsing phase genuinely exist within the operational `Catalog` schema before allowing execution.

## Core Responsibilities
* **Semantic Analysis:** Inspects syntax trees ensuring raw string identifiers accurately map to loaded physical schema entities.
* **Alias Resolution:** Safely untangles SQL correlations assigning columns to explicitly declared aliases acting temporarily inside queries.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `SelectBinder.cs` | The central validation service orchestrating identifier resolution specifically validating deep select graphs involving join structures or correlated WHERE filters. |

## Dependencies & Interactions
This binding service dynamically interfaces with `DataVo.Core/Models/Catalog` utilizing the active dictionary cache to ensure requests map correctly. Instantiated directly by the `Select` evaluator to sanitize expressions before the engine attempts physical disk fetches.

## Implementation Specifics
* **Strict Enforcement:** Aggressively traps undefined references immediately post-parse throwing domain `BindingException` instances explicitly pointing out unmatched aliases before execution begins.
