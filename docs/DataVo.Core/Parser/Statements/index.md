# Statements (Parser) Overview
The `Statements` module acts as the individual parsing handlers for sub-clauses appearing within massive complex SQL statements like `SELECT` or `UPDATE`. They are solely responsible for converting raw grammar strings representing filters, groupers, or relational boundaries into the distinct typed `Models`.

## Core Responsibilities
* **Sub-clause Parsing:** Extract specific string chunks mapped by the `Lexer` breaking down massive commands into digestible filtering boundaries ensuring syntactic compliance.
* **Component Isolation:** Validating independent SQL clauses individually preventing monolithic logic paths intertwining parsing behaviors inside the `QueryEngine`.

## Component Breakdown

| Component (File/Dir) | Architectural Role |
|----------------------|--------------------|
| `JoinStrategies/` | Encapsulates concrete execution methodologies determining algorithm behavior executing join queries based strictly against evaluated model representations. |
| `Mechanism/` | Root processing engines dictating dynamic execution loop bounds resolving `Where` filters dynamically. |
| `Aggregate.cs, GroupBy.cs` | Isolate specific syntactical logic reducing selected nodes ensuring formula compliance building aggregate arrays correctly. |
| `Join.cs` | Resolves complex Cartesian relational links defining target catalogs tracking constraints mapping foreign references cleanly. |
| `Where.cs` | Recursive evaluator translating complex multi-conditional Booleans capturing literal bounds processing `AND`/`OR` thresholds dynamically. |

## Dependencies & Interactions
Primarily instantiated recursively by the outermost evaluation contexts (`Select`, `Update`, `DeleteFrom`). Returns instances directly matching definitions contained within the `DataVo.Core/Models/Statement` directory ensuring schema independence exclusively during grammar resolution phases.
