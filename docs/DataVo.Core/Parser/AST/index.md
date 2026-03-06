# AST (Parser) Overview
The `AST` (Abstract Syntax Tree) module provides the foundational hierarchical types used by the `Parser` to model complex SQL grammar. By transforming a linear SQL string into a strict tree data structure, `DataVo.Core` guarantees order-of-operations compliance and structured context validation.

## Core Responsibilities
* **Hierarchical Definition:** Exposes the base nodes formulating any parsed mathematical context, structural command, or conditional Boolean operator.
* **Node Categorization:** Separates abstract literals from operational identifiers maintaining strict syntax boundaries.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `SqlNode.cs` | Exposes the universally extended foundational base classes (`ExpressionNode`, `StatementNode`, `IdentifierNode`, etc.) indicating how an evaluation branch splits dynamically across left and right contexts. |

## Dependencies & Interactions
The `Parser.cs` instantiates instances of classes derived from `SqlNode` dynamically when moving left-to-right processing tokens. The `AST` operates effectively detached from the `Catalog` schema during parsing, maintaining strict structural grammar representation before being validated by `Binding` actions or parsed into the `Models` definition structures. 

## Implementation Specifics
* **Tree Format:** Nodes aggressively favor binary splits ensuring evaluators implement recursive, post-order traversal matching established mathematical calculation sequences.
