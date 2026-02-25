# AST Build Sequence (Detailed)

## Query to AST

```mermaid
sequenceDiagram
    participant U as User
    participant QE as QueryEngine
    participant LX as Lexer
    participant PR as Parser
    participant EV as Evaluator

    U->>QE: SQL string
    QE->>LX: Tokenize(sql)
    LX-->>QE: List<Token>
    QE->>PR: Parse(tokens)
    PR-->>QE: List<SqlStatement>
    QE->>EV: ToRunnables(statements)
    EV-->>QE: Queues of actions
```

## Expression AST shape

For a query like:

`SELECT * FROM Employees WHERE Salary >= 90000 AND DeptId = 1`

Typical expression tree:

```mermaid
graph TD
    AND[BinaryExpressionNode AND]
    GE[BinaryExpressionNode >=]
    EQ[BinaryExpressionNode =]
    C1[ColumnRefNode Salary]
    V1[LiteralNode 90000]
    C2[ColumnRefNode DeptId]
    V2[LiteralNode 1]

    AND --> GE
    AND --> EQ
    GE --> C1
    GE --> V1
    EQ --> C2
    EQ --> V2
```

## Bound expression tree

After binder pass, unresolved column refs are replaced by resolved refs:

```mermaid
graph TD
    AND[BinaryExpressionNode AND]
    GE[BinaryExpressionNode >=]
    EQ[BinaryExpressionNode =]
    RC1[ResolvedColumnRefNode Employees.Salary]
    V1[LiteralNode 90000]
    RC2[ResolvedColumnRefNode Employees.DeptId]
    V2[LiteralNode 1]

    AND --> GE
    AND --> EQ
    GE --> RC1
    GE --> V1
    EQ --> RC2
    EQ --> V2
```

## Notes

- Parser should not access catalog/table metadata.
- Binder performs semantic resolution.
- Evaluator should execute against resolved expressions.
