# IDbAction

`IDbAction` is the execution contract produced by the parse/evaluate pipeline.

## Pipeline role

1. `Lexer` tokenizes SQL
2. `Parser` builds AST statements
3. `Evaluator` turns AST statements into `IDbAction` instances
4. each action executes through `Perform(Guid session)`

## Why it matters

This contract keeps the parser phase separate from execution. It also makes it easier to add new statement families because the rest of the pipeline only needs an object that can return a `QueryResult`.
