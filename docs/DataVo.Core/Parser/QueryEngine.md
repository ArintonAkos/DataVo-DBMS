# QueryEngine

`QueryEngine` coordinates the end-to-end SQL pipeline for a query batch.

## Stages

1. tokenize SQL with `Lexer`
2. parse tokens into AST statements
3. convert statements into executable actions with `Evaluator`
4. execute actions and collect `QueryResult` objects

## Debugging support

Set `DATAVO_PARSER_DEBUG=1` to emit parser diagnostics such as the incoming query, token list, and parsed statement types.
