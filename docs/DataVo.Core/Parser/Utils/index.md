# Utils (Parser) Overview

Helper types used by the parser and expression evaluation pipeline.

## Components

| Component (File) | Purpose |
|------------------|---------|
| `AggregateExpressionFormatter.cs` | Formats aggregate expressions (for headers / display). |
| `ExpressionNodeNormalizer.cs` | Normalizes/simplifies expression trees. |
| `ExpressionValueComparer.cs` | Comparison helpers across numeric/string/object values. |
| `ParserConfig.cs` | Parser configuration limits and switches. |
| `ParserSyntaxHelper.cs` | Token inspection helpers shared by lexer/parser components. |
| `ScalarEvaluator.cs` | Evaluates scalar expressions against a row scope. |

## Notes

These utilities do not perform storage operations directly; they are plumbing helpers used by parsing and evaluation layers.
