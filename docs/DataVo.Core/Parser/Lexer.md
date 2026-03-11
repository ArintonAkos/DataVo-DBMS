# Lexer

`Lexer` tokenizes raw SQL into a flat token stream consumed by the parser.

## What it supports

- keywords from `SqlKeywords`
- identifiers, including dotted forms such as `Users.Name`
- string literals
- integer and floating-point literals
- punctuation such as `(`, `)`, `,`, `*`
- comment removal for `-- ...` and `/* ... */`

## Example

```csharp
var lexer = new Lexer("SELECT Id, Name FROM Users WHERE Id >= 1");
List<Token> tokens = lexer.Tokenize();
```

## Contributor notes

- multi-character operators are matched before single-character operators
- `AND` and `OR` are emitted as operator tokens rather than generic keywords
- keyword values are normalized to uppercase
