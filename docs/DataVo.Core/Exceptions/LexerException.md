# LexerException

`LexerException` is thrown when SQL tokenization fails.

## Typical causes

- malformed string literals
- unexpected characters
- broken token boundaries

Contributor tip: lexer errors should be precise and fast-failing because they are the earliest validation boundary in the SQL pipeline.
