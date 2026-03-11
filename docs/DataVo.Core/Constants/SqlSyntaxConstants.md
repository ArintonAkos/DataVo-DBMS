# SqlSyntaxConstants

`SqlSyntaxConstants.cs` centralizes the tokens that define the SQL grammar recognized by `DataVo`.

## Contents

- `SqlKeywords`: reserved words such as `SELECT`, `UPDATE`, `ALTER`, and transaction commands
- `SqlPunctuation`: single-character and tokenized punctuation markers
- `SqlLiterals`: special literal expressions used by parsing helpers

## Why it matters

Keeping grammar tokens in one place makes parser changes safer. When new features such as `EXISTS`, `UNION`, or `ALTER TABLE MODIFY` are added, the first step is usually extending this constants file.

## Contributor guidance

When adding grammar support:

1. add the token here
2. update lexer/parser logic
3. add end-to-end tests
4. update the developer docs in `docs/DataVo.Core/Parser`
