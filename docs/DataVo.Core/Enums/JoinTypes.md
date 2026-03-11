# JoinTypes

`JoinTypes` contains the canonical string values used to represent join modes.

## Supported joins

- `INNER`
- `LEFT`
- `RIGHT`
- `FULL`
- `CROSS`

## Developer note

These values are intentionally centralized so parser normalization and join strategy dispatch use the same identifiers.
