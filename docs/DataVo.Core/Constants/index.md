# Constants Overview

The `Constants` module is the centralized repository for static definitions and immutable magic strings essential to the `DataVo.Core` infrastructure. By isolating these literal values, it ensures grammatical consistency across the query compilation and execution pipeline, eliminating the risk of hardcoded string duplication.

## Core Responsibilities

- **Syntax Centralization:** Encapsulates keyword tokens, mathematical operators, and grammatical symbols used to analyze raw SQL input.
- **System Limits:** Establishes pre-defined capacity constraints and structural boundaries for the data models.

## Component Breakdown

| Component (File)        | Architectural Role                                                                                                                        |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `SqlSyntaxConstants.cs` | Holds the vast array of structural SQL string literals, mathematical operators, and keyword definitions utilized by the lexer and parser. |

## File Documentation

- [SqlSyntaxConstants](./SqlSyntaxConstants.md)

## Dependencies & Interactions

This module is primarily referenced by the `Parser` module during the lexical tokenization and syntactic analysis phases of query ingestion. It is also used by various `Services` to validate incoming syntax and generate standard system responses based on strict keyword bindings.
