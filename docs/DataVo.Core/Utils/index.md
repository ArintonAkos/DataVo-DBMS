# Utils Overview
The `Utils` module serves as a repository for global helper functions, C# extension methods, and generic boilerplate reducers used cross-functionally throughout `DataVo.Core`. Since it contains purely functional statics and pure helpers, it holds no systemic state and dictates no business logic.

## Core Responsibilities
* **Syntactic Sugar:** Extends foundational C# types (Dictionaries, Lists, Strings) with custom chainable operations tailored for database manipulations.
* **I/O Helpers:** Standardizes repetitive interactions with raw file paths and system inputs.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `ConsoleInputHandler.cs` | Provides secure, normalized terminal input reading for database prompts or localized interactive sessions. |
| `DictionaryComparer.cs` | Evaluates semantic equality across row dictionaries, handling disparate data types to securely determine dataset uniqueness. |
| `DictionaryExtensions.cs` | Extracts null-safe retrieval logic or batched mutators for concurrent dictionary maps used heavily in indexing and caching. |
| `DynamicObjectComparer.cs` | Normalizes dynamic type sorting gracefully routing execution checks handling mismatched types robustly when applying Order By matrices natively validating objects securely. |
| `FileHandler.cs` | Encapsulates standardized file creation, locking, and byte-stream opening mechanisms ensuring safe OS-level handles. |
| `ListExtensions.cs` | Provides custom list manipulation algorithms used to efficiently merge, slice, or filter result sets during query execution. |
| `StringExtensions.cs` | Supplies text transformations, case-insensitive comparators, and trimming routines heavily used by the Lexer and AST serializers. |

## Dependencies & Interactions
As an auxiliary module, `Utils` has zero dependencies on other parts of `DataVo.Core` but is universally consumed. The `StorageEngine` leverages `FileHandler`, the `Parser` constantly invokes `StringExtensions`, and the `BTree` indices rely heavily on the collection extensions for manipulating nodes.
