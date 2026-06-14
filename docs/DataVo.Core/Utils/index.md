# Utils Overview

General-purpose helper types and extensions shared across `DataVo.Core`.

## Component breakdown

| Component (File) | Purpose |
|------------------|---------|
| `ConsoleInputHandler.cs` | Reads normalized console input for interactive scenarios. |
| `DictionaryComparer.cs` | Compares row dictionaries for structural equality. |
| `DictionaryExtensions.cs` | Convenience helpers for dictionary operations. |
| `DynamicObjectComparer.cs` | Ordering/comparison across `object?` values. |
| `FileHandler.cs` | File and directory helpers used by storage and indexing. |
| `ListExtensions.cs` | Collection helpers used across evaluation and indexing. |
| `StringExtensions.cs` | String helpers used by lexer/parser and general utilities. |
| `AtomicFileOperations.cs` | Helpers for atomic write/replace patterns. |
| `SimdDistanceKernels.cs` | SIMD-backed distance kernels for vector workloads. |
| `VectorParser.cs` | Coerces values into vector shapes (e.g., `float[]`) used by vector indexes. |

## Notes

Most utilities are pure/static helpers and should not hold long-lived mutable state.
