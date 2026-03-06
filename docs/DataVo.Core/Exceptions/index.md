# Exceptions Overview
The `Exceptions` module defines a comprehensive hierarchy of domain-specific exception types tailored to the various failure modes of the `DataVo.Core` engine. Throwing explicit exception types ensures that execution faults—whether caused by malformed input or missing database objects—can be caught, categorized, and intelligently translated into standard error responses.

## Core Responsibilities
* **Granular Fault Identification:** Provides unique exception classes mapped to distinct phases of execution (e.g., lexical errors vs. evaluation errors).
* **Debugging Context:** Encapsulates metadata and informative stack information to pinpoint where an operation degraded within the engine pipeline.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `BindingException.cs` | Thrown when a parsed column name or table identifier fails to resolve against the active catalog schema. |
| `EvaluationException.cs` | Thrown during query execution when a logical error occurs, such as a type mismatch in mathematical operations. |
| `FileExtensionNotSupported.cs` | Thrown by the storage engine when attempting to read or write persistent data from a malformatted internal file type. |
| `LexerException.cs` | Thrown during the initial tokenization phase when unidentifiable or malformed characters are encountered in the input string. |
| `NoSourceFileProvided.cs` | Thrown when utility loaders or storage engines are instantiated without a valid path to an on-disk database file. |
| `ParserException.cs` | Thrown when the recursive descent parser encounters unexpected tokens that violate the grammatically expected SQL structure. |

## Dependencies & Interactions
Exception classes are instantiated heavily across the `Parser` and `Services` modules, representing the primary feedback mechanism for gracefully failing malformed user input. These exceptions are typically caught at the outermost `DataVoContext` or `Server` module boundary and converted into descriptive, standardized error payloads for the frontend client.
