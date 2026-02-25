# DataVo

![img](https://github.com/ArintonAkos/ABKR/assets/43067524/5f90c2f1-71a9-42a5-9bac-5128750fc089)

DataVo is a C# database engine project focused on predictable SQL execution, native in-memory and disk-backed storage, and planned first-class vector/GIS capabilities.

## Executive Summary

DataVo began as a university coursework codebase. Even in its early phase, SQL processing was implemented in-house (`Lexer -> Parser -> AST -> Executor`), while persistence depended on MongoDB.

The current direction is a full C#-native engine where storage, indexing, parsing, binding, and execution are controlled end-to-end in one architecture.

## Why this project is worth attention

### Engine ownership, not glue code

DataVo is not only a query wrapper around an external database. The core objective is to own the runtime path:

- SQL tokenization/parsing
- semantic binding
- statement execution
- storage abstractions (memory + disk)
- indexing mechanics

### Real-world problem being targeted

In .NET projects, embedding/vector workflows with SQLite typically require external extension plugins. This can increase setup/debug complexity across environments.

DataVo’s target is to provide native capabilities that reduce plugin friction and make development/testing workflows more deterministic.

### Practical engineering value

- faster local and CI testing without mandatory external DB containers for many scenarios
- easier diagnostics because execution is inside a managed C# codebase
- clearer performance tuning opportunities via architecture-level decisions

## Technical Design Direction

### AST-first SQL pipeline

DataVo follows an AST-first pipeline to separate concerns clearly:

1. Lexer produces tokens.
2. Parser builds AST.
3. Binder resolves symbols/semantics.
4. Executor runs bound statements.
5. Storage/index layers provide physical data access.

### Performance-oriented internals

Current and planned optimization strategy includes:

- page-oriented storage model
- cache-aware access patterns (L1/L2 locality mindset)
- reduced runtime lookup overhead via early binding
- SIMD-friendly operator evolution paths

For deeper details, see: [performance and storage design](docs/architecture/performance-and-storage-design.md).

### Cross-platform runtime

DataVo targets modern .NET and runs on Windows, Linux, and macOS. Runtime execution benefits from native machine code generation pathways (JIT and AOT in the .NET toolchain).

## Repository Focus

### DataVo.Core

Primary engine implementation:

- `Parser/`: lexer, parser, AST, binding, execution
- `StorageEngine/`: memory/disk abstractions and persistence context
- `BTree/`: indexing infrastructure
- `Models/`, `Services/`, `Exceptions/`, `Enums/`, `Constants/`, `Utils/`: shared domain and infrastructure support

### DataVo.Tests

Quality and regression safety net:

- `E2E/`: end-to-end SQL behavior
- `BTree/`: index correctness and persistence behavior
- `StorageEngine/`: storage-level behavior

## Architecture Docs

- [SQL pipeline and components](docs/architecture/sql-pipeline-and-components.md)
- [AST build sequence](docs/architecture/ast-build-sequence.md)
- [AST visualizer](docs/architecture/ast-tree-visualizer.html)
- [Performance and storage design](docs/architecture/performance-and-storage-design.md)

## Roadmap

- ACID transaction management
- ADO.NET provider support
- EF LINQ integration
- deeper vector and GIS embedding support
- WebAssembly target support for browser/edge scenarios

## Build and Test

```bash
dotnet build
dotnet test
```

## Contributing

Contributions are welcome via pull requests with clear scope and tests for behavioral changes.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
