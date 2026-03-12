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

# DataVo

[![build](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/) [![tests](https://img.shields.io/badge/tests-passing-brightgreen)](https://github.com/) [![license-MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

DataVo is a C#-native SQL engine with a browser-friendly WebAssembly build and an embeddable TypeScript/npm packaging option for easy demos and live playgrounds.

Quick highlights:

- Full SQL pipeline implemented in C# (Lexer → Parser → AST → Binder → Executor)
- In-memory and disk-backed storage engines, with B-Tree indexes
- WASM/browser target with a docs-hosted interactive SQL playground
- Test suite with end-to-end SQL regressions

Why publish this here? The project is both a technical demonstration and a foundation for embedding a deterministic SQL engine in apps or demos without relying on external DB services.

## Quickstart (npm-distributed WASM bundle)

If you publish the prebuilt browser bundle as an npm package (suggested package name: `@your-org/datavo-wasm`), consumers can install and run the interactive playground or embed the engine in a web app with two steps.

Install the package (example):

```bash
npm install @your-org/datavo-wasm
```

Example usage (browser/edge):

```ts
import { initialize, executeSql } from "@your-org/datavo-wasm";

await initialize(); // loads and initializes the WASM runtime
const result = await executeSql(
  "CREATE DATABASE Demo; USE Demo; CREATE TABLE Users (UserId INT, UserName VARCHAR); INSERT INTO Users VALUES (1,'Alice'); SELECT * FROM Users;",
);
console.log(result);
```

If you prefer shipping the WASM assets alongside your docs site, `./scripts/deploy-browser-wasm.sh` shows the exact files copied into `docs/public/datavo-wasm` during publishing.

## Local development (engine and docs)

Build and run tests locally:

```bash
dotnet build
dotnet test
```

Run the docs site (includes the integrated SQL playground):

```bash
./scripts/deploy-browser-wasm.sh
cd docs
npm install
npm run docs:dev   # or `npm run docs:build` to build static site
```

Notes: the `deploy-browser-wasm` script publishes the browser-wasm project and copies the runtime files and `datavo.interop.js` into the docs site.

## API & integration notes for the npm bundle

- Provide a small JS/TS API surface from the package that abstracts loading and calling the WASM bundle (e.g. `initialize()`, `executeSql(sql)`, and a `reset()` helper).
- Include prebuilt `datavo.wasm` and the minimal JS shims (`dotnet.*.js` + `datavo.interop.js`) so consumers do not need a .NET toolchain.
- Document known browser limitations (trimming/AOT differences and that the initial boot can take a couple seconds on first load).

## Known caveats

- WASM publish emits a small number of warnings related to runtime pinvoke detection; these are benign for the demo flow but noted for transparency.
- We temporarily disabled trimming while debugging WASM boot issues; if you enable aggressive trimming you should test the published bundle across browsers.

## Contributing

Contributions welcome. Suggested first tasks:

- triage small issues and label `good first issue`
- add CI to run `dotnet test` and `npm run docs:build` on PRs
- improve README/docs examples showing how to embed the npm bundle

## License

MIT — see [LICENSE](LICENSE)

## Contact

Open issues or PRs on GitHub; ping me for a walkthrough demo.
