# DataVo

[![license-MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

DataVo is a C#-native SQL engine focused on deterministic behavior, embeddable runtime integration, and transparent internals.

It includes:

- a full in-process SQL pipeline (lexer, parser, model binding, execution)
- in-memory and disk-backed storage modes
- B-Tree and vector indexing infrastructure
- transaction and MVCC foundations
- browser WebAssembly runtime assets used by the documentation playground

## Project Status

DataVo is actively developed and used as a local-first engine/runtime.

- local packaging is supported today (`DataVo.Core`, `DataVo.Data`)
- browser/WASM docs playground is supported today
- auth/session SQL commands are implemented (`CREATE USER`, `CREATE ROLE`, `GRANT`, `REVOKE`, `LOGIN`, `LOGOUT`, `SHOW USERS/ROLES/GRANTS`)
- public package distribution and broader provider polish are still evolving

## Quick Start (.NET Embedding)

```bash
dotnet build DataVo.sln
```

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

context.Execute("CREATE DATABASE Demo");
context.Execute("USE Demo");
context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
context.Execute("INSERT INTO Users VALUES (1, 'Alice')");

var result = context.Execute("SELECT * FROM Users ORDER BY Id");
```

## Quick Start (Docs + WASM Playground)

```bash
bash ./scripts/deploy-browser-wasm.sh
cd docs
npm install
npm run docs:dev
```

The deploy script publishes browser assets and copies them to `docs/public/datavo-wasm`.

## Build, Test, Package

Core workflows:

```bash
dotnet build DataVo.sln
dotnet test DataVo.Tests/DataVo.Tests.csproj
dotnet pack DataVo.sln -c Release
```

Output packages:

- `artifacts/packages/DataVo.Core.<version>.nupkg`
- `artifacts/packages/DataVo.Data.<version>.nupkg`

Useful stress/perf lanes:

```bash
bash ./scripts/test-hnsw-fast.sh
bash ./scripts/test-hnsw-perf.sh
bash ./scripts/test-browser-strict-stress.sh
bash ./scripts/test-relational-hardening.sh
bash ./scripts/phase-closeout.sh
```

## Security and Auth SQL Surface

Implemented command families:

- principals: `CREATE USER`, `CREATE ROLE`
- grants: `GRANT`, `REVOKE`
- session auth: `LOGIN`, `LOGOUT`
- inspection: `SHOW USERS`, `SHOW ROLES`, `SHOW GRANTS`, `SHOW GRANTS FOR USER`, `SHOW GRANTS FOR ROLE`

See documentation pages under `docs/features` for examples and guardrails.

## Repository Layout

- `DataVo.Core`: engine runtime, parser, execution, storage, indexing, transactions
- `DataVo.Data`: data-access/provider-facing integration surface
- `DataVo.EntityFrameworkCore`: EF integration layer and helpers
- `DataVo.Browser`: browser/WASM runtime host
- `DataVo.Tests`: unit, integration, and E2E tests
- `docs`: VitePress documentation and browser parity tests

## Documentation

Run local docs:

```bash
cd docs
npm install
npm run docs:dev
```

Key entry points:

- `docs/index.md`
- `docs/features/index.md`
- `docs/features/getting-started.md`
- `docs/features/setup-and-packaging.md`

## Contributing

1. Open an issue describing the bug/feature.
2. Add or update tests in `DataVo.Tests`.
3. Run build + relevant test lanes locally.
4. Update docs for SQL/API behavior changes.

## License

MIT. See [LICENSE](LICENSE).
