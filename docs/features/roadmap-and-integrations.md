# Roadmap and Integrations

This page explains the integration direction around ADO.NET, Entity Framework, vector embeddings, and WebAssembly.

## Current state vs roadmap

It is important to distinguish between:

- **available today**
- **partially present in the repository**
- **planned roadmap work**

## Available today

### Local packaging

Today, the repository supports local packaging of:

- `DataVo.Core`
- `DataVo.Data`

That means the project is already usable in local development workflows, even though public NuGet publication is still pending.

### Embedding through `DataVo.Core`

The clearest integration path today is direct embedding through the core package and `DataVoContext` / `DataVoEngine`.

### Browser and WebAssembly runtime

The repository includes a browser/WASM runtime and docs playground flow.

What this enables today:

- interactive SQL execution in the docs site
- browser parity scenarios generated from .NET E2E inputs
- strict parity test lanes for browser execution

Primary workflow:

```bash
bash ./scripts/deploy-browser-wasm.sh
cd docs
npm run docs:dev
```

### Security and auth command surface

Principal, grant, and session auth SQL command families are implemented.

See [Security and Authentication](./security-and-authentication.md) for command examples and inspection flows.

## ADO.NET direction

`DataVo.Data` is the natural place for ADO.NET-facing integration.

### What to tell developers today

- the repository already contains `DataVo.Data`
- local packages are available now
- the polished public ADO.NET provider story is still evolving

### What that means in practice

If you want to experiment today:

- package locally
- reference the local packages
- use the core engine directly where needed

If you want a polished public provider experience with stable external documentation, that is still part of the product maturation path.

## Entity Framework support

Entity Framework support is a roadmap item.

### Intended meaning

The long-term goal is that `DataVo` should be approachable from mainstream .NET data stacks, not only from direct engine APIs.

### Current status

- preview integration package and helpers are present in the repository
- not documented as fully production-ready yet
- additional provider hardening/documentation remains on the roadmap

## Vector embedding support

Vector embedding support is explicitly part of the product direction.

Detailed execution plan:

- [Vector DB + HNSW roadmap](vector-db-hnsw-roadmap.md)

### Why it matters

The project is aiming to reduce the friction of embedding/vector workflows that often require external plugins in other local databases.

### Current status

- implemented indexing foundations exist, including HNSW-related workstreams
- production hardening and end-user integration guidance are still evolving

## WebAssembly support

WebAssembly support is available today for docs/playground scenarios, with further hardening still on the roadmap.

### Why it matters

- browser and edge execution scenarios
- interactive demos
- lighter embedded developer experiences

### Current status

- browser runtime assets and deploy scripts are implemented
- strict browser parity testing is available
- additional packaging/distribution polish remains planned

## NuGet publication status

| Distribution mode        | Status        |
| :----------------------- | :------------ |
| local packaging          | available now |
| public NuGet publication | planned       |

## Recommended message to developers right now

If you are evaluating `DataVo` today, the clearest statement is:

> `DataVo` can already be packaged and consumed locally as `DataVo.Core` and `DataVo.Data`. Public NuGet publication, a polished ADO.NET story, Entity Framework integration, vector embedding features, and WebAssembly support are part of the active roadmap.

## Updated practical message

Use this summary when introducing the project today:

> `DataVo` supports local packaging, direct embedding via `DataVo.Core`, browser/WASM playground execution, and principal/auth SQL commands today. Public package distribution and broader provider-level production polish remain active roadmap items.
