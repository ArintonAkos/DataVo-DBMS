# WebAssembly and npm Status

This page clarifies the exact status of DataVo's WebAssembly runtime (`DataVo.Browser`) and npm packaging.

> [!IMPORTANT]
> **Experimental / Playground Lane Only:** The WebAssembly runtime (`DataVo.Browser` / `docs/src/DataVoClient.ts`) exists strictly as an experimental browser playground and interactive documentation runtime. It is **not** a production-ready target and is **not** published on npm, in contrast to the production-ready .NET 10 embedded engine (`DataVo.Core` and `DataVo.Data`).

## Current Architecture & Scope

DataVo compiles a browser-targeted assembly (`DataVo.Browser`) that boots inside the .NET WebAssembly runtime and exposes JavaScript interop bindings consumed by `docs/src/DataVoClient.ts`.

| Target / Artifact | Status | Scope & Contract |
| :---------------- | :----- | :--------------- |
| .NET 10 Embedded Engine (`DataVo.Core`, `DataVo.Data`) | Release Candidate (`v0.2.0-rc.3`) | Production-oriented in-process engine with LSM durability, locked Native AOT, and 2,358 passing tests. |
| Browser WASM Runtime (`DataVo.Browser` / `docs/src/DataVoClient.ts`) | `Experimental / Playground Lane` | Powers interactive SQL examples in the documentation site and Playwright browser parity checks. Not hardened for production deployment. |
| npm Package (`@datavo/wasm`) | Not Published / Future Exploration | Not published on the npm registry. Do not depend on `@datavo/wasm` in production JavaScript or TypeScript applications. |

## Running the Documentation Playground Locally

Maintainers and contributors can build and deploy the WASM runtime assets into the VitePress documentation site to test interactive SQL blocks:

```bash
bash ./scripts/deploy-browser-wasm.sh
cd docs
npm install
npm run docs:dev
```

This workflow builds `DataVo.Browser`, copies the `_framework` and `datavo.interop.js` assets into `docs/public/datavo-wasm/`, and enables interactive SQL execution via `docs/src/DataVoClient.ts`.

## Why WebAssembly Is Scoped to the Playground Lane

- **Ephemeral Browser Execution:** In-browser execution runs against in-memory or experimental browser storage shims without the strict OS-level `fsync` durability guarantees of the native .NET 10 LSM engine.
- **No Public npm Contract:** There is no published `@datavo/wasm` package on npm, and the JavaScript interop surface in `DataVoClient.ts` is an internal documentation contract.
- **Focus on .NET 10 Core:** Engineering priority in v0.2 and v0.3 is locked on the embedded .NET 10 runtime, Native AOT compliance, and HTAP columnar execution.

## Related Pages

- [Setup and Packaging](./setup-and-packaging.md)
- [Roadmap and Integrations](./roadmap-and-integrations.md)
- [Getting Started](./getting-started.md)
