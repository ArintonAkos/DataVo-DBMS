# WebAssembly and npm Integration

This page explains the end-user path for running DataVo in browser and JavaScript ecosystems.

## What is available today

DataVo includes browser runtime assets and a working WebAssembly docs playground flow.

Current supported workflow:

```bash
bash ./scripts/deploy-browser-wasm.sh
cd docs
npm install
npm run docs:dev
```

This deploys runtime assets into the docs site and enables interactive SQL execution.

## npm package plan

A public npm package is planned for streamlined integration.

Planned install command:

```bash
npm install @datavo/wasm
```

Planned package goals:

- predictable initialization API
- browser-first runtime support
- integration guidance for bundlers and frameworks
- versioned release flow aligned with core engine releases

## JavaScript usage model (planned public package)

```ts
import { initialize, executeSql } from "@datavo/wasm";

await initialize();
const result = await executeSql("SELECT 1 AS value;");
console.log(result);
```

## Why this matters for customers

- Use DataVo in demos, local sandboxes, and browser-native workflows.
- Keep SQL semantics aligned with core engine behavior.
- Reduce friction in local-first and offline-capable product experiences.

## Notes on support scope

- Browser WebAssembly runtime support is available now.
- npm publication is in deployment preparation.
- Refer to roadmap docs for package publication milestones.

## Related pages

- [Setup and Packaging](./setup-and-packaging.md)
- [Roadmap and Integrations](./roadmap-and-integrations.md)
- [Getting Started](./getting-started.md)
