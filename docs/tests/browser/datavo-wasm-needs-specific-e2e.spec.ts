import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: E2E",
  shardName: "e2e",
  match: (source) => source.startsWith("DataVo.Tests/E2E/"),
});
