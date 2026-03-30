import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: Indexing",
  shardName: "indexing",
  match: (source) => source.startsWith("DataVo.Tests/Indexing/"),
});
