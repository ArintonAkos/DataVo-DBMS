import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: Execution",
  shardName: "execution",
  match: (source) => source.startsWith("DataVo.Tests/Execution/"),
});
