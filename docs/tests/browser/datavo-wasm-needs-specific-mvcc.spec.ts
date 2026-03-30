import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: MVCC",
  shardName: "mvcc",
  match: (source) => source.startsWith("DataVo.Tests/MVCC/"),
});
