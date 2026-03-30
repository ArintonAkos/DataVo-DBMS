import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: BTree",
  shardName: "btree",
  match: (source) => source.startsWith("DataVo.Tests/BTree/"),
});
