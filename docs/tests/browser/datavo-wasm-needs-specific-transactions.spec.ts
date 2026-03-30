import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: Transactions",
  shardName: "transactions",
  match: (source) => source.startsWith("DataVo.Tests/Transactions/"),
});
