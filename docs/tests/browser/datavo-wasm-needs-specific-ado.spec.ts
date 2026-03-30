import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: ADO",
  shardName: "ado",
  match: (source) => source.startsWith("DataVo.Tests/ADO/"),
  assertNonEmpty: true,
});
