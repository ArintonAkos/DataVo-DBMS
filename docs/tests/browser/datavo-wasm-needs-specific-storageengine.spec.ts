import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: StorageEngine",
  shardName: "storageengine",
  match: (source) => source.startsWith("DataVo.Tests/StorageEngine/"),
});
