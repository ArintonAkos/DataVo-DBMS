import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: Other",
  shardName: "other",
  match: (source) =>
    !source.startsWith("DataVo.Tests/ADO/") &&
    !source.startsWith("DataVo.Tests/AuditFixes/") &&
    !source.startsWith("DataVo.Tests/BTree/") &&
    !source.startsWith("DataVo.Tests/E2E/") &&
    !source.startsWith("DataVo.Tests/Execution/") &&
    !source.startsWith("DataVo.Tests/Indexing/") &&
    !source.startsWith("DataVo.Tests/MVCC/") &&
    !source.startsWith("DataVo.Tests/StorageEngine/") &&
    !source.startsWith("DataVo.Tests/Transactions/"),
});
