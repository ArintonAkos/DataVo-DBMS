import { registerNeedsSpecificSuite } from "./support/needs-specific-suite";

registerNeedsSpecificSuite({
  title: "DataVo WASM needsSpecific parity: AuditFixes",
  shardName: "auditfixes",
  match: (source) => source.startsWith("DataVo.Tests/AuditFixes/"),
});
