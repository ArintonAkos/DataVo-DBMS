import { expect, test } from "@playwright/test";
import {
  loadGeneratedScenarioReport,
  writeOverallParitySummary,
} from "./support/wasm-parity";
import fs from "fs";
import path from "path";

test.describe("DataVo WASM needsSpecific summary", () => {
  test("builds merged needsSpecific and overall summaries", async () => {
    writeOverallParitySummary();

    const generatedDir = path.join(
      process.cwd(),
      "tests",
      "browser",
      "generated",
    );

    const needsSpecificSummaryPath = path.join(
      generatedDir,
      "wasm-needs-specific.execution-summary.json",
    );
    const overallSummaryPath = path.join(
      generatedDir,
      "wasm-parity.overall-summary.json",
    );

    expect(fs.existsSync(needsSpecificSummaryPath)).toBe(true);
    expect(fs.existsSync(overallSummaryPath)).toBe(true);

    const report = loadGeneratedScenarioReport();
    const needsSummary = JSON.parse(
      fs.readFileSync(needsSpecificSummaryPath, "utf8"),
    ) as { totals?: { total?: number } };

    expect(needsSummary.totals?.total || 0).toBe(
      report?.totals?.needsSpecificCode || 0,
    );
  });
});
