import { expect, test } from "@playwright/test";
import {
  SummaryRecord,
  loadNeedsSpecificItems,
  writeExecutionSummary,
} from "./wasm-parity";
import { runNeedsSpecificCase } from "./needs-specific";

type SuiteOptions = {
  title: string;
  shardName: string;
  match: (sourcePath: string) => boolean;
  assertNonEmpty?: boolean;
};

export function registerNeedsSpecificSuite(options: SuiteOptions): void {
  const items = loadNeedsSpecificItems().filter((item) =>
    options.match(item.source),
  );
  const summaryRecords: SummaryRecord[] = [];

  test.describe(options.title, () => {
    test.describe.configure({ mode: "serial" });

    test.beforeEach(async ({ page }) => {
      await page.goto("/");
    });

    test.afterAll(() => {
      writeExecutionSummary(
        `wasm-needs-specific.execution-summary.${options.shardName}.json`,
        `needs-specific:${options.shardName}`,
        summaryRecords,
      );
    });

    if (options.assertNonEmpty) {
      test("needsSpecific shard has cases", async () => {
        expect(items.length).toBeGreaterThan(0);
      });
    }

    for (let index = 0; index < items.length; index++) {
      const item = items[index];
      test(`needsSpecific: ${item.id}`, async ({ page }) => {
        const started = Date.now();
        try {
          const run = await runNeedsSpecificCase(page, item, index);
          summaryRecords.push({
            id: item.id,
            source: item.source,
            status: "passed",
            reason: `lane=${run.lane}`,
            durationMs: Date.now() - started,
          });

          expect(run.result.IsError).toBe(false);
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error);
          summaryRecords.push({
            id: item.id,
            source: item.source,
            status: "failed",
            reason,
            durationMs: Date.now() - started,
          });
          throw error;
        }
      });
    }
  });
}
