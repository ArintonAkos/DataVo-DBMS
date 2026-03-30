/// <reference types="node" />
import fs from "fs";
import path from "path";

export type QueryResult = {
  Messages: string[];
  Data: Record<string, unknown>[];
  Fields: string[];
  ExecutionTime: string;
  IsError: boolean;
};

export type GeneratedScenario = {
  id: string;
  source: string;
  sql: string[];
  query: string;
  expected: {
    rowCount?: number;
    minRowCount?: number;
    field?: string;
    value?: string | number | boolean;
    isError?: boolean;
    messageContains?: string[];
  };
};

export type NeedsSpecificItem = {
  id: string;
  source: string;
  reason: string;
};

export type GeneratedScenarioReport = {
  totals?: {
    totalTests?: number;
    translated?: number;
    ignored?: number;
    needsSpecificCode?: number;
    untranslated?: number;
  };
  ignored?: Array<{ id: string; source: string; reason: string }>;
  needsSpecificCode?: NeedsSpecificItem[];
};

export type SummaryRecord = {
  id: string;
  source: string;
  status: "passed" | "failed";
  reason?: string;
  durationMs?: number;
};

function generatedDir(): string {
  return path.join(process.cwd(), "tests", "browser", "generated");
}

export function createDbName(prefix: string, index: number): string {
  return `${prefix}_${Date.now()}_${index}`;
}

export function writeGeneratedArtifact(
  fileName: string,
  payload: unknown,
): void {
  const filePath = path.join(generatedDir(), fileName);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

export function loadGeneratedScenarios(): GeneratedScenario[] {
  const filePath = path.join(generatedDir(), "wasm-scenarios.json");
  if (!fs.existsSync(filePath)) {
    throw new Error(
      `Generated scenarios file is missing: ${filePath}. Run scripts/generate-wasm-browser-scenarios.sh first.`,
    );
  }

  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as {
    scenarios?: GeneratedScenario[];
  };

  return parsed.scenarios || [];
}

export function loadGeneratedScenarioReport(): GeneratedScenarioReport | null {
  const filePath = path.join(generatedDir(), "wasm-scenarios.report.json");
  if (!fs.existsSync(filePath)) {
    return null;
  }

  return JSON.parse(
    fs.readFileSync(filePath, "utf8"),
  ) as GeneratedScenarioReport;
}

export function normalizeGeneratedSql(
  command: string,
  sequence: number,
): string {
  const numericToken = String(sequence);
  return command
    .replace(/\bAUTO_I\b/g, numericToken)
    .replace(/\bAUTO_ID\b/g, numericToken)
    .replace(/\bAUTO_DUPLICATEID\b/g, numericToken);
}

export async function executeSqlRaw(
  page: import("@playwright/test").Page,
  sql: string,
): Promise<QueryResult> {
  return await page.evaluate(async (command) => {
    const moduleUrl = "/src/DataVoClient.ts";
    const { DataVoClient } = await import(moduleUrl);
    const client = DataVoClient.getInstance();
    await client.initialize();

    const results = await client.execute(command);
    const last = results[results.length - 1];
    if (!last) {
      throw new Error(`No QueryResult returned for SQL: ${command}`);
    }

    return last;
  }, sql);
}

export async function executeSql(
  page: import("@playwright/test").Page,
  sql: string,
): Promise<QueryResult> {
  const last = await executeSqlRaw(page, sql);
  if (last.IsError) {
    throw new Error(`SQL failed: ${sql}\n${(last.Messages || []).join(" | ")}`);
  }

  return last;
}

export async function resetBrowserClient(
  page: import("@playwright/test").Page,
): Promise<void> {
  await page.evaluate(async () => {
    const moduleUrl = "/src/DataVoClient.ts";
    const { DataVoClient } = await import(moduleUrl);
    const client = DataVoClient.getInstance();
    await client.initialize();
    await client.reset();
    await client.initialize();
  });
}

export async function getCapabilities(
  page: import("@playwright/test").Page,
): Promise<Record<string, any>> {
  return await page.evaluate(async () => {
    const moduleUrl = "/src/DataVoClient.ts";
    const { DataVoClient } = await import(moduleUrl);
    const client = DataVoClient.getInstance();
    await client.initialize();
    return await client.runtimeCapabilities();
  });
}

export function hasSuspiciousErrorMessage(messages: string[]): boolean {
  if (!Array.isArray(messages) || messages.length === 0) {
    return false;
  }

  return messages.some((message) =>
    /\berror\b|\bexception\b|\bfailed\b|\bcannot\b|\bcan only\b|does not exist|not registered|incompatible/i.test(
      message,
    ),
  );
}

export function isAuthorizationScenario(scenario: GeneratedScenario): boolean {
  return /AuthorizationTests\.cs$/i.test(scenario.source);
}

export function isIgnorableTransactionSetupError(
  command: string,
  message: string,
): boolean {
  if (/^\s*COMMIT\s*;?\s*$/i.test(command)) {
    return /Row\s+\d+\s+not\s+found\s+in\s+.*T_AUTO_TOKEN/i.test(message);
  }

  return false;
}

export function isRollbackToReleasedSavepointCase(
  scenario: GeneratedScenario,
  query: string,
  result: QueryResult,
): boolean {
  if (!/TransactionTests\.cs$/i.test(scenario.source)) {
    return false;
  }

  if (!/^\s*ROLLBACK\s+TO\s+SAVEPOINT\b/i.test(query)) {
    return false;
  }

  if (!result.IsError) {
    return false;
  }

  const messages = (result.Messages || []).join(" | ");
  return /savepoint\s+'.*'\s+does\s+not\s+exist/i.test(messages);
}

export function isRollbackToReleasedSavepointQuery(
  scenario: GeneratedScenario,
  query: string,
): boolean {
  return (
    /TransactionTests\.cs$/i.test(scenario.source) &&
    /^\s*ROLLBACK\s+TO\s+SAVEPOINT\b/i.test(query)
  );
}

export function writeRuntimeNeedsSpecificReport(
  items: NeedsSpecificItem[],
): void {
  writeGeneratedArtifact("wasm-scenarios.runtime-needs-specific.json", {
    generatedAtUtc: new Date().toISOString(),
    count: items.length,
    bySource: items.reduce<Record<string, number>>((acc, item) => {
      acc[item.source] = (acc[item.source] || 0) + 1;
      return acc;
    }, {}),
    items,
  });
}

export function writeExecutionSummary(
  fileName: string,
  suite: string,
  records: SummaryRecord[],
): void {
  const passed = records.filter((r) => r.status === "passed").length;
  const failed = records.filter((r) => r.status === "failed").length;

  writeGeneratedArtifact(fileName, {
    generatedAtUtc: new Date().toISOString(),
    suite,
    totals: {
      total: records.length,
      passed,
      failed,
    },
    bySource: records.reduce<
      Record<string, { passed: number; failed: number }>
    >((acc, rec) => {
      if (!acc[rec.source]) {
        acc[rec.source] = { passed: 0, failed: 0 };
      }

      acc[rec.source][rec.status]++;
      return acc;
    }, {}),
    records,
  });
}

export function writeOverallParitySummary(): void {
  const dir = generatedDir();
  const files = ["wasm-scenarios.execution-summary.json"];
  const shardFiles = fs
    .readdirSync(dir)
    .filter((name) =>
      /^wasm-needs-specific\.execution-summary(?:\.[A-Za-z0-9_-]+)?\.json$/.test(
        name,
      ),
    )
    .sort();
  files.push(...shardFiles);

  const suiteSummaries = files
    .map((fileName) => {
      const filePath = path.join(dir, fileName);
      if (!fs.existsSync(filePath)) {
        return null;
      }

      const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as {
        suite?: string;
        totals?: { total?: number; passed?: number; failed?: number };
      };

      return {
        fileName,
        suite: parsed.suite || fileName,
        totals: {
          total: parsed.totals?.total || 0,
          passed: parsed.totals?.passed || 0,
          failed: parsed.totals?.failed || 0,
        },
      };
    })
    .filter(
      (
        x,
      ): x is {
        fileName: string;
        suite: string;
        totals: { total: number; passed: number; failed: number };
      } => x !== null,
    );

  const totals = suiteSummaries.reduce(
    (acc, item) => {
      acc.total += item.totals.total;
      acc.passed += item.totals.passed;
      acc.failed += item.totals.failed;
      return acc;
    },
    { total: 0, passed: 0, failed: 0 },
  );

  const needsSpecificRecords = shardFiles
    .map((fileName) => {
      const filePath = path.join(dir, fileName);
      const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as {
        records?: SummaryRecord[];
      };
      return parsed.records || [];
    })
    .flat();

  if (needsSpecificRecords.length > 0) {
    writeGeneratedArtifact("wasm-needs-specific.execution-summary.json", {
      generatedAtUtc: new Date().toISOString(),
      suite: "needs-specific",
      totals: {
        total: needsSpecificRecords.length,
        passed: needsSpecificRecords.filter((x) => x.status === "passed")
          .length,
        failed: needsSpecificRecords.filter((x) => x.status === "failed")
          .length,
      },
      bySource: needsSpecificRecords.reduce<
        Record<string, { passed: number; failed: number }>
      >((acc, rec) => {
        if (!acc[rec.source]) {
          acc[rec.source] = { passed: 0, failed: 0 };
        }
        acc[rec.source][rec.status]++;
        return acc;
      }, {}),
      records: needsSpecificRecords,
      shardFiles,
    });
  }

  writeGeneratedArtifact("wasm-parity.overall-summary.json", {
    generatedAtUtc: new Date().toISOString(),
    totals,
    suiteSummaries,
  });
}

export function loadNeedsSpecificItems(): NeedsSpecificItem[] {
  const report = loadGeneratedScenarioReport();
  return report?.needsSpecificCode || [];
}
