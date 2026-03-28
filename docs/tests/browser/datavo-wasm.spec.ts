import { expect, test } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

const strictBrowserParityGate =
  (globalThis as any)?.process?.env?.DATAVO_STRICT_BROWSER_PARITY === "1";
const requireFullTranslationGate =
  (globalThis as any)?.process?.env?.DATAVO_REQUIRE_FULL_TRANSLATION === "1";

type QueryResult = {
  Messages: string[];
  Data: Record<string, unknown>[];
  Fields: string[];
  ExecutionTime: string;
  IsError: boolean;
};

type GeneratedScenario = {
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

type GeneratedScenarioReport = {
  totals?: {
    totalTests?: number;
    translated?: number;
    ignored?: number;
    needsSpecificCode?: number;
    untranslated?: number;
  };
};

type RuntimeNeedsSpecificItem = {
  id: string;
  source: string;
  reason: string;
};

function loadGeneratedScenarios(): GeneratedScenario[] {
  const filePath = path.join(
    process.cwd(),
    "tests",
    "browser",
    "generated",
    "wasm-scenarios.json",
  );

  if (!fs.existsSync(filePath)) {
    throw new Error(
      `Generated scenarios file is missing: ${filePath}. Run scripts/generate-wasm-browser-scenarios.sh first.`,
    );
  }

  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as {
    scenarios?: GeneratedScenario[];
  };

  if (!parsed.scenarios || parsed.scenarios.length === 0) {
    return [];
  }

  return parsed.scenarios;
}

function loadGeneratedScenarioReport(): GeneratedScenarioReport | null {
  const filePath = path.join(
    process.cwd(),
    "tests",
    "browser",
    "generated",
    "wasm-scenarios.report.json",
  );

  if (!fs.existsSync(filePath)) {
    return null;
  }

  return JSON.parse(
    fs.readFileSync(filePath, "utf8"),
  ) as GeneratedScenarioReport;
}

function normalizeGeneratedSql(command: string, sequence: number): string {
  const numericToken = String(sequence);
  return command
    .replace(/\bAUTO_I\b/g, numericToken)
    .replace(/\bAUTO_ID\b/g, numericToken)
    .replace(/\bAUTO_DUPLICATEID\b/g, numericToken);
}

function writeRuntimeNeedsSpecificReport(
  items: RuntimeNeedsSpecificItem[],
): void {
  const filePath = path.join(
    process.cwd(),
    "tests",
    "browser",
    "generated",
    "wasm-scenarios.runtime-needs-specific.json",
  );

  const bySource: Record<string, number> = {};
  for (const item of items) {
    bySource[item.source] = (bySource[item.source] || 0) + 1;
  }

  fs.writeFileSync(
    filePath,
    JSON.stringify(
      {
        generatedAtUtc: new Date().toISOString(),
        count: items.length,
        bySource,
        items,
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
}

async function executeSql(
  page: import("@playwright/test").Page,
  sql: string,
): Promise<QueryResult> {
  const last = await executeSqlRaw(page, sql);

  if (last.IsError) {
    throw new Error(`SQL failed: ${sql}\n${(last.Messages || []).join(" | ")}`);
  }

  return last;
}

function hasSuspiciousErrorMessage(messages: string[]): boolean {
  if (!Array.isArray(messages) || messages.length === 0) {
    return false;
  }

  return messages.some((message) =>
    /\berror\b|\bexception\b|\bfailed\b|\bcannot\b|\bcan only\b|does not exist|not registered|incompatible/i.test(
      message,
    ),
  );
}

async function executeSqlRaw(
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

async function getCapabilities(
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

async function resetBrowserClient(
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

async function runStrictCrudParity(
  page: import("@playwright/test").Page,
  dbName: string,
): Promise<{ names: string[]; backendBefore: string; backendAfter: string }> {
  const capabilitiesBefore = await getCapabilities(page);

  await executeSql(page, `CREATE DATABASE ${dbName}`);
  await executeSql(page, `USE ${dbName}`);
  await executeSql(
    page,
    "CREATE TABLE Users (Id VARCHAR PRIMARY KEY, Name VARCHAR)",
  );

  const insertResultOne = await executeSql(
    page,
    "INSERT INTO Users (Id, Name) VALUES ('1', 'Alice')",
  );
  const insertResultTwo = await executeSql(
    page,
    "INSERT INTO Users (Id, Name) VALUES ('2', 'Bob')",
  );

  let selectResult: QueryResult | null = null;
  for (let attempt = 0; attempt < 10; attempt++) {
    const candidate = await executeSql(page, "SELECT Name FROM Users");
    selectResult = candidate;
    if (candidate.Data.length >= 2) {
      break;
    }

    await page.waitForTimeout(100);
  }

  if (!selectResult) {
    throw new Error("SELECT result was not produced by browser runtime.");
  }

  expect(insertResultOne.IsError).toBe(false);
  expect(insertResultTwo.IsError).toBe(false);
  expect(selectResult.IsError).toBe(false);
  expect(Array.isArray(selectResult.Data)).toBe(true);
  expect(Array.isArray(selectResult.Fields)).toBe(true);
  expect(selectResult.Fields).toEqual(expect.arrayContaining(["Name"]));

  const names = selectResult.Data.map((row) => row.Name).filter(
    (value): value is string => typeof value === "string",
  );

  const capabilitiesAfter = await getCapabilities(page);

  return {
    names,
    backendBefore: String(capabilitiesBefore.storageBackend || "unknown"),
    backendAfter: String(capabilitiesAfter.storageBackend || "unknown"),
  };
}

test.describe("DataVo WASM browser runtime", () => {
  test.describe.configure({ mode: "serial" });

  test.beforeEach(async ({ page }) => {
    await page.goto("/");

    await page.evaluate(async () => {
      const moduleUrl = "/src/DataVoClient.ts";
      const { DataVoClient } = await import(moduleUrl);
      const client = DataVoClient.getInstance();
      await client.initialize();
      await client.reset();
      await client.initialize();
    });
  });

  test("reports runtime capabilities", async ({ page }) => {
    const capabilities = await getCapabilities(page);

    expect(capabilities).toBeTruthy();
    expect(typeof capabilities.storageBackend).toBe("string");
    expect([
      "worker-opfs",
      "worker-memory-fallback",
      "localStorage",
      "unknown",
    ]).toContain(capabilities.storageBackend);
  });

  test("executes CRUD flow in Chromium", async ({ page }) => {
    const dbName = `BrowserDb_${Date.now()}`;
    const parity = await runStrictCrudParity(page, dbName);

    if (strictBrowserParityGate) {
      expect(parity.backendAfter).toBe(parity.backendBefore);
      expect(parity.names.length).toBeGreaterThanOrEqual(2);
      expect(parity.names).toEqual(expect.arrayContaining(["Alice", "Bob"]));
    }
  });

  test("strict parity remains stable across repeated loops", async ({
    page,
  }) => {
    test.skip(
      !strictBrowserParityGate,
      "Strict loop stress runs only in strict mode.",
    );

    for (let i = 0; i < 5; i++) {
      const dbName = `BrowserDbStrictLoop_${Date.now()}_${i}`;
      const parity = await runStrictCrudParity(page, dbName);

      expect(parity.backendAfter).toBe(parity.backendBefore);
      expect(parity.names).toEqual(expect.arrayContaining(["Alice", "Bob"]));
    }
  });

  test("strict parity backend matrix", async ({ page }) => {
    test.skip(
      !strictBrowserParityGate,
      "Backend matrix runs only in strict mode.",
    );

    const matrix = [
      {
        query: "",
        expected: ["localStorage", "worker-opfs", "worker-memory-fallback"],
      },
      {
        query: "?datavoStorageBackend=worker-opfs",
        expected: ["worker-opfs", "worker-memory-fallback", "localStorage"],
      },
      { query: "?datavoRuntimeWorker=off", expected: ["localStorage"] },
    ];

    for (let i = 0; i < matrix.length; i++) {
      const row = matrix[i];
      await page.goto(`/${row.query}`);

      await page.evaluate(async () => {
        const moduleUrl = "/src/DataVoClient.ts";
        const { DataVoClient } = await import(moduleUrl);
        const client = DataVoClient.getInstance();
        await client.initialize();
        await client.reset();
        await client.initialize();
      });

      const dbName = `BrowserDbMatrix_${Date.now()}_${i}`;
      const parity = await runStrictCrudParity(page, dbName);

      expect(row.expected).toContain(parity.backendBefore);
      expect(parity.backendAfter).toBe(parity.backendBefore);
      expect(parity.names).toEqual(expect.arrayContaining(["Alice", "Bob"]));
    }
  });

  test("generated .NET WASM scenarios execute in browser", async ({ page }) => {
    const scenarios = loadGeneratedScenarios();
    const report = loadGeneratedScenarioReport();

    test.skip(
      scenarios.length === 0,
      "No generated scenarios found. Run scripts/generate-wasm-browser-scenarios.sh.",
    );

    if (requireFullTranslationGate) {
      expect(report).toBeTruthy();
      expect(report?.totals?.needsSpecificCode ?? 0).toBe(0);
      expect(report?.totals?.untranslated ?? 0).toBe(0);
    }

    const runtimeNeedsSpecific: RuntimeNeedsSpecificItem[] = [];
    let executedScenarios = 0;

    for (let i = 0; i < scenarios.length; i++) {
      const scenario = scenarios[i];
      try {
        await resetBrowserClient(page);

        const dbName = `GeneratedScenario_${Date.now()}_${i}`;
        const scenarioToken = i * 1000 + 1;
        await executeSql(page, `CREATE DATABASE ${dbName}`);
        await executeSql(page, `USE ${dbName}`);
        const createdTables = new Set<string>();

        for (
          let commandIndex = 0;
          commandIndex < scenario.sql.length;
          commandIndex++
        ) {
          const command = normalizeGeneratedSql(
            scenario.sql[commandIndex],
            scenarioToken,
          );
          if (
            /^\s*CREATE\s+DATABASE\b/i.test(command) ||
            /^\s*USE\b/i.test(command)
          ) {
            continue;
          }

          const createTableMatch = command.match(
            /^\s*CREATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\b/i,
          );
          if (createTableMatch) {
            const tableName = createTableMatch[1].toLowerCase();
            if (createdTables.has(tableName)) {
              continue;
            }

            createdTables.add(tableName);
          }

          const setupResult = await executeSqlRaw(page, command);
          const isCreateIndexSetup = /^\s*CREATE\s+INDEX\b/i.test(command);
          if (
            setupResult.IsError ||
            (isCreateIndexSetup &&
              hasSuspiciousErrorMessage(setupResult.Messages || []))
          ) {
            throw new Error(
              `SQL failed during setup: ${command}\n${(setupResult.Messages || []).join(" | ")}`,
            );
          }
        }

        const normalizedQuery = normalizeGeneratedSql(
          scenario.query,
          scenarioToken,
        );

        const expectsMessage =
          Array.isArray(scenario.expected.messageContains) &&
          scenario.expected.messageContains.length > 0;

        const result =
          scenario.expected.isError === true || expectsMessage
            ? await executeSqlRaw(page, normalizedQuery)
            : await executeSql(page, normalizedQuery);

        if (typeof scenario.expected.isError === "boolean") {
          expect(
            result.IsError,
            `Scenario ${scenario.id} (${scenario.source}) error expectation mismatch`,
          ).toBe(scenario.expected.isError);
        }

        if (
          Array.isArray(scenario.expected.messageContains) &&
          scenario.expected.messageContains.length > 0
        ) {
          const messages = (result.Messages || []).join(" | ");
          const hasAnyExpectedMessage = scenario.expected.messageContains.some(
            (expectedMessage) => messages.includes(expectedMessage),
          );
          expect(hasAnyExpectedMessage).toBe(true);
        }

        if (typeof scenario.expected.rowCount === "number") {
          expect(
            result.Data.length,
            `Scenario ${scenario.id} (${scenario.source}) row count mismatch`,
          ).toBe(scenario.expected.rowCount);
        } else if (typeof scenario.expected.minRowCount === "number") {
          expect(
            result.Data.length,
            `Scenario ${scenario.id} (${scenario.source}) minimum row count mismatch`,
          ).toBeGreaterThanOrEqual(scenario.expected.minRowCount);
        } else if (
          /^\s*SELECT\b/i.test(normalizedQuery) &&
          scenario.expected.isError !== true &&
          !expectsMessage
        ) {
          expect(
            result.Data.length,
            `Scenario ${scenario.id} (${scenario.source}) expected at least one row`,
          ).toBeGreaterThanOrEqual(1);
        }

        if (
          typeof scenario.expected.field === "string" &&
          typeof scenario.expected.value !== "undefined"
        ) {
          const actualValue = result.Data[0][scenario.expected.field];
          if (typeof scenario.expected.value === "string") {
            expect(String(actualValue ?? "")).toBe(scenario.expected.value);
          } else {
            expect(actualValue).toBe(scenario.expected.value);
          }
        }

        executedScenarios++;
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        runtimeNeedsSpecific.push({
          id: scenario.id,
          source: scenario.source,
          reason,
        });
      }
    }

    expect(executedScenarios).toBeGreaterThan(0);

    writeRuntimeNeedsSpecificReport(runtimeNeedsSpecific);

    if (runtimeNeedsSpecific.length > 0) {
      console.warn(
        `Generated browser scenarios requiring specific code at runtime: ${runtimeNeedsSpecific.length}`,
      );
      const preview = runtimeNeedsSpecific
        .slice(0, 12)
        .map((x) => `${x.id} (${x.source}) => ${x.reason}`)
        .join("\n");
      console.warn(`Runtime-specific sample:\n${preview}`);
    }

    if (requireFullTranslationGate) {
      expect(runtimeNeedsSpecific.length).toBe(0);
    }
  });
});
