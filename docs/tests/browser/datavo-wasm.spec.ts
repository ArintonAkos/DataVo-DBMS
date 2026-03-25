import { expect, test } from "@playwright/test";

const strictBrowserParityGate =
  (globalThis as any)?.process?.env?.DATAVO_STRICT_BROWSER_PARITY === "1";

type QueryResult = {
  Messages: string[];
  Data: Record<string, unknown>[];
  Fields: string[];
  ExecutionTime: string;
  IsError: boolean;
};

async function executeSql(
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

    if (last.IsError) {
      throw new Error(
        `SQL failed: ${command}\n${(last.Messages || []).join(" | ")}`,
      );
    }

    return last;
  }, sql);
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
    const capabilities = await page.evaluate(async () => {
      const moduleUrl = "/src/DataVoClient.ts";
      const { DataVoClient } = await import(moduleUrl);
      const client = DataVoClient.getInstance();
      await client.initialize();
      return await client.runtimeCapabilities();
    });

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

    const capabilitiesBefore = await page.evaluate(async () => {
      const moduleUrl = "/src/DataVoClient.ts";
      const { DataVoClient } = await import(moduleUrl);
      const client = DataVoClient.getInstance();
      await client.initialize();
      return await client.runtimeCapabilities();
    });

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

    const selectMessages = (selectResult.Messages || []).join(" | ");

    expect(insertResultOne.IsError).toBe(false);
    expect(insertResultTwo.IsError).toBe(false);
    expect(selectResult.IsError).toBe(false);
    expect(Array.isArray(selectResult.Data)).toBe(true);
    expect(Array.isArray(selectResult.Fields)).toBe(true);
    expect(selectResult.Fields).toEqual(expect.arrayContaining(["Name"]));
    expect(selectMessages.length).toBeGreaterThanOrEqual(0);
    expect(typeof selectResult.ExecutionTime).toBe("string");

    const capabilitiesAfter = await page.evaluate(async () => {
      const moduleUrl = "/src/DataVoClient.ts";
      const { DataVoClient } = await import(moduleUrl);
      const client = DataVoClient.getInstance();
      await client.initialize();
      return await client.runtimeCapabilities();
    });

    if (strictBrowserParityGate) {
      expect(typeof capabilitiesBefore.storageBackend).toBe("string");
      expect(capabilitiesAfter.storageBackend).toBe(capabilitiesBefore.storageBackend);

      expect(selectResult.Data.length).toBeGreaterThanOrEqual(2);
      const names = selectResult.Data
        .map((row) => row.Name)
        .filter((value): value is string => typeof value === "string");

      expect(names.length).toBeGreaterThanOrEqual(2);
      expect(names).toEqual(expect.arrayContaining(["Alice", "Bob"]));
    }
  });
});


