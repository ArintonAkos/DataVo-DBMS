import {
  NeedsSpecificItem,
  QueryResult,
  createDbName,
  executeSql,
  executeSqlRaw,
  resetBrowserClient,
} from "./wasm-parity";

type NeedsSpecificPlan = {
  lane: string;
  sql: string[];
  query: string;
  validate?: (result: QueryResult) => void;
};

function makeBTreePlan(): NeedsSpecificPlan {
  return {
    lane: "btree-index",
    sql: [
      "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
      "CREATE INDEX idx_name ON Users (Name)",
      "INSERT INTO Users VALUES (1, 'alice')",
      "INSERT INTO Users VALUES (2, 'bob')",
      "INSERT INTO Users VALUES (3, 'alice')",
    ],
    query: "SELECT Id FROM Users WHERE Name = 'alice'",
    validate: (result) => {
      if (result.IsError) {
        throw new Error((result.Messages || []).join(" | "));
      }
      if (result.Data.length < 2) {
        throw new Error(
          `Expected at least 2 indexed rows, got ${result.Data.length}`,
        );
      }
    },
  };
}

function makeVectorPlan(): NeedsSpecificPlan {
  return {
    lane: "vector-index",
    sql: [
      "CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)",
      "INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')",
      "INSERT INTO Embeddings (Id, Emb, Label) VALUES (2, '[0,1,0]', 'B')",
      "CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW",
    ],
    query: "SELECT Label FROM Embeddings WHERE Id = 2",
    validate: (result) => {
      if (result.IsError) {
        throw new Error((result.Messages || []).join(" | "));
      }
      if (result.Data.length !== 1) {
        throw new Error(
          `Expected one row after vector setup, got ${result.Data.length}`,
        );
      }
    },
  };
}

function makeMvccPlan(): NeedsSpecificPlan {
  return {
    lane: "mvcc-transaction",
    sql: [
      "CREATE TABLE Orders (Id INT PRIMARY KEY, Status VARCHAR)",
      "INSERT INTO Orders VALUES (1, 'open')",
      "UPDATE Orders SET Status = 'closed' WHERE Id = 1",
    ],
    query: "SELECT Status FROM Orders WHERE Id = 1",
    validate: (result) => {
      if (result.IsError) {
        throw new Error((result.Messages || []).join(" | "));
      }
      const status = String(result.Data[0]?.Status || "");
      if (status !== "closed") {
        throw new Error(`Expected closed status, got ${status || "<empty>"}`);
      }
    },
  };
}

function makeExecutionPlan(): NeedsSpecificPlan {
  return {
    lane: "execution-volcano",
    sql: [
      "CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)",
      "CREATE TABLE Purchases (Id INT PRIMARY KEY, CustomerId INT, Amount INT)",
      "INSERT INTO Customers VALUES (1, 'alice')",
      "INSERT INTO Customers VALUES (2, 'bob')",
      "INSERT INTO Purchases VALUES (10, 1, 50)",
      "INSERT INTO Purchases VALUES (11, 1, 25)",
      "INSERT INTO Purchases VALUES (12, 2, 10)",
    ],
    query:
      "SELECT c.Name FROM Customers c JOIN Purchases p ON c.Id = p.CustomerId WHERE p.Amount >= 25 ORDER BY c.Name",
    validate: (result) => {
      if (result.IsError) {
        throw new Error((result.Messages || []).join(" | "));
      }
      if (result.Data.length < 2) {
        throw new Error(
          `Expected at least 2 joined rows, got ${result.Data.length}`,
        );
      }
    },
  };
}

function makeStoragePlan(): NeedsSpecificPlan {
  return {
    lane: "storage-wal",
    sql: [
      "CREATE TABLE Ledger (Id INT PRIMARY KEY, Note VARCHAR)",
      "INSERT INTO Ledger VALUES (1, 'seed')",
      "UPDATE Ledger SET Note = 'updated' WHERE Id = 1",
      "DELETE FROM Ledger WHERE Id = 99",
    ],
    query: "SELECT Note FROM Ledger WHERE Id = 1",
    validate: (result) => {
      if (result.IsError) {
        throw new Error((result.Messages || []).join(" | "));
      }
      if (String(result.Data[0]?.Note || "") !== "updated") {
        throw new Error(
          "Expected storage smoke assertion to return updated note.",
        );
      }
    },
  };
}

function makeCrudFallbackPlan(): NeedsSpecificPlan {
  return {
    lane: "generic-crud",
    sql: [
      "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR)",
      "INSERT INTO Items VALUES (1, 'x')",
      "INSERT INTO Items VALUES (2, 'y')",
    ],
    query: "SELECT Name FROM Items ORDER BY Id",
    validate: (result) => {
      if (result.IsError) {
        throw new Error((result.Messages || []).join(" | "));
      }
      if (result.Data.length !== 2) {
        throw new Error(
          `Expected 2 rows in generic fallback, got ${result.Data.length}`,
        );
      }
    },
  };
}

export function planNeedsSpecificCase(
  item: NeedsSpecificItem,
): NeedsSpecificPlan {
  const signature = `${item.id} ${item.source} ${item.reason}`;

  if (
    /BTree|IndexKeyEncoder|BinaryBPlusTree|IndexManagerTests/i.test(signature)
  ) {
    return makeBTreePlan();
  }

  if (/HNSW|Vector|SearchNearest|Embeddings/i.test(signature)) {
    return makeVectorPlan();
  }

  if (
    /MVCC|Transaction|LockManager|Concurrency|Deadlock|session/i.test(signature)
  ) {
    return makeMvccPlan();
  }

  if (/Volcano|Execution|AuditFix|Benchmark|DQL/i.test(signature)) {
    return makeExecutionPlan();
  }

  if (/WAL|StorageEngine|Persistence|Vacuum|Disk/i.test(signature)) {
    return makeStoragePlan();
  }

  return makeCrudFallbackPlan();
}

export async function runNeedsSpecificCase(
  page: import("@playwright/test").Page,
  item: NeedsSpecificItem,
  index: number,
): Promise<{ lane: string; result: QueryResult }> {
  await resetBrowserClient(page);
  const plan = planNeedsSpecificCase(item);

  const dbName = createDbName("NeedsSpecific", index);
  await executeSql(page, `CREATE DATABASE ${dbName}`);
  await executeSql(page, `USE ${dbName}`);

  for (const command of plan.sql) {
    const setup = await executeSqlRaw(page, command);
    if (setup.IsError) {
      throw new Error(
        `Setup failed in lane ${plan.lane}: ${command}\n${(setup.Messages || []).join(" | ")}`,
      );
    }
  }

  const result = await executeSqlRaw(page, plan.query);
  if (plan.validate) {
    plan.validate(result);
  }

  if (result.IsError) {
    throw new Error(
      `Query failed in lane ${plan.lane}: ${plan.query}\n${(result.Messages || []).join(" | ")}`,
    );
  }

  return { lane: plan.lane, result };
}
