function createManualScenario(id, sourcePath, sql, query, expected) {
  return {
    id,
    source: sourcePath,
    sql,
    query,
    expected,
  };
}

function buildManualScenarioForUnitTest(method, sourcePath) {
  const scenarioId = `${method.className}.${method.name}`;

  switch (scenarioId) {
    case "BTreeNodeTests.InsertAndSearch_SingleKey_ReturnsValue":
      return createManualScenario(
        scenarioId,
        sourcePath,
        [
          "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
          "CREATE INDEX idx_name ON Users (Name)",
          "INSERT INTO Users VALUES (1, 'alice')",
        ],
        "SELECT Id FROM Users WHERE Name = 'alice'",
        { rowCount: 1, field: "Id", value: 1 },
      );

    case "BTreeNodeTests.Search_NonExistentKey_ReturnsEmpty":
      return createManualScenario(
        scenarioId,
        sourcePath,
        [
          "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
          "CREATE INDEX idx_name ON Users (Name)",
          "INSERT INTO Users VALUES (1, 'alice')",
        ],
        "SELECT Id FROM Users WHERE Name = 'bob'",
        { rowCount: 0 },
      );

    case "BTreeNodeTests.InsertDuplicateKey_AppendsValue":
      return createManualScenario(
        scenarioId,
        sourcePath,
        [
          "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
          "CREATE INDEX idx_name ON Users (Name)",
          "INSERT INTO Users VALUES (1, 'alice')",
          "INSERT INTO Users VALUES (2, 'alice')",
        ],
        "SELECT Id FROM Users WHERE Name = 'alice'",
        { rowCount: 2 },
      );

    case "BTreeNodeTests.InsertMultipleKeys_MaintainsSortedOrder":
      return createManualScenario(
        scenarioId,
        sourcePath,
        [
          "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
          "CREATE INDEX idx_name ON Users (Name)",
          "INSERT INTO Users VALUES (1, 'charlie')",
          "INSERT INTO Users VALUES (2, 'alice')",
          "INSERT INTO Users VALUES (3, 'bob')",
        ],
        "SELECT Name FROM Users ORDER BY Name",
        { rowCount: 3, field: "Name", value: "alice" },
      );

    case "BTreeNodeTests.ContainsKey_ReturnsCorrectly":
      return createManualScenario(
        scenarioId,
        sourcePath,
        [
          "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
          "CREATE INDEX idx_name ON Users (Name)",
          "INSERT INTO Users VALUES (1, 'alice')",
        ],
        "SELECT Id FROM Users WHERE Name = 'alice'",
        { rowCount: 1 },
      );

    case "BTreeNodeTests.CollectAll_ReturnsAllEntries":
      return createManualScenario(
        scenarioId,
        sourcePath,
        [
          "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
          "CREATE INDEX idx_name ON Users (Name)",
          "INSERT INTO Users VALUES (1, 'alice')",
          "INSERT INTO Users VALUES (2, 'bob')",
          "INSERT INTO Users VALUES (3, 'charlie')",
        ],
        "SELECT Name FROM Users ORDER BY Name",
        { rowCount: 3, field: "Name", value: "alice" },
      );

    case "IndexManagerTests.CreateIndex_WithEmptyData_CreatesIndex":
      if (sourcePath.endsWith("DataVo.Tests/BTree/IndexManagerTests.cs")) {
        return createManualScenario(
          scenarioId,
          sourcePath,
          [
            "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
            "CREATE INDEX idx_name ON Users (Name)",
          ],
          "SELECT * FROM Users WHERE Name = 'alice'",
          { rowCount: 0 },
        );
      }
      break;

    case "IndexManagerTests.CreateIndex_WithData_CanFilter":
      if (sourcePath.endsWith("DataVo.Tests/BTree/IndexManagerTests.cs")) {
        return createManualScenario(
          scenarioId,
          sourcePath,
          [
            "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
            "INSERT INTO Users VALUES (1, 'alice')",
            "INSERT INTO Users VALUES (2, 'bob')",
            "INSERT INTO Users VALUES (3, 'alice')",
            "CREATE INDEX idx_name ON Users (Name)",
          ],
          "SELECT Id FROM Users WHERE Name = 'alice'",
          { rowCount: 2 },
        );
      }
      break;

    case "IndexManagerTests.InsertIntoIndex_AddsEntryToExistingIndex":
      if (sourcePath.endsWith("DataVo.Tests/BTree/IndexManagerTests.cs")) {
        return createManualScenario(
          scenarioId,
          sourcePath,
          [
            "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
            "CREATE INDEX idx_name ON Users (Name)",
            "INSERT INTO Users VALUES (1, 'alice')",
          ],
          "SELECT Id FROM Users WHERE Name = 'alice'",
          { rowCount: 1, field: "Id", value: 1 },
        );
      }
      break;

    case "IndexManagerTests.IndexContainsRow_ReturnsTrueForExistingValue":
      if (sourcePath.endsWith("DataVo.Tests/BTree/IndexManagerTests.cs")) {
        return createManualScenario(
          scenarioId,
          sourcePath,
          [
            "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
            "CREATE INDEX idx_name ON Users (Name)",
            "INSERT INTO Users VALUES (1, 'alice')",
          ],
          "SELECT Id FROM Users WHERE Name = 'alice'",
          { rowCount: 1 },
        );
      }
      break;

    case "IndexManagerTests.DeleteFromIndex_RemovesRowIds":
      if (sourcePath.endsWith("DataVo.Tests/BTree/IndexManagerTests.cs")) {
        return createManualScenario(
          scenarioId,
          sourcePath,
          [
            "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
            "CREATE INDEX idx_name ON Users (Name)",
            "INSERT INTO Users VALUES (1, 'alice')",
            "INSERT INTO Users VALUES (2, 'alice')",
            "DELETE FROM Users WHERE Id = 1",
            "DELETE FROM Users WHERE Id = 2",
          ],
          "SELECT Id FROM Users WHERE Name = 'alice'",
          { rowCount: 0 },
        );
      }
      break;

    case "IndexManagerTests.DropIndex_RemovesIndex":
      if (sourcePath.endsWith("DataVo.Tests/BTree/IndexManagerTests.cs")) {
        return createManualScenario(
          scenarioId,
          sourcePath,
          [
            "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)",
            "CREATE INDEX idx_name ON Users (Name)",
            "INSERT INTO Users VALUES (1, 'alice')",
            "DROP INDEX idx_name ON Users",
          ],
          "SELECT Id FROM Users WHERE Name = 'alice'",
          { rowCount: 1, field: "Id", value: 1 },
        );
      }
      break;
  }

  return null;
}

module.exports = {
  buildManualScenarioForUnitTest,
};
