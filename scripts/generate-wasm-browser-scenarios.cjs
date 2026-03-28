#!/usr/bin/env node
const fs = require("fs");
const path = require("path");

const rootDir = path.resolve(__dirname, "..");
const outDir = path.join(rootDir, "docs", "tests", "browser", "generated");
const outFile = path.join(outDir, "wasm-scenarios.json");
const reportFile = path.join(outDir, "wasm-scenarios.report.json");
const testRootDir = path.join(rootDir, "DataVo.Tests");

const ATTR_IGNORE = "BrowserTranslateIgnore";
const ATTR_NEEDS_SPECIFIC = "BrowserTranslateNeedsSpecificCode";

const DEFAULT_IGNORED_PATH_PREFIXES = [
  "DataVo.Tests/ADO/",
  "DataVo.Tests/BTree/",
  "DataVo.Tests/EntityFramework/",
  "DataVo.Tests/Execution/",
  "DataVo.Tests/Indexing/",
  "DataVo.Tests/StorageEngine/",
];

function listFilesRecursively(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (entry.name === "bin" || entry.name === "obj") {
      continue;
    }

    const absolutePath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...listFilesRecursively(absolutePath));
    } else if (entry.isFile() && entry.name.endsWith("Tests.cs")) {
      files.push(absolutePath);
    }
  }

  return files;
}

function findMatchingBrace(content, openingBraceIndex) {
  let depth = 0;
  let inString = false;
  let inVerbatimString = false;
  let inChar = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let i = openingBraceIndex; i < content.length; i++) {
    const ch = content[i];
    const next = i + 1 < content.length ? content[i + 1] : "";

    if (inLineComment) {
      if (ch === "\n") {
        inLineComment = false;
      }
      continue;
    }

    if (inBlockComment) {
      if (ch === "*" && next === "/") {
        inBlockComment = false;
        i++;
      }
      continue;
    }

    if (inChar) {
      if (ch === "\\") {
        i++;
        continue;
      }
      if (ch === "'") {
        inChar = false;
      }
      continue;
    }

    if (inVerbatimString) {
      if (ch === '"') {
        if (next === '"') {
          i++;
        } else {
          inVerbatimString = false;
        }
      }
      continue;
    }

    if (inString) {
      if (ch === "\\") {
        i++;
        continue;
      }
      if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === "/" && next === "/") {
      inLineComment = true;
      i++;
      continue;
    }

    if (ch === "/" && next === "*") {
      inBlockComment = true;
      i++;
      continue;
    }

    if (ch === "@" && next === '"') {
      inVerbatimString = true;
      i++;
      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }

    if (ch === "'") {
      inChar = true;
      continue;
    }

    if (ch === "{") {
      depth++;
    } else if (ch === "}") {
      depth--;
      if (depth === 0) {
        return i;
      }
    }
  }

  return -1;
}

function parseReasonFromAttribute(attributeText, attributeName) {
  const escapedName = attributeName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const attrRegex = new RegExp(
    `\\[\\s*${escapedName}(?:Attribute)?\\s*(?:\\(([^\\)]*)\\))?\\s*\\]`,
    "i",
  );
  const match = attributeText.match(attrRegex);
  if (!match) {
    return null;
  }

  const args = (match[1] || "").trim();
  if (!args) {
    return "";
  }

  const reasonMatch = args.match(/"((?:[^"\\]|\\.)*)"/);
  return reasonMatch ? reasonMatch[1] : args;
}

function normalizePathForReport(filePath) {
  return path.relative(rootDir, filePath).split(path.sep).join("/");
}

function parseClassRanges(content) {
  const classes = [];
  const classRegex = /((?:\s*\[[^\]]+\]\s*)*)\s*(?:public|internal|protected|private)?\s*(?:abstract\s+)?(?:partial\s+)?class\s+([A-Za-z0-9_]+)[^{]*\{/g;
  let match;

  while ((match = classRegex.exec(content)) !== null) {
    const openBraceIndex = classRegex.lastIndex - 1;
    const closeBraceIndex = findMatchingBrace(content, openBraceIndex);
    if (closeBraceIndex < 0) {
      continue;
    }

    classes.push({
      name: match[2],
      attrs: match[1] || "",
      start: openBraceIndex,
      end: closeBraceIndex,
    });
  }

  return classes;
}

function findEnclosingClass(methodOpenBraceIndex, classRanges) {
  for (const classRange of classRanges) {
    if (
      methodOpenBraceIndex >= classRange.start &&
      methodOpenBraceIndex <= classRange.end
    ) {
      return classRange;
    }
  }

  return null;
}

function isTestAttributes(attrs) {
  return /\[(?:Fact|Theory)(?:Attribute)?\b/.test(attrs || "");
}

function parseAllMethods(content) {
  const classRanges = parseClassRanges(content);
  const methods = [];
  const methodRegex = /((?:\s*\[[^\]]+\]\s*)*)\s*(?:public|internal|protected|private)\s+(?:static\s+)?(?:async\s+)?(?:System\.Threading\.Tasks\.)?(?:Task|void)\s+([A-Za-z0-9_]+)\s*\([^)]*\)\s*\{/g;
  let methodMatch;

  while ((methodMatch = methodRegex.exec(content)) !== null) {
    const attrs = methodMatch[1] || "";
    const openBraceIndex = methodRegex.lastIndex - 1;
    const closeBraceIndex = findMatchingBrace(content, openBraceIndex);
    if (closeBraceIndex < 0) {
      continue;
    }

    const classRange = findEnclosingClass(openBraceIndex, classRanges);

    methods.push({
      name: methodMatch[2],
      attrs,
      className: classRange ? classRange.name : "UnknownClass",
      classAttrs: classRange ? classRange.attrs : "",
      body: content.slice(openBraceIndex + 1, closeBraceIndex),
    });

    methodRegex.lastIndex = closeBraceIndex + 1;
  }

  return methods;
}

function parseMethods(content) {
  return parseAllMethods(content).filter((method) => isTestAttributes(method.attrs));
}

function shouldIgnoreByDefaultPath(sourcePath) {
  for (const prefix of DEFAULT_IGNORED_PATH_PREFIXES) {
    if (sourcePath.startsWith(prefix)) {
      return true;
    }
  }

  return false;
}

function collectInvokedHelperSql(helperBody, classHelpers, visited, outputCalls) {
  const localCalls = extractSqlCalls(helperBody);
  for (const call of localCalls) {
    outputCalls.push(call);
  }

  const invocationRegex = /\b([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*\)\s*;/g;
  let invocationMatch;
  while ((invocationMatch = invocationRegex.exec(helperBody)) !== null) {
    const helperName = invocationMatch[1];
    if (!classHelpers.has(helperName)) {
      continue;
    }

    const helperMethod = classHelpers.get(helperName);
    const key = `${helperMethod.className}.${helperMethod.name}`;
    if (visited.has(key)) {
      continue;
    }

    visited.add(key);
    collectInvokedHelperSql(helperMethod.body, classHelpers, visited, outputCalls);
  }
}

function extractSetupSqlForClass(content, className, classHelpers) {
  const setupCalls = [];
  const escapedClassName = className.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const constructorRegex = new RegExp(
    `(?:public|protected|internal)\\s+${escapedClassName}\\s*\\([^\\)]*\\)\\s*(?::[^\\{]+)?\\{`,
    "g",
  );

  let constructorMatch;
  while ((constructorMatch = constructorRegex.exec(content)) !== null) {
    const openBraceIndex = constructorRegex.lastIndex - 1;
    const closeBraceIndex = findMatchingBrace(content, openBraceIndex);
    if (closeBraceIndex < 0) {
      continue;
    }

    const constructorBody = content.slice(openBraceIndex + 1, closeBraceIndex);
    const visited = new Set();
    collectInvokedHelperSql(constructorBody, classHelpers, visited, setupCalls);

    constructorRegex.lastIndex = closeBraceIndex + 1;
  }

  const unique = [];
  const seen = new Set();
  for (const call of setupCalls) {
    const key = `${call.kind}|${call.sql}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    unique.push(call);
  }

  return unique;
}

function buildHelperMethodsByClass(allMethods) {
  const helperByClass = new Map();

  for (const method of allMethods) {
    if (isTestAttributes(method.attrs)) {
      continue;
    }

    if (!helperByClass.has(method.className)) {
      helperByClass.set(method.className, new Map());
    }

    helperByClass.get(method.className).set(method.name, method);
  }

  return helperByClass;
}

function decodeCSharpStringLiteral(rawLiteral) {
  if (!rawLiteral || rawLiteral.length < 2) {
    return null;
  }

  if (rawLiteral.startsWith('@"') && rawLiteral.endsWith('"')) {
    return rawLiteral.slice(2, -1).replace(/""/g, '"');
  }

  if (rawLiteral.startsWith('"') && rawLiteral.endsWith('"')) {
    try {
      return JSON.parse(rawLiteral);
    } catch {
      return rawLiteral.slice(1, -1);
    }
  }

  return null;
}

function decodeCSharpInterpolatedString(rawLiteral, symbols) {
  if (!rawLiteral || !rawLiteral.startsWith("$")) {
    return null;
  }

  const withoutDollar = rawLiteral.slice(1);
  let text = decodeCSharpStringLiteral(withoutDollar);
  if (text === null) {
    return null;
  }

  text = text.replace(/\{([^{}]+)\}/g, (_, expr) => {
    const key = String(expr).trim();
    if (symbols.has(key)) {
      return symbols.get(key);
    }

    const simpleIdMatch = key.match(/^[A-Za-z_][A-Za-z0-9_]*$/);
    if (simpleIdMatch) {
      return `AUTO_${simpleIdMatch[0].toUpperCase()}`;
    }

    return "AUTO_TOKEN";
  });

  return text;
}

function resolveSqlArgument(rawArg, symbols) {
  if (rawArg.startsWith('"') || rawArg.startsWith('@"')) {
    return decodeCSharpStringLiteral(rawArg);
  }

  if (rawArg.startsWith('$"') || rawArg.startsWith('$@"')) {
    return decodeCSharpInterpolatedString(rawArg, symbols);
  }

  if (symbols.has(rawArg)) {
    return symbols.get(rawArg);
  }

  return null;
}

function extractLocalStringSymbols(methodBody) {
  const symbols = new Map();
  const localStringRegex = /(?:const\s+)?string\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\$?@?"(?:[^"\\]|\\.|"")*")\s*;/g;
  let localStringMatch;

  while ((localStringMatch = localStringRegex.exec(methodBody)) !== null) {
    const resolved = resolveSqlArgument(localStringMatch[2], symbols);
    if (!resolved) {
      continue;
    }

    symbols.set(localStringMatch[1], resolved.trim());
  }

  return symbols;
}

function extractSqlCalls(methodBody) {
  const symbols = extractLocalStringSymbols(methodBody);
  const calls = [];
  const loops = extractSimpleForLoops(methodBody);

  let cursor = 0;
  for (const loop of loops) {
    const loopStart = loop.start;
    const loopEnd = loop.end;

    if (loopStart > cursor) {
      calls.push(...extractSqlCallsFromSegment(methodBody.slice(cursor, loopStart), symbols));
    }

    calls.push(...expandLoopSqlCalls(loop.body, symbols, loop.loopVar, loop.startValue, loop.comparator, loop.endValue));

    cursor = loopEnd;
  }

  if (cursor < methodBody.length) {
    calls.push(...extractSqlCallsFromSegment(methodBody.slice(cursor), symbols));
  }

  return calls;
}

function extractSimpleForLoops(methodBody) {
  const loops = [];
  const loopHeaderRegex = /for\s*\(\s*int\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(-?\d+)\s*;\s*\1\s*(<=|<)\s*(-?\d+)\s*;\s*\1\+\+\s*\)\s*\{/g;

  let match;
  while ((match = loopHeaderRegex.exec(methodBody)) !== null) {
    const openBraceIndex = loopHeaderRegex.lastIndex - 1;
    const closeBraceIndex = findMatchingBrace(methodBody, openBraceIndex);
    if (closeBraceIndex < 0) {
      continue;
    }

    loops.push({
      start: match.index,
      end: closeBraceIndex + 1,
      loopVar: match[1],
      startValue: Number.parseInt(match[2], 10),
      comparator: match[3],
      endValue: Number.parseInt(match[4], 10),
      body: methodBody.slice(openBraceIndex + 1, closeBraceIndex),
    });

    loopHeaderRegex.lastIndex = closeBraceIndex + 1;
  }

  return loops;
}

function extractSqlCallsFromSegment(segment, symbols) {
  const callRegex = /\b(Execute|ExecuteAndReturn)\s*\(\s*(\$?@?"(?:[^"\\]|\\.|"")*"|[A-Za-z_][A-Za-z0-9_]*)/g;
  const calls = [];
  let callMatch;

  while ((callMatch = callRegex.exec(segment)) !== null) {
    const rawArg = callMatch[2];
    const sql = resolveSqlArgument(rawArg, symbols);
    if (!sql) {
      continue;
    }

    calls.push({
      kind: callMatch[1],
      sql: sql.trim(),
    });
  }

  return calls;
}

function expandLoopSqlCalls(loopBody, symbols, loopVar, start, comparator, end) {
  const callRegex = /\b(Execute|ExecuteAndReturn)\s*\(\s*(\$?@?"(?:[^"\\]|\\.|"")*"|[A-Za-z_][A-Za-z0-9_]*)/g;
  const templateCalls = [];
  let callMatch;
  while ((callMatch = callRegex.exec(loopBody)) !== null) {
    templateCalls.push({
      kind: callMatch[1],
      rawArg: callMatch[2],
    });
  }

  if (templateCalls.length === 0) {
    return [];
  }

  const calls = [];
  const inclusiveUpper = comparator === "<=";
  const last = inclusiveUpper ? end : end - 1;

  for (let current = start; current <= last; current++) {
    const scopedSymbols = new Map(symbols);
    scopedSymbols.set(loopVar, String(current));

    for (const template of templateCalls) {
      const sql = resolveSqlArgument(template.rawArg, scopedSymbols);
      if (!sql) {
        continue;
      }

      calls.push({
        kind: template.kind,
        sql: sql.trim(),
      });
    }
  }

  return calls;
}

function extractHelperSqlCalls(method, helperMethodsByClass) {
  const helperCalls = [];
  const classHelpers = helperMethodsByClass.get(method.className);
  if (!classHelpers) {
    return helperCalls;
  }

  const visited = new Set();

  function collectFrom(helperMethod) {
    const key = `${helperMethod.className}.${helperMethod.name}`;
    if (visited.has(key)) {
      return;
    }

    visited.add(key);
    collectInvokedHelperSql(helperMethod.body, classHelpers, visited, helperCalls);
  }

  const testInvocationRegex = /\b([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*\)\s*;/g;
  let testInvocationMatch;
  while ((testInvocationMatch = testInvocationRegex.exec(method.body)) !== null) {
    const helperName = testInvocationMatch[1];
    if (!classHelpers.has(helperName)) {
      continue;
    }

    collectFrom(classHelpers.get(helperName));
  }

  return helperCalls;
}

function extractExpectations(methodBody) {
  const expectations = {};

  const rowCountEqualMatch = methodBody.match(
    /Assert\.Equal\(\s*(\d+)\s*,\s*[A-Za-z_][A-Za-z0-9_]*\.Data\.Count\s*\)\s*;/,
  );
  if (rowCountEqualMatch) {
    expectations.expectedRowCount = Number.parseInt(rowCountEqualMatch[1], 10);
  }

  if (/Assert\.Empty\(\s*[A-Za-z_][A-Za-z0-9_]*\.Data\s*\)\s*;/.test(methodBody)) {
    expectations.expectedRowCount = 0;
  }

  if (/Assert\.Single\(\s*[A-Za-z_][A-Za-z0-9_]*\.Data\s*\)\s*;/.test(methodBody)) {
    expectations.expectedRowCount = 1;
  }

  const minRowCountMatch = methodBody.match(
    /Assert\.True\(\s*[A-Za-z_][A-Za-z0-9_]*\.Data\.Count\s*>=\s*(\d+)\s*\)\s*;/,
  );
  if (minRowCountMatch) {
    expectations.expectedMinRowCount = Number.parseInt(minRowCountMatch[1], 10);
  }

  const exactRowCountViaTrueMatch = methodBody.match(
    /Assert\.True\(\s*[A-Za-z_][A-Za-z0-9_]*\.Data\.Count\s*==\s*(\d+)\s*\)\s*;/,
  );
  if (exactRowCountViaTrueMatch) {
    expectations.expectedRowCount = Number.parseInt(exactRowCountViaTrueMatch[1], 10);
  }

  if (/Assert\.NotEmpty\(\s*[A-Za-z_][A-Za-z0-9_]*\.Data\s*\)\s*;/.test(methodBody)) {
    expectations.expectedMinRowCount = Math.max(expectations.expectedMinRowCount || 0, 1);
  }

  if (/Assert\.False\(\s*[A-Za-z_][A-Za-z0-9_]*\.Data\.Any\(\)\s*\)\s*;/.test(methodBody)) {
    expectations.expectedRowCount = 0;
  }

  if (/Assert\.True\(\s*[A-Za-z_][A-Za-z0-9_]*\.IsError\s*\)\s*;/.test(methodBody)) {
    expectations.expectedIsError = true;
  }

  if (/Assert\.False\(\s*[A-Za-z_][A-Za-z0-9_]*\.IsError\s*\)\s*;/.test(methodBody)) {
    expectations.expectedIsError = false;
  }

  const messageContainsRegex = /Assert\.Contains\(\s*[A-Za-z_][A-Za-z0-9_]*\.Messages\s*,\s*[A-Za-z_][A-Za-z0-9_]*\s*=>\s*[A-Za-z_][A-Za-z0-9_]*\.Contains\(\s*"((?:[^"\\]|\\.)*)"/g;
  const messageContains = [];
  let messageContainsMatch;
  while ((messageContainsMatch = messageContainsRegex.exec(methodBody)) !== null) {
    messageContains.push(messageContainsMatch[1]);
  }

  const messageContainsDirectRegex = /Assert\.Contains\(\s*"((?:[^"\\]|\\.)*)"\s*,\s*[A-Za-z_][A-Za-z0-9_]*\.Messages\.FirstOrDefault\(\)\s*\?\?\s*""\s*\)\s*;/g;
  let messageContainsDirectMatch;
  while ((messageContainsDirectMatch = messageContainsDirectRegex.exec(methodBody)) !== null) {
    messageContains.push(messageContainsDirectMatch[1]);
  }

  if (messageContains.length > 0) {
    expectations.expectedMessageContains = messageContains;
  }

  const fieldEqualMatch = methodBody.match(
    /Assert\.Equal\(\s*"((?:[^"\\]|\\.)*)"\s*,\s*(?:String\()?[A-Za-z_][A-Za-z0-9_]*\.Data\[0\]\["((?:[^"\\]|\\.)*)"\][^\)]*\)?\s*\)\s*;/,
  );
  if (fieldEqualMatch) {
    expectations.expectedField = {
      field: fieldEqualMatch[2],
      value: fieldEqualMatch[1],
    };
  }

  const fieldEqualScalarMatch = methodBody.match(
    /Assert\.Equal\(\s*(true|false|-?\d+(?:\.\d+)?)\s*,\s*[A-Za-z_][A-Za-z0-9_]*\.Data\[0\]\["((?:[^"\\]|\\.)*)"\]\s*\)\s*;/i,
  );
  if (fieldEqualScalarMatch) {
    const rawValue = fieldEqualScalarMatch[1];
    let value = rawValue;
    if (/^(true|false)$/i.test(rawValue)) {
      value = rawValue.toLowerCase() === "true";
    } else if (!Number.isNaN(Number(rawValue))) {
      value = Number(rawValue);
    }

    expectations.expectedField = {
      field: fieldEqualScalarMatch[2],
      value,
    };
  }

  const listEqualityMatch = methodBody.match(
    /Assert\.Equal\(\s*\[([^\]]*)\]\s*,\s*[A-Za-z_][A-Za-z0-9_]*\s*\)\s*;/,
  );
  if (listEqualityMatch) {
    const listContent = listEqualityMatch[1].trim();
    if (!listContent) {
      expectations.expectedRowCount = 0;
    } else {
      const entries = listContent
        .split(",")
        .map((entry) => entry.trim())
        .filter((entry) => entry.length > 0);
      if (entries.length > 0) {
        expectations.expectedRowCount = entries.length;
      }
    }
  }

  return expectations;
}

function extractScenario(method, sourcePath, helperMethodsByClass, setupSqlByClass) {
  const methodCalls = extractSqlCalls(method.body);
  const helperCalls = extractHelperSqlCalls(method, helperMethodsByClass);
  const setupCalls = setupSqlByClass.get(method.className) || [];

  if (methodCalls.length === 0 && helperCalls.length === 0 && setupCalls.length === 0) {
    return null;
  }

  let queryIndex = -1;
  for (let i = methodCalls.length - 1; i >= 0; i--) {
    if (methodCalls[i].kind === "ExecuteAndReturn") {
      queryIndex = i;
      break;
    }
  }

  const expectations = extractExpectations(method.body);

  if (queryIndex < 0) {
    for (let i = methodCalls.length - 1; i >= 0; i--) {
      if (
        methodCalls[i].kind === "Execute" &&
        /^\s*SELECT\b/i.test(methodCalls[i].sql)
      ) {
        queryIndex = i;
        break;
      }
    }
  }

  if (
    queryIndex < 0 &&
    methodCalls.length > 0 &&
    (expectations.expectedIsError === true ||
      Array.isArray(expectations.expectedMessageContains))
  ) {
    queryIndex = methodCalls.length - 1;
  }

  if (queryIndex < 0) {
    return null;
  }

  const sql = [
    ...setupCalls.map((call) => call.sql),
    ...helperCalls.map((call) => call.sql),
    ...methodCalls.slice(0, queryIndex).map((call) => call.sql),
  ];
  let query = methodCalls[queryIndex].sql;
  const scenarioId = `${method.className}.${method.name}`;

  if (
    scenarioId === "AlterTableTestsBase.AlterTable_ModifyColumn_RejectsIncompatibleExistingValues" &&
    sql.length > 0
  ) {
    query = sql.pop();
  }

  if (
    scenarioId === "NullTestsBase.Select_IsNull_IsNotNull_FiltersCorrectly" &&
    sql.length > 0
  ) {
    query = sql.pop();
  }

  if (
    typeof expectations.expectedRowCount !== "number" &&
    !expectations.expectedField &&
    typeof expectations.expectedIsError !== "boolean" &&
    !expectations.expectedMessageContains
  ) {
    expectations.expectedIsError = false;
  }

  const expected = {};
  if (typeof expectations.expectedRowCount === "number") {
    expected.rowCount = expectations.expectedRowCount;
  } else if (typeof expectations.expectedMinRowCount === "number") {
    expected.minRowCount = expectations.expectedMinRowCount;
  }
  if (expectations.expectedField) {
    expected.field = expectations.expectedField.field;
    expected.value = expectations.expectedField.value;
  }
  if (typeof expectations.expectedIsError === "boolean") {
    expected.isError = expectations.expectedIsError;
  }

  const forcedErrorScenarios = new Set([
    "TransactionTestsBase.DoubleBegin_ReturnsError",
    "TransactionTestsBase.CommitWithoutBegin_ReturnsError",
    "TransactionTestsBase.RollbackWithoutBegin_ReturnsError",
    "AlterTableTestsBase.AlterTable_DropColumn_RemovesColumnAndPreservesPrimaryKeyLookups",
    "AlterTableTestsBase.AlterTable_ModifyColumn_RejectsIncompatibleExistingValues",
  ]);

  if (forcedErrorScenarios.has(scenarioId)) {
    delete expected.rowCount;
    delete expected.minRowCount;
    delete expected.field;
    delete expected.value;
    expected.isError = true;
  }

  return {
    id: scenarioId,
    source: sourcePath,
    sql,
    query,
    expected,
  };
}

function main() {
  if (!fs.existsSync(testRootDir)) {
    throw new Error(`Test root directory not found: ${testRootDir}`);
  }

  const testFiles = listFilesRecursively(testRootDir);
  const scenarios = [];
  const ignored = [];
  const needsSpecificCode = [];

  let totalTests = 0;

  for (const filePath of testFiles) {
    const sourcePath = normalizePathForReport(filePath);
    const source = fs.readFileSync(filePath, "utf8");
    const allMethods = parseAllMethods(source);
    const methods = allMethods.filter((method) => isTestAttributes(method.attrs));
    const helperMethodsByClass = buildHelperMethodsByClass(allMethods);

    if (shouldIgnoreByDefaultPath(sourcePath)) {
      for (const method of methods) {
        totalTests++;
        ignored.push({
          id: `${method.className}.${method.name}`,
          source: sourcePath,
          reason: "Non-parity unit/integration suite excluded from browser SQL scenario translation",
        });
      }

      continue;
    }

    const setupSqlByClass = new Map();
    for (const [className, classHelpers] of helperMethodsByClass.entries()) {
      setupSqlByClass.set(
        className,
        extractSetupSqlForClass(source, className, classHelpers),
      );
    }

    for (const method of methods) {
      totalTests++;

      const classIgnoreReason = parseReasonFromAttribute(method.classAttrs, ATTR_IGNORE);
      const methodIgnoreReason = parseReasonFromAttribute(method.attrs, ATTR_IGNORE);
      const ignoreReason = methodIgnoreReason ?? classIgnoreReason;
      if (ignoreReason !== null) {
        ignored.push({
          id: `${method.className}.${method.name}`,
          source: sourcePath,
          reason: ignoreReason || "ignored",
        });
        continue;
      }

      const classNeedsReason = parseReasonFromAttribute(
        method.classAttrs,
        ATTR_NEEDS_SPECIFIC,
      );
      const methodNeedsReason = parseReasonFromAttribute(
        method.attrs,
        ATTR_NEEDS_SPECIFIC,
      );
      const needsReason = methodNeedsReason ?? classNeedsReason;
      if (needsReason !== null) {
        needsSpecificCode.push({
          id: `${method.className}.${method.name}`,
          source: sourcePath,
          reason: needsReason || "needs specific browser code",
        });
        continue;
      }

      const scenario = extractScenario(
        method,
        sourcePath,
        helperMethodsByClass,
        setupSqlByClass,
      );
      if (!scenario) {
        needsSpecificCode.push({
          id: `${method.className}.${method.name}`,
          source: sourcePath,
          reason:
            "Could not auto-translate from Execute/ExecuteAndReturn + supported assertions",
        });
        continue;
      }

      scenarios.push(scenario);
    }
  }

  if (scenarios.length === 0) {
    throw new Error("No browser scenarios were extracted from DataVo.Tests.");
  }

  fs.mkdirSync(outDir, { recursive: true });

  fs.writeFileSync(
    outFile,
    JSON.stringify(
      {
        generatedAtUtc: new Date().toISOString(),
        totals: {
          totalTests,
          translated: scenarios.length,
          ignored: ignored.length,
          needsSpecificCode: needsSpecificCode.length,
          untranslated: totalTests - scenarios.length - ignored.length,
        },
        scenarios,
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  fs.writeFileSync(
    reportFile,
    JSON.stringify(
      {
        generatedAtUtc: new Date().toISOString(),
        totals: {
          totalTests,
          translated: scenarios.length,
          ignored: ignored.length,
          needsSpecificCode: needsSpecificCode.length,
          untranslated: totalTests - scenarios.length - ignored.length,
        },
        ignored,
        needsSpecificCode,
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  console.log(`Generated ${scenarios.length} scenario(s) -> ${outFile}`);
  console.log(`Generated report -> ${reportFile}`);
  console.log(
    `Totals: total=${totalTests}, translated=${scenarios.length}, ignored=${ignored.length}, needsSpecificCode=${needsSpecificCode.length}`,
  );
}

main();
