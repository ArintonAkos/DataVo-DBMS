#!/usr/bin/env node
const fs = require("fs");
const path = require("path");

const rootDir = path.resolve(__dirname, "..");
const generatedDir = path.join(
  rootDir,
  "docs",
  "tests",
  "browser",
  "generated",
);

function readJson(fileName) {
  const filePath = path.join(generatedDir, fileName);
  if (!fs.existsSync(filePath)) {
    return null;
  }

  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function printTotals(title, totals) {
  if (!totals) {
    console.log(`${title}: missing`);
    return;
  }

  console.log(
    `${title}: total=${totals.total || 0}, passed=${totals.passed || 0}, failed=${totals.failed || 0}`,
  );
}

function main() {
  const generated = readJson("wasm-scenarios.execution-summary.json");
  const needs = readJson("wasm-needs-specific.execution-summary.json");
  const overall = readJson("wasm-parity.overall-summary.json");

  console.log("DataVo WASM Browser Parity Summary");
  console.log("---------------------------------");

  printTotals("Generated scenarios", generated?.totals);
  printTotals("Needs-specific", needs?.totals);
  printTotals("Overall", overall?.totals);

  if (overall?.suiteSummaries?.length) {
    console.log("\nSuites:");
    for (const suite of overall.suiteSummaries) {
      console.log(
        `- ${suite.suite}: total=${suite.totals.total}, passed=${suite.totals.passed}, failed=${suite.totals.failed}`,
      );
    }
  }
}

main();
