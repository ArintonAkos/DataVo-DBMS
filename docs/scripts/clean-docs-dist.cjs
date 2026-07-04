#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const docsRoot = path.resolve(__dirname, "..");
const distRoot = path.join(docsRoot, ".vitepress", "dist");

if (fs.existsSync(distRoot)) {
  fs.rmSync(distRoot, { recursive: true, force: true });
}
