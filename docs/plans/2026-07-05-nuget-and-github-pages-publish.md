# NuGet Publish + GitHub Pages Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the four DataVo packages to nuget.org (with a tag-triggered release workflow for future versions) and deploy the VitePress docs to GitHub Pages at the project subpath.

**Architecture:** Packaging is already configured in `Directory.Build.props` and each generator csproj — this plan verifies/re-packs, adds two GitHub Actions workflows (`publish-nuget.yml`, `deploy-docs.yml`), and makes the VitePress config Pages-subpath-aware. Steps the owner alone can do (nuget push with a secret key, adding the `NUGET_API_KEY` secret, enabling Pages) are called out explicitly and are NOT Claude steps.

**Tech Stack:** .NET 10 SDK (`dotnet pack`/`nuget push`), GitHub Actions (`setup-dotnet`, `setup-node`, `configure-pages`/`upload-pages-artifact`/`deploy-pages`), VitePress 1.6 (Node 20).

## Global Constraints

- Package version: `0.1.0-preview.1` (from `Directory.Build.props` / generator csproj) — lists as prerelease.
- Publishable packages (all four): `DataVo.Core`, `DataVo.Data`, `DataVo.EntityFrameworkCore`, `DataVo.Generators`.
- Package output path: `artifacts/packages/` (gitignored — built packages are never committed).
- NuGet feed: `https://api.nuget.org/v3/index.json`; always push with `--skip-duplicate`.
- Pages base path: `base: "/DataVo-DBMS/"`; live URL `https://arintonakos.github.io/DataVo-DBMS/`.
- Docs build entry point: `npm run docs:build` (runs `docs:ai` → `clean-docs-dist.cjs` → `vitepress build`), output `docs/.vitepress/dist`.
- Repo: `github.com/ArintonAkos/DataVo-DBMS`, default branch `master`. No `global.json` — CI pins `dotnet-version: 10.0.x`, Node 20.
- `favicon.svg` was intentionally deleted (commit `a8448c0`); do not reference it.

---

### Task 1: Verify and re-pack the NuGet packages locally

**Files:**
- Modify: none (build outputs only; `artifacts/packages/` is gitignored)
- Reference: `Directory.Build.props`, `DataVo.Generators/DataVo.Generators.csproj`

**Interfaces:**
- Consumes: nothing.
- Produces: verified `artifacts/packages/*.nupkg` (+ `.snupkg`) for all four packages, ready for the owner's push. No commit.

- [ ] **Step 1: Clean-pack all packable projects in Release**

Run from repo root:
```bash
rm -f artifacts/packages/*.nupkg artifacts/packages/*.snupkg
dotnet pack DataVo.sln -c Release -o artifacts/packages
```
Expected: `Build succeeded`, `0 Error(s)`, and lines like `Successfully created package '.../artifacts/packages/DataVo.Core.0.1.0-preview.1.nupkg'` for each of the four packages.

- [ ] **Step 2: Confirm all four packages emitted**

Run:
```bash
ls -1 artifacts/packages/*.nupkg
```
Expected exactly these four:
```
artifacts/packages/DataVo.Core.0.1.0-preview.1.nupkg
artifacts/packages/DataVo.Data.0.1.0-preview.1.nupkg
artifacts/packages/DataVo.EntityFrameworkCore.0.1.0-preview.1.nupkg
artifacts/packages/DataVo.Generators.0.1.0-preview.1.nupkg
```

- [ ] **Step 3: Inspect Core package contents (README + license + lib)**

Run:
```bash
unzip -l artifacts/packages/DataVo.Core.0.1.0-preview.1.nupkg
```
Expected to see: `README.md`, `DataVo.Core.nuspec`, and `lib/<tfm>/DataVo.Core.dll`. Open the nuspec to confirm license + readme wiring:
```bash
unzip -p artifacts/packages/DataVo.Core.0.1.0-preview.1.nupkg 'DataVo.Core.nuspec' | grep -E 'license|readme|<version>'
```
Expected: `<license type="expression">MIT</license>`, `<readme>README.md</readme>`, `<version>0.1.0-preview.1</version>`.

- [ ] **Step 4: Inspect the Generators analyzer layout**

Run:
```bash
unzip -l artifacts/packages/DataVo.Generators.0.1.0-preview.1.nupkg
```
Expected: `DataVo.Generators.dll` under `analyzers/dotnet/cs/` (and README), with NO `lib/` build output (the csproj sets `IncludeBuildOutput=false`).

- [ ] **Step 5: Verify no junk in packages**

Run:
```bash
for p in artifacts/packages/*.nupkg; do echo "== $p =="; unzip -l "$p" | grep -iE 'test|\.pdb|\.user|appsettings' || echo "  (clean)"; done
```
Expected: `(clean)` for each (no test assemblies, no stray `.pdb`/`.user`/config files). `.pdb` inside the `.snupkg` symbol packages is expected and fine — this check is over the `.nupkg` files only.

- [ ] **Step 6: Print the owner push command (no commit for this task)**

This task ends WITHOUT a commit (packages are gitignored build artifacts). Surface the exact command the OWNER runs (Claude cannot — it needs the secret key):
```bash
# OWNER runs this (via the `!` prefix or their own terminal):
dotnet nuget push "artifacts/packages/*.nupkg" \
  --api-key <YOUR_NUGET_API_KEY> \
  --source https://api.nuget.org/v3/index.json \
  --skip-duplicate
```
Note for the owner: create the key at nuget.org (scope **Push**, glob `DataVo.*`); `.snupkg` symbols upload automatically; `0.1.0-preview.1` shows only when "include prerelease" is enabled.

---

### Task 2: Add the NuGet release workflow

**Files:**
- Create: `.github/workflows/publish-nuget.yml`

**Interfaces:**
- Consumes: repo secret `NUGET_API_KEY` (owner adds it — see final checklist).
- Produces: a workflow that packs with the tag-derived version and pushes all four packages on a `v*.*.*` tag.

- [ ] **Step 1: Create the workflow file**

Create `.github/workflows/publish-nuget.yml` with exactly:
```yaml
name: Publish NuGet

on:
  push:
    tags:
      - "v*.*.*"
  workflow_dispatch:

permissions:
  contents: read

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: 10.0.x

      - name: Determine version from tag
        id: version
        run: |
          if [ "${GITHUB_REF_TYPE}" = "tag" ]; then
            echo "value=${GITHUB_REF_NAME#v}" >> "$GITHUB_OUTPUT"
          else
            echo "value=" >> "$GITHUB_OUTPUT"
          fi

      - name: Pack
        run: |
          if [ -n "${{ steps.version.outputs.value }}" ]; then
            dotnet pack DataVo.sln -c Release -p:Version=${{ steps.version.outputs.value }} -o artifacts/packages
          else
            dotnet pack DataVo.sln -c Release -o artifacts/packages
          fi

      - name: Push to NuGet
        run: >-
          dotnet nuget push "artifacts/packages/*.nupkg"
          --api-key "${{ secrets.NUGET_API_KEY }}"
          --source https://api.nuget.org/v3/index.json
          --skip-duplicate
```

- [ ] **Step 2: Validate the YAML parses**

Run:
```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/publish-nuget.yml')); print('YAML OK')" 2>/dev/null || echo "pyyaml unavailable — verify structure by eye"
```
Expected: `YAML OK` (or the fallback message; if fallback, confirm the file matches Step 1 verbatim — indentation is the usual failure).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/publish-nuget.yml
git commit -m "ci: add NuGet release workflow (publish on v*.*.* tag)"
```

---

### Task 3: Make VitePress config Pages-subpath-aware

**Files:**
- Modify: `docs/.vitepress/config.mts` (add `base`; remove dangling favicon/logo references)

**Interfaces:**
- Consumes: nothing.
- Produces: a `dist` whose asset URLs are prefixed with `/DataVo-DBMS/`, ready for `upload-pages-artifact` in Task 4.

- [ ] **Step 1: Add the `base` option**

In `docs/.vitepress/config.mts`, inside the `defineConfig({ ... })` object, add `base` right after the `title` line. Change:
```ts
    title: "DataVo",
    description:
```
to:
```ts
    title: "DataVo",
    base: "/DataVo-DBMS/",
    description:
```

- [ ] **Step 2: Remove the dangling favicon `head` link**

`favicon.svg` no longer exists (deleted in `a8448c0`). Remove the whole `head` block (lines currently 26–28):
```ts
    head: [
      ["link", { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" }],
    ],
```
Delete those three lines entirely.

- [ ] **Step 3: Remove the dangling `logo` reference**

In `themeConfig`, remove the line:
```ts
      logo: "/favicon.svg",
```
Leave `siteTitle: "DataVo",` — the navbar keeps the text title.

- [ ] **Step 4: Build the docs locally and verify base-prefixed assets**

Run:
```bash
cd docs && npm run docs:build && cd ..
```
Expected: `build complete` with no errors, producing `docs/.vitepress/dist`. Then confirm assets are base-prefixed:
```bash
grep -o '/DataVo-DBMS/assets/[^"]*' docs/.vitepress/dist/index.html | head -3
```
Expected: at least one `/DataVo-DBMS/assets/...` path (proves `base` took effect). Also confirm no dangling favicon ref remains:
```bash
grep -c 'favicon.svg' docs/.vitepress/dist/index.html || true
```
Expected: `0`.

- [ ] **Step 5: Commit**

```bash
git add docs/.vitepress/config.mts
git commit -m "docs: set Pages base path and drop deleted favicon references"
```

---

### Task 4: Add the GitHub Pages deploy workflow

**Files:**
- Create: `.github/workflows/deploy-docs.yml`

**Interfaces:**
- Consumes: `base`-aware config from Task 3; `docs/package-lock.json` for `npm ci`.
- Produces: a workflow that builds `docs/.vitepress/dist` and deploys it to GitHub Pages on pushes to `master` touching `docs/**`.

- [ ] **Step 1: Create the workflow file**

Create `.github/workflows/deploy-docs.yml` with exactly:
```yaml
name: Deploy Docs

on:
  push:
    branches: [master]
    paths:
      - "docs/**"
      - ".github/workflows/deploy-docs.yml"
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: pages
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: npm
          cache-dependency-path: docs/package-lock.json

      - name: Install dependencies
        working-directory: docs
        run: npm ci

      - name: Build docs
        working-directory: docs
        run: npm run docs:build

      - name: Setup Pages
        uses: actions/configure-pages@v5

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: docs/.vitepress/dist

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
```

- [ ] **Step 2: Validate the YAML parses**

Run:
```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/deploy-docs.yml')); print('YAML OK')" 2>/dev/null || echo "pyyaml unavailable — verify structure by eye"
```
Expected: `YAML OK` (or verify verbatim against Step 1).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/deploy-docs.yml
git commit -m "ci: add GitHub Pages docs deploy workflow"
```

---

### Task 5: Update README badges (AFTER both are live)

**Files:**
- Modify: `README.md` (Docs + NuGet badges)

**Interfaces:**
- Consumes: a live Pages URL and a live `DataVo.Core` NuGet listing.
- Produces: badges pointing at the real, live resources.

**Gate:** Do NOT run this task until the owner confirms (a) the nuget push succeeded and `DataVo.Core` is listed, and (b) the Pages site loads at `https://arintonakos.github.io/DataVo-DBMS/`. This depends on owner-only actions (push, add secret, enable Pages) — see the checklist below.

- [ ] **Step 1: Point the Docs badge at the live site**

In `README.md`, change:
```md
[![Docs](https://img.shields.io/badge/docs-vitepress-3eaf7c)](docs/index.md)
```
to:
```md
[![Docs](https://img.shields.io/badge/docs-vitepress-3eaf7c)](https://arintonakos.github.io/DataVo-DBMS/)
```

- [ ] **Step 2: Replace the "NuGet coming soon" badge with the real one**

In `README.md`, change:
```md
[![NuGet](https://img.shields.io/badge/NuGet-coming_soon-004880)](#install-with-nuget)
```
to:
```md
[![NuGet](https://img.shields.io/nuget/vpre/DataVo.Core.svg?label=DataVo.Core)](https://www.nuget.org/packages/DataVo.Core/)
```
(The `vpre` variant shows the current prerelease version. Leave the npm "coming soon" badge unchanged — out of scope.)

- [ ] **Step 3: Verify the badge links resolve**

Run:
```bash
grep -nE 'shields.io/nuget/vpre/DataVo.Core|arintonakos.github.io/DataVo-DBMS' README.md
```
Expected: both replaced lines present.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: point Docs and NuGet badges at live resources"
```

---

## Owner action checklist (Claude cannot do these — they need your credentials/permissions)

1. **NuGet push (now):** create a nuget.org API key (scope **Push**, glob `DataVo.*`), then run the Task 1 Step 6 push command. This makes `0.1.0-preview.1` live.
2. **Release secret:** repo **Settings → Secrets and variables → Actions → New repository secret** named `NUGET_API_KEY`. Enables the Task 2 workflow; afterwards `git tag v0.1.0-preview.2 && git push origin v0.1.0-preview.2` auto-publishes.
3. **Enable Pages:** repo **Settings → Pages → Build and deployment → Source = "GitHub Actions"**. Then push the Task 3+4 commits to `master` (or run the workflow manually) to publish the site.
4. **After 1 + 3 are confirmed live:** tell Claude to run Task 5 (README badge update).

## Self-review notes

- **Spec coverage:** Part 1a → Task 1 (+ owner checklist #1); Part 1b → Task 2 (+ #2); Part 2a → Task 3; Part 2b → Task 4 (+ #3); Part 2c → Task 5. All spec sections covered.
- **Favicon nuance:** spec said "fix or drop if missing"; asset confirmed deleted, so Task 3 drops both the `head` link and the `logo` reference.
- **No placeholders:** the only `<...>` token is the owner's secret API key, which is intentionally owner-supplied.
