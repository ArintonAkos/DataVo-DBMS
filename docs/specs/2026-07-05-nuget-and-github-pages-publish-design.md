# Design: Publish DataVo to NuGet + Docs to GitHub Pages

**Date:** 2026-07-05
**Status:** Approved (pending user spec review)
**Scope:** Two deliverables — (1) publish the DataVo packages to nuget.org and add release CI; (2) deploy the VitePress docs to GitHub Pages. A Medium article was discussed but is explicitly **out of scope** for this spec (owner will specify separately).

---

## Context / current state

- **Packaging is already configured.** `Directory.Build.props` marks `DataVo.Core`, `DataVo.Data`, and `DataVo.EntityFrameworkCore` packable with full metadata; `DataVo.Generators/DataVo.Generators.csproj` is independently packable as an analyzer package (packs its DLL to `analyzers/dotnet/cs`, `IncludeBuildOutput=false`). All are versioned `0.1.0-preview.1`. Metadata includes MIT license expression, embedded `README.md`, symbols (`snupkg`), repo/project URLs, and `GenerateDocumentationFile`.
- Built packages already exist in `artifacts/packages/` (`PackageOutputPath`).
- **Nothing has been pushed to nuget.org yet.** README badges say "NuGet coming soon."
- **Docs:** VitePress site under `docs/`, builds via `npm run docs:build` (`docs:ai` generator → `clean-docs-dist.cjs` → `vitepress build`) into `docs/.vitepress/dist`. Static assets in `docs/public/`.
- **No `base`** is set in `docs/.vitepress/config.mts`.
- Only one workflow exists: `.github/workflows/linux-benchmark.yml`. No NuGet or Pages workflow.
- Repo remote: `https://github.com/ArintonAkos/DataVo-DBMS`. Project-pages URL will be `https://arintonakos.github.io/DataVo-DBMS/` (github.io lowercases the owner).
- Local SDK: .NET `10.0.103`. No `global.json` pinning.

## Decisions (from brainstorming)

- **NuGet:** Publish `0.1.0-preview.1` now (owner runs the push with their API key) **and** add a release CI workflow for future versions.
- **Packages:** Publish all four — `DataVo.Core`, `DataVo.Data`, `DataVo.EntityFrameworkCore`, `DataVo.Generators`.
- **Pages:** GitHub-Actions deploy to the **project subpath** `https://arintonakos.github.io/DataVo-DBMS/` (requires `base: "/DataVo-DBMS/"`).
- **Medium article:** deferred / out of scope.

---

## Part 1 — NuGet publish + release CI

### 1a. Publish `0.1.0-preview.1` now

Claude performs:
1. Clean release pack: `dotnet pack -c Release` (all four packages emit fresh `.nupkg` + `.snupkg` into `artifacts/packages/`). Confirm a clean build with 0 errors.
2. **Verify package contents** before anything is pushed:
   - `README.md` embedded, `PackageLicenseExpression=MIT`, `.snupkg` present for each (except `DataVo.Generators`, which is `IncludeBuildOutput=false` and may not emit symbols — acceptable).
   - No test assemblies, `.pdb` junk, or stray files in the lib/analyzer paths.
   - `DataVo.Generators` DLL lands under `analyzers/dotnet/cs`.
3. Provide the owner the exact push command and the API-key steps.

Owner performs (Claude cannot — needs the secret key):
1. Create a nuget.org API key (scope: **Push**, glob pattern `DataVo.*`).
2. Run, via the `!` prefix (so the key never enters Claude's context) or their own terminal:
   ```bash
   dotnet nuget push "artifacts/packages/*.nupkg" \
     --api-key <KEY> \
     --source https://api.nuget.org/v3/index.json \
     --skip-duplicate
   ```
   - `--skip-duplicate` makes re-runs safe. `.snupkg` symbols are pushed automatically alongside each `.nupkg`.
   - `0.1.0-preview.1` lists as a **prerelease** (consumers need "include prerelease" or an explicit version).
   - Order does not matter for nuget.org acceptance (dependency resolution is at restore time), but pushing `DataVo.Core` first is tidy.

### 1b. Release CI — `.github/workflows/publish-nuget.yml`

- **Triggers:** push of tag matching `v*.*.*`, plus `workflow_dispatch` (manual).
- **Permissions:** `contents: read`.
- **Steps:**
  1. `actions/checkout`.
  2. `actions/setup-dotnet` with `dotnet-version: 10.0.x`.
  3. Derive version from the tag: `VERSION=${GITHUB_REF_NAME#v}`.
  4. `dotnet pack -c Release -p:Version=$VERSION -o artifacts/packages` (packs all `IsPackable` projects; the tag drives the version, overriding the `Directory.Build.props` default).
  5. `dotnet nuget push "artifacts/packages/*.nupkg" --api-key "${{ secrets.NUGET_API_KEY }}" --source https://api.nuget.org/v3/index.json --skip-duplicate`.
- **Owner one-time action:** add repo secret `NUGET_API_KEY`. Thereafter `git tag v0.1.0-preview.2 && git push origin v0.1.0-preview.2` publishes automatically.
- **Note:** `workflow_dispatch` runs off the default `Directory.Build.props` version (no tag) — that path is for manual re-packs; tag-triggered runs are the normal release path.

---

## Part 2 — Docs to GitHub Pages

### 2a. VitePress config change

- Add `base: "/DataVo-DBMS/"` to the `defineConfig` object in `docs/.vitepress/config.mts`.
- **Base-awareness of assets:** VitePress auto-prefixes `base` onto internal markdown links and the `themeConfig.logo`. It does **not** auto-prefix raw hrefs inside the `head` array. The `head` favicon (`href: "/favicon.svg"`) must be made base-aware — either hardcode `/DataVo-DBMS/favicon.svg` or compute it. Verify the favicon asset actually exists under `docs/public/` (fix or drop the reference if missing).
- Local dev: with a non-root `base`, `vitepress dev` serves under the subpath; note this in case the owner tests locally.

### 2b. Deploy workflow — `.github/workflows/deploy-docs.yml`

- **Triggers:** push to `master` touching `docs/**`, plus `workflow_dispatch`.
- **Permissions:** `pages: write`, `id-token: write`, `contents: read`.
- **Concurrency:** group `pages`, `cancel-in-progress: false` (standard Pages pattern).
- **Build job** (`working-directory: docs` where relevant):
  1. `actions/checkout`.
  2. `actions/setup-node` with `node-version: 20` and npm cache keyed to `docs/package-lock.json`.
  3. `npm ci` in `docs/`.
  4. `npm run docs:build` (runs `docs:ai` generator → `clean-docs-dist.cjs` → `vitepress build`).
  5. `actions/configure-pages`.
  6. `actions/upload-pages-artifact` with `path: docs/.vitepress/dist`.
- **Deploy job:** `needs: build`, environment `github-pages`, `actions/deploy-pages`.
- **Owner one-time action:** repo **Settings → Pages → Source = "GitHub Actions"**.
- **Result:** live at `https://arintonakos.github.io/DataVo-DBMS/`.

### 2c. README badge cleanup (after both live)

- Flip the "Docs — vitepress" badge target to the live Pages URL.
- Flip the "NuGet — coming soon" badge to the real `DataVo.Core` shields.io NuGet badge/link. (npm badge stays "coming soon" unless separately addressed.)

---

## Testing / verification

- **NuGet:** local `dotnet pack -c Release` succeeds with 0 errors; inspect one `.nupkg` (unzip) to confirm README + license + analyzer/lib layout before advising the push. CI workflow validated by YAML lint + a dry tag on a throwaway version if desired (guarded by `--skip-duplicate`).
- **Pages:** local `npm run docs:build` succeeds and produces `docs/.vitepress/dist` with base-prefixed asset URLs; spot-check generated HTML for `/DataVo-DBMS/` asset paths. First live deploy confirmed by loading the Pages URL and checking CSS/JS/logo load (the classic `base` failure mode is unstyled pages from 404'd assets).

## Out of scope

- Medium / blog article (owner will specify separately).
- npm package publishing (badge remains "coming soon").
- Custom domain for docs.
- Any engine/source changes — this is packaging, CI, and docs config only.

## Owner action checklist (things Claude cannot do)

1. Create nuget.org API key and run the push command for `0.1.0-preview.1`.
2. Add `NUGET_API_KEY` repo secret for the release workflow.
3. Set repo **Settings → Pages → Source = "GitHub Actions"**.
