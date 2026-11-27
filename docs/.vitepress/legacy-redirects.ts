/**
 * Pre-reorg manual URLs (and two mistyped roadmap links that circulate externally) mapped to the pages that replaced
 * them. The build writes one static redirect page per old path (see `writeLegacyRedirectPages`), so a bookmarked or
 * externally linked old URL forwards immediately instead of hitting the 404 page. No site page links to these paths.
 */
export const LEGACY_REDIRECTS: Record<string, string> = {
  '/manual/preface/what-is-datavo': '/manual/getting-started/introduction',
  '/manual/preface/project-status': '/manual/getting-started/roadmap',
  '/manual/preface/alpha-scope': '/manual/getting-started/roadmap',
  '/manual/preface/roadmap': '/manual/getting-started/roadmap',
  '/manual/getting-started/roadmap)': '/manual/getting-started/roadmap',
  '/manual/getting-started/roadmap))': '/manual/getting-started/roadmap',
  '/manual/tutorial/quickstart': '/manual/getting-started/quickstart',
  '/manual/tutorial/datavo-context': '/manual/client-interfaces/datavo-context-api',
  '/manual/tutorial/ado-net-and-dapper': '/manual/client-interfaces/dapper',
  '/manual/tutorial/entity-framework-example': '/manual/client-interfaces/entity-framework',
  '/manual/tutorial/vector-search-example': '/manual/fundamentals/vector-search',
  '/manual/sql-language/supported-sql': '/manual/sql/compatibility',
  '/manual/sql-language/create-table': '/manual/sql/statements/create-table',
  '/manual/sql-language/data-manipulation': '/manual/sql/statements/insert',
  '/manual/sql-language/queries': '/manual/sql/statements/select',
  '/manual/sql-language/vector-search-syntax': '/manual/fundamentals/vector-search',
  '/manual/sql-language/sql-compatibility': '/manual/sql/compatibility',
  '/manual/sql/statements/dml': '/manual/sql/statements/insert',
  '/manual/sql/statements/ddl': '/manual/sql/statements/create-table',
  '/manual/sql/statements/admin-and-security': '/manual/sql/statements/databases-and-security',
  '/manual/storage-engine/storage-modes': '/manual/fundamentals/storage-modes',
  '/manual/storage-engine/lsm-mode': '/manual/fundamentals/lsm-and-durability',
  '/manual/storage-engine/wal-and-durability': '/manual/fundamentals/lsm-and-durability',
  '/manual/storage-engine/transactions-acid-mvcc': '/manual/fundamentals/transactions-and-mvcc',
  '/manual/storage-engine/query-planner-fast-paths': '/manual/internals/query-planner',
  '/manual/client-interfaces/streaming-queries': '/manual/fundamentals/streaming-queries',
  '/manual/client-interfaces/reactive-views': '/manual/fundamentals/reactive-views',
  '/manual/client-interfaces/error-handling': '/manual/reference/error-handling',
  '/manual/performance/methodology': '/manual/performance/benchmarks',
  '/manual/reference/unsupported-features': '/manual/reference/limits',
};

/** Writes one static redirect page per legacy path into the built site, without overwriting a real page. */
export function writeLegacyRedirectPages(outDir: string, base: string, fs: typeof import("node:fs"), path: typeof import("node:path")): number {
  let written = 0;
  for (const [from, to] of Object.entries(LEGACY_REDIRECTS)) {
    const file = path.join(outDir, `${from.replace(/^\//u, "")}.html`);
    if (fs.existsSync(file)) {
      continue;
    }

    const target = `${base.replace(/\/$/u, "")}${to}`;
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(
      file,
      `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Moved</title>
<meta name="robots" content="noindex">
<link rel="canonical" href="${target}">
<meta http-equiv="refresh" content="0; url=${target}">
<script>location.replace(${JSON.stringify(target)} + location.search + location.hash);</script>
</head>
<body><p>This page moved to <a href="${target}">${target}</a>.</p></body>
</html>
`,
    );
    written++;
  }

  return written;
}
