/**
 * VitePress serves the docs site under `base: "/DataVo-DBMS/"` (see `.vitepress/config.mts`), so every
 * page and source module the browser tests load lives under this prefix, never at the server root.
 */
export const DOCS_BASE_PATH = "/DataVo-DBMS/";

/** Dev-server URL of the browser client module, imported inside `page.evaluate`. */
export const DATAVO_CLIENT_MODULE_URL = `${DOCS_BASE_PATH}src/DataVoClient.ts`;
