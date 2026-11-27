/**
 * Resolves the URL of the published DataVo WASM bundle (`docs/public/datavo-wasm/`).
 *
 * VitePress serves the site under its configured `base` (`/DataVo-DBMS/`), which Vite exposes as
 * `import.meta.env.BASE_URL`, so the bundle is never at the server root.
 *
 * @param origin Origin of the page or worker loading the bundle.
 * @returns Absolute bundle URL ending in `/`.
 *
 * @example
 * const interopUrl = `${resolveDataVoWasmBaseUrl(location.origin)}datavo.interop.js`;
 */
export function resolveDataVoWasmBaseUrl(origin: string): string {
  return new URL(`${import.meta.env.BASE_URL}datavo-wasm/`, origin).href;
}
