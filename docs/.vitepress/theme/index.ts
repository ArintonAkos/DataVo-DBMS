import DefaultTheme from 'vitepress/theme';
import { withBase, type EnhanceAppContext } from 'vitepress';
import { h } from 'vue';
import './custom.css';
// @ts-ignore
import SqlEditor from '../../src/components/SqlEditor.vue';
// @ts-ignore
import BenchmarkChart from '../../src/components/BenchmarkChart.vue';
// @ts-ignore
import CopyAiMarkdown from '../../src/components/CopyAiMarkdown.vue';
// @ts-ignore
import ImageLightbox from '../../src/components/ImageLightbox.vue';
// @ts-ignore
import GitHubStarButton from './components/GitHubStarButton.vue';

const BASE_PREFIX = '/DataVo-DBMS';

const LEGACY_REDIRECTS: Record<string, string> = {
  '/manual/preface/what-is-datavo': '/manual/getting-started/introduction',
  '/manual/preface/project-status': '/manual/getting-started/roadmap',
  '/manual/preface/alpha-scope': '/manual/getting-started/roadmap',
  '/manual/preface/roadmap': '/manual/getting-started/roadmap',
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

function resolveLegacyRedirect(raw: string): string | null {
  const hashIndex = raw.indexOf('#');
  const hash = hashIndex >= 0 ? raw.slice(hashIndex) : '';
  const withoutHash = hashIndex >= 0 ? raw.slice(0, hashIndex) : raw;

  const queryIndex = withoutHash.indexOf('?');
  const query = queryIndex >= 0 ? withoutHash.slice(queryIndex) : '';
  let pathname = queryIndex >= 0 ? withoutHash.slice(0, queryIndex) : withoutHash;

  if (pathname.startsWith(`${BASE_PREFIX}/`)) {
    pathname = pathname.slice(BASE_PREFIX.length);
  } else if (pathname === BASE_PREFIX) {
    pathname = '/';
  }

  pathname = pathname.replace(/\.html$/u, '').replace(/\/+$/u, '');
  const target = LEGACY_REDIRECTS[pathname];
  if (!target) {
    return null;
  }

  return `${withBase(target)}${query}${hash}`;
}

declare global {
  interface Window {
    goatcounter?: {
      count: (vars?: {
        path?: string;
        title?: string;
        referrer?: string;
        event?: boolean;
      }) => void;
    };
  }
}

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'nav-bar-content-after': () => h(GitHubStarButton),
      'doc-before': () => h(CopyAiMarkdown),
      'layout-bottom': () => h(ImageLightbox),
    });
  },
  enhanceApp({ app, router }: EnhanceAppContext) {
    // Register custom components globally
    app.component('SqlEditor', SqlEditor);
    app.component('BenchmarkChart', BenchmarkChart);
    app.component('CopyAiMarkdown', CopyAiMarkdown);
    app.component('ImageLightbox', ImageLightbox);
    app.component('GitHubStarButton', GitHubStarButton);

    if (typeof window !== 'undefined') {
      const initialRedirect = resolveLegacyRedirect(
        `${window.location.pathname}${window.location.search}${window.location.hash}`,
      );
      if (initialRedirect) {
        queueMicrotask(() => {
          router.go(initialRedirect);
        });
      }

      const previousBeforeRouteChange = router.onBeforeRouteChange;
      router.onBeforeRouteChange = (to: string) => {
        const redirect = resolveLegacyRedirect(to);
        if (redirect) {
          queueMicrotask(() => {
            router.go(redirect);
          });
          return false;
        }
        return previousBeforeRouteChange?.(to);
      };

      if (import.meta.env.PROD) {
        const previousAfterRouteChange = router.onAfterRouteChange;
        router.onAfterRouteChange = (to: string) => {
          previousAfterRouteChange?.(to);
          window.goatcounter?.count({ path: to });
        };
      }
    }
  },
};
