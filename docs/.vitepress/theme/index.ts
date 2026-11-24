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
import { LEGACY_REDIRECTS } from '../legacy-redirects';

const BASE_PREFIX = '/DataVo-DBMS';

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
