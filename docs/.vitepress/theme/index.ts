import DefaultTheme from 'vitepress/theme';
import type { EnhanceAppContext } from 'vitepress';
import { h } from 'vue';
import './custom.css';
// @ts-ignore
import SqlEditor from '../../src/components/SqlEditor.vue';
// @ts-ignore
import CopyAiMarkdown from '../../src/components/CopyAiMarkdown.vue';
// @ts-ignore
import ImageLightbox from '../../src/components/ImageLightbox.vue';
// @ts-ignore
import GitHubStarButton from './components/GitHubStarButton.vue';

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
    app.component('CopyAiMarkdown', CopyAiMarkdown);
    app.component('ImageLightbox', ImageLightbox);
    app.component('GitHubStarButton', GitHubStarButton);

    if (typeof window !== 'undefined' && import.meta.env.PROD) {
      const previousAfterRouteChange = router.onAfterRouteChange;
      router.onAfterRouteChange = (to: string) => {
        previousAfterRouteChange?.(to);
        window.goatcounter?.count({ path: to });
      };
    }
  },
};
