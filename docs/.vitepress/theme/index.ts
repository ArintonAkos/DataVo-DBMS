import DefaultTheme from 'vitepress/theme';
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

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'doc-before': () => h(CopyAiMarkdown),
      'layout-bottom': () => h(ImageLightbox),
    });
  },
  enhanceApp({ app }: { app: any }) {
    // Register the custom component globally
    app.component('SqlEditor', SqlEditor);
    app.component('BenchmarkChart', BenchmarkChart);
    app.component('CopyAiMarkdown', CopyAiMarkdown);
    app.component('ImageLightbox', ImageLightbox);
  }
};
