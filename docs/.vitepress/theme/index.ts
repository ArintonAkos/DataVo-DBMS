import DefaultTheme from 'vitepress/theme';
import { h } from 'vue';
// @ts-ignore
import SqlEditor from '../../src/components/SqlEditor.vue';
// @ts-ignore
import BenchmarkChart from '../../src/components/BenchmarkChart.vue';
// @ts-ignore
import CopyAiMarkdown from '../../src/components/CopyAiMarkdown.vue';

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'doc-before': () => h(CopyAiMarkdown),
    });
  },
  enhanceApp({ app }: { app: any }) {
    // Register the custom component globally
    app.component('SqlEditor', SqlEditor);
    app.component('BenchmarkChart', BenchmarkChart);
    app.component('CopyAiMarkdown', CopyAiMarkdown);
  }
};
