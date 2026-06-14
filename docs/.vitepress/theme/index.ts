import DefaultTheme from 'vitepress/theme';
// @ts-ignore
import SqlEditor from '../../src/components/SqlEditor.vue';

export default {
  extends: DefaultTheme,
  enhanceApp({ app }: { app: any }) {
    // Register the custom component globally
    app.component('SqlEditor', SqlEditor);
  }
};
