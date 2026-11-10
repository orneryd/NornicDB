import DefaultTheme from "vitepress/theme";
import type { Theme } from "vitepress";
import Explorer from "./Explorer.vue";
import Diagram from "./Diagram.vue";
import SearchPage from "./SearchPage.vue";
import Related from "./Related.vue";
import InquiryForm from './InquiryForm.vue';
import "./style.css";
export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component("CodeExplorer", Explorer);
    app.component("Diagram", Diagram);
    app.component("SearchPage", SearchPage);
    app.component('InquiryForm', InquiryForm);
  },
  Layout: Related,
} satisfies Theme;
