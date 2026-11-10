<script setup>
import Analytics from "./Analytics.vue";
import DefaultTheme from "vitepress/theme";
import SearchPage from "./SearchPage.vue";
import VersionPicker from "./VersionPicker.vue";
import Explorer from "./Explorer.vue";
import SearchDialog from "./SearchDialog.vue";
import { useData, withBase } from "vitepress";
import { computed, ref, onMounted, nextTick } from "vue";
const { page } = useData();
const nodes = ref({});
const dialog = ref(null),
  active = ref(null);
async function preview(node) {
  active.value = node;
  await nextTick();
  dialog.value.showModal();
}
onMounted(async () => {
  nodes.value = await fetch(withBase("/doc-code.json")).then((r) => r.json());
});
const related = computed(
  () => nodes.value["/" + page.value.relativePath.replace(/\.md$/, "")] || [],
);
</script>
<template>
  <DefaultTheme.Layout
    ><template #layout-bottom><Analytics /></template><template #nav-bar-content-before><SearchDialog /></template
    ><template #nav-bar-content-after><VersionPicker /></template
    ><template #doc-after
      ><aside v-if="related.length" class="related">
        <h2>Related implementation</h2>
        <p>
          Source symbols linked to this page. Select one to inspect its callers
          and callees.
        </p>
        <div v-for="node in related" :key="node.id" class="related-row">
          <button @click="preview(node)">
            {{ node.name }} <small>{{ node.file }}</small></button
          ><a
            :href="
              withBase(
                '/explore?symbol=' +
                  encodeURIComponent(node.id) +
                  '&doc=' +
                  encodeURIComponent(node.docs[0].id),
              )
            "
            >Open explorer</a
          >
        </div>
        <dialog
          ref="dialog"
          class="source-dialog"
          aria-label="Related source code"
        >
          <button class="dialog-close" @click="dialog.close()">
            Close code preview</button
          ><Explorer
            v-if="active"
            :key="active.id"
            :initial-symbol="active.id"
            :context-doc="active.docs[0].id"
            embedded
          />
        </dialog></aside></template
    ><template #not-found><SearchPage /></template
  ></DefaultTheme.Layout>
</template>
