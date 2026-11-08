<script setup>
import { ref, onMounted } from "vue";
const props = defineProps({ source: String });
const target = ref(null);
const error = ref(false);
onMounted(async () => {
  try {
    const { default: mermaid } = await import("mermaid");
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: "strict",
      theme: "neutral",
    });
    const { svg } = await mermaid.render(
      "diagram-" + Math.random().toString(36).slice(2),
      props.source,
    );
    target.value.innerHTML = svg;
  } catch {
    error.value = true;
  }
});
</script>
<template>
  <div
    class="diagram"
    ref="target"
    role="img"
    aria-label="Architecture diagram"
  ></div>
  <details v-if="error">
    <summary>Diagram source (rendering unavailable)</summary>
    <pre>{{ source }}</pre>
  </details>
</template>
