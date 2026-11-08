<script setup>
import { ref, computed, onMounted, onBeforeUnmount, watch, useId } from "vue";
import { indexGraph, neighborhood, fractalAnchors } from "./graph-layout.mjs";
const props = defineProps({
    graph: Object,
    selected: String,
    direction: String,
  }),
  emit = defineEmits(["select"]);
const host = ref(null),
  depth = ref(2),
  error = ref(""),
  settled = ref(false),
  frozen = ref(false),
  helpId = useId();
const index = computed(() => indexGraph(props.graph)),
  visible = computed(() =>
    neighborhood(index.value, props.selected, {
      direction: props.direction,
      depth: depth.value,
    }),
  );
const palette = [
  "#54d5b1",
  "#79b9fa",
  "#ecad78",
  "#d5a0ed",
  "#f0d579",
  "#ef91a6",
];
const color = (node) => {
  let code = 0;
  for (const char of node.package || node.file)
    code = (code * 31 + char.charCodeAt(0)) >>> 0;
  return node.id === props.selected
    ? "#97ffe0"
    : palette[code % palette.length];
};
let scene,
  three,
  forces,
  SpriteText,
  resize,
  intersection,
  alive = true,
  clusterGroup,
  fitPending = false,
  reduced = false;
function resizeScene() {
  if (scene && host.value)
    scene
      .width(host.value.clientWidth)
      .height(host.value.clientWidth < 600 ? 340 : 460);
}
function fit() {
  if (!scene) return;
  const nodes = scene.graphData().nodes;
  if (!nodes.length) return;
  const bounds = ["x", "y", "z"].map((axis) => {
    const values = nodes.map((node) => Number(node[axis]) || 0);
    return [Math.min(...values), Math.max(...values)];
  });
  const center = {
    x: (bounds[0][0] + bounds[0][1]) / 2,
    y: (bounds[1][0] + bounds[1][1]) / 2,
    z: (bounds[2][0] + bounds[2][1]) / 2,
  };
  const span = Math.max(...bounds.map(([min, max]) => max - min));
  // Keep labels legible without fitting the camera inside a compact cluster.
  const distance = Math.max(200, (span + 50) * 1.12);
  scene.cameraPosition(
    {
      x: center.x + distance * 0.35,
      y: center.y + distance * 0.22,
      z: center.z + distance,
    },
    center,
    reduced ? 0 : 350,
  );
}
function rebuild() {
  if (!scene) return;
  settled.value = false;
  fitPending = true;
  const data = visible.value,
    { anchors, clusters } = fractalAnchors(data.nodes, props.selected);
  const nodes = data.nodes.map((node) => {
    const point = anchors.get(node.id);
    return {
      ...node,
      ...point,
      ...(node.id === props.selected ? { fx: 0, fy: 0, fz: 0 } : {}),
    };
  });
  const labelled = new Set(
    data.nodes
      .filter((node) => node.depth <= 1)
      .slice(0, 7)
      .map((node) => node.id),
  );
  if (clusterGroup) {
    scene.scene().remove(clusterGroup);
    clusterGroup.traverse((object) => {
      object.geometry?.dispose();
      object.material?.dispose();
    });
  }
  clusterGroup = new three.Group();
  for (const cluster of clusters) {
    const mesh = new three.Mesh(
      new three.SphereGeometry(1, 16, 10),
      new three.MeshBasicMaterial({
        color: "#79b9fa",
        wireframe: false,
        side: three.BackSide,
        transparent: true,
        opacity: 0.018,
        depthWrite: false,
      }),
    );
    mesh.position.set(
      cluster.position.x,
      cluster.position.y,
      cluster.position.z,
    );
    mesh.scale.setScalar(cluster.radius);
    mesh.userData.members = cluster.members;
    clusterGroup.add(mesh);
  }
  scene.scene().add(clusterGroup);
  scene
    .nodeColor(color)
    .nodeVal((node) => (node.id === props.selected ? 5 : 1.7))
    .nodeThreeObject((node) => {
      const group = new three.Group();
      if (labelled.has(node.id)) {
        const text = new SpriteText(
          node.name.length > 26 ? node.name.slice(0, 24) + "…" : node.name,
        );
        text.color = node.id === props.selected ? "#b3ffe7" : "#d7e3ed";
        text.textHeight = node.id === props.selected ? 14 : 9;
        text.position.y = 8;
        text.material.depthWrite = false;
        group.add(text);
      }
      return group;
    })
    .nodeThreeObjectExtend(true)
    .d3Force(
      "fractal-x",
      forces
        .forceX((node) => anchors.get(node.id)?.x ?? node.x ?? 0)
        .strength(0.16),
    )
    .d3Force(
      "fractal-y",
      forces
        .forceY((node) => anchors.get(node.id)?.y ?? node.y ?? 0)
        .strength(0.16),
    )
    .d3Force(
      "fractal-z",
      forces
        .forceZ((node) => anchors.get(node.id)?.z ?? node.z ?? 0)
        .strength(0.16),
    )
    .d3Force("collision", forces.forceCollide(10))
    .warmupTicks(70)
    .cooldownTicks(frozen.value ? 0 : 120)
    .graphData({ nodes, links: data.links.map((edge) => ({ ...edge })) });
  scene.d3Force("charge").strength(-65);
  scene.d3Force("link").distance(72).strength(0.08);
}
function alignClusters() {
  if (!clusterGroup || !scene) return;
  const byId = new Map(scene.graphData().nodes.map((node) => [node.id, node]));
  for (const mesh of clusterGroup.children) {
    const nodes = mesh.userData.members
      .map((id) => byId.get(id))
      .filter(Boolean);
    if (!nodes.length) continue;
    const center = new three.Vector3();
    for (const node of nodes)
      center.add(new three.Vector3(node.x || 0, node.y || 0, node.z || 0));
    center.divideScalar(nodes.length);
    const radius =
      Math.max(
        ...nodes.map((node) =>
          center.distanceTo(
            new three.Vector3(node.x || 0, node.y || 0, node.z || 0),
          ),
        ),
      ) + 12;
    mesh.position.copy(center);
    mesh.scale.setScalar(radius);
  }
}
function freeze() {
  frozen.value = !frozen.value;
  scene?.cooldownTicks(frozen.value ? 0 : 120);
  if (!frozen.value) scene?.d3ReheatSimulation();
}
onMounted(async () => {
  try {
    reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;
    frozen.value = reduced;
    const [graphModule, forceModule, textModule, threeModule] =
      await Promise.all([
        import("3d-force-graph"),
        import("d3-force-3d"),
        import("three-spritetext"),
        import("three"),
      ]);
    if (!alive) return;
    three = threeModule;
    forces = forceModule;
    SpriteText = textModule.default;
    scene = new graphModule.default(host.value, {
      controlType: "orbit",
      rendererConfig: { antialias: true, powerPreference: "low-power" },
    })
      .forceEngine("d3")
      .numDimensions(3)
      .backgroundColor("#151a21")
      .showNavInfo(false)
      .enableNodeDrag(false)
      .nodeRelSize(3)
      .linkColor(() => "#6f929f")
      .linkOpacity(0.45)
      .linkWidth(0.5)
      .linkDirectionalArrowLength(5)
      .linkDirectionalArrowRelPos(0.8)
      .nodeLabel((node) => {
        const label = document.createElement("span");
        label.textContent = node.name + " — " + node.file;
        return label;
      })
      .onNodeClick((node) => emit("select", node.id))
      .onEngineTick(alignClusters)
      .onEngineStop(() => {
        alignClusters();
        settled.value = true;
        if (fitPending) {
          fitPending = false;
          fit();
        }
      });
    scene.cameraPosition({ x: 240, y: 140, z: 500 });
    resizeScene();
    rebuild();
    resize = new ResizeObserver(resizeScene);
    resize.observe(host.value);
    intersection = new IntersectionObserver((entries) => {
      if (!scene) return;
      entries[0].isIntersecting
        ? scene.resumeAnimation()
        : scene.pauseAnimation();
    });
    intersection.observe(host.value);
  } catch {
    error.value =
      "3D rendering is unavailable on this device. Use the source and node lists below.";
  }
});
watch([() => props.selected, () => props.direction, depth], rebuild);
onBeforeUnmount(() => {
  alive = false;
  resize?.disconnect();
  intersection?.disconnect();
  scene?._destructor();
  scene = null;
});
</script>
<template>
  <section class="force-neighborhood">
    <div class="graph-controls">
      <label
        >Call depth
        <select v-model.number="depth">
          <option :value="1">1 hop</option>
          <option :value="2">2 hops</option>
          <option :value="3">3 hops</option>
        </select></label
      ><button @click="fit">Fit graph</button
      ><button @click="freeze">
        {{ frozen ? "Resume layout" : "Freeze layout" }}</button
      ><span class="status"
        >{{ visible.nodes.length }} nodes · {{ visible.links.length }} calls ·
        max 60 nodes</span
      >
    </div>
    <p :id="helpId" class="status">
      Hover for names · drag to rotate · scroll or pinch to zoom · select a node
      to inspect. Nested clusters group packages and call branches.
    </p>
    <div
      v-if="!error"
      ref="host"
      class="graph3d"
      role="group"
      aria-label="3D force-directed code graph"
      :aria-describedby="helpId"
      :data-state="settled ? 'settled' : 'simulating'"
    ></div>
    <p v-else role="status">{{ error }}</p>
    <details :open="!!error" class="graph-node-list">
      <summary>Browse graph nodes ({{ visible.nodes.length }})</summary>
      <ul>
        <li v-for="node in visible.nodes" :key="node.id">
          <button
            @click="emit('select', node.id)"
            :aria-current="node.id === selected ? 'true' : undefined"
          >
            {{ node.name }} <small>{{ node.file }}</small>
          </button>
        </li>
      </ul>
    </details>
  </section>
</template>
