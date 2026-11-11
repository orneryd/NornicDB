import { useEffect, useRef, useState } from "react";
import ForceGraph3D, { type ForceGraph3DInstance } from "3d-force-graph";
import * as THREE from "three";
import { forceX, forceY, forceZ } from "d3-force-3d";
import type { GraphNodePayload, GraphEdgePayload } from "../../utils/api";
import { useViewState } from "../../utils/useUrlState";
import { Disclosure } from "../common/Disclosure";
import { Button } from "../common/Button";
import { Alert } from "../common/Alert";

type Point = { x: number; y: number; z: number };
type SceneNode = GraphNodePayload &
  Partial<Point> & { fx?: number; fy?: number; fz?: number };
type SceneLink = Omit<GraphEdgePayload, "source" | "target"> & {
  source: string | SceneNode;
  target: string | SceneNode;
};
type Camera = { position: Point; target: Point };
const endpoint = (node: string | SceneNode) =>
  typeof node === "string" ? node : node.id;
const name = (node: GraphNodePayload) =>
  String(
    node.properties.title ??
      node.properties.name ??
      node.properties.path ??
      node.id,
  );
const colors = [
  "#34d399",
  "#60a5fa",
  "#c4b5fd",
  "#fbbf24",
  "#fb7185",
  "#67e8f9",
];
export function graphColor(label: string) {
  let hash = 0;
  for (const c of label) hash = (hash * 31 + c.charCodeAt(0)) >>> 0;
  return colors[hash % colors.length];
}

export function QueryGraphScene({
  nodes,
  edges,
  selectedId,
  selectedEdgeId,
  rootId,
  onNodeSelect,
  onEdgeSelect,
}: {
  nodes: GraphNodePayload[];
  edges: GraphEdgePayload[];
  selectedId: string | null;
  selectedEdgeId: string | null;
  rootId: string | null;
  onNodeSelect: (node: GraphNodePayload) => void;
  onEdgeSelect: (edge: GraphEdgePayload) => void;
}) {
  const host = useRef<HTMLDivElement>(null),
    instance = useRef<ForceGraph3DInstance<SceneNode, SceneLink> | null>(null);
  const [labels, setLabels] = useViewState("graph.labels", false),
    [paused, setPaused] = useViewState("graph.paused", false);
  const [pins, setPins] = useViewState<Record<string, Point>>("graph.pins", {});
  const [camera, setCamera] = useViewState<Camera | null>("graph.camera", null);
  const [failure, setFailure] = useState("");
  const latest = useRef({
    nodes,
    edges,
    selectedId,
    selectedEdgeId,
    rootId,
    onNodeSelect,
    onEdgeSelect,
    labels,
    pins,
    paused,
    camera,
  });
  latest.current = {
    nodes,
    edges,
    selectedId,
    selectedEdgeId,
    rootId,
    onNodeSelect,
    onEdgeSelect,
    labels,
    pins,
    paused,
    camera,
  };
  const engineReady = useRef(false);
  const restoringCamera = useRef(false),
    resources = useRef<THREE.Object3D[]>([]);
  const captureCamera = () => {
    const fg = instance.current;
    if (!fg || restoringCamera.current) return;
    const position = fg.cameraPosition(),
      controls = fg.controls() as unknown as { target: Point };
    setCamera({
      position: { x: position.x, y: position.y, z: position.z },
      target: {
        x: controls.target.x,
        y: controls.target.y,
        z: controls.target.z,
      },
    });
  };
  const moveCamera = (angle: number, scale: number) => {
    const fg = instance.current;
    if (!fg) return;
    const position = fg.cameraPosition(),
      target = (fg.controls() as unknown as { target: Point }).target;
    const x = (position.x - target.x) * scale,
      z = (position.z - target.z) * scale;
    setCamera({
      position: {
        x: target.x + x * Math.cos(angle) - z * Math.sin(angle),
        y: target.y + (position.y - target.y) * scale,
        z: target.z + x * Math.sin(angle) + z * Math.cos(angle),
      },
      target: { x: target.x, y: target.y, z: target.z },
    });
  };
  const fitGraph = () => {
    const fg = instance.current;
    if (!fg || !host.current) return;
    const points = fg
      .graphData()
      .nodes.filter(
        (node) =>
          Number.isFinite(node.x) &&
          Number.isFinite(node.y) &&
          Number.isFinite(node.z),
      );
    if (!points.length) return;
    const bounds = (axis: "x" | "y" | "z") => {
      const values = points.map((node) => node[axis] ?? 0);
      return [Math.min(...values) - 12, Math.max(...values) + 12];
    };
    const [minX, maxX] = bounds("x"),
      [minY, maxY] = bounds("y"),
      [minZ, maxZ] = bounds("z");
    const target = {
      x: (minX + maxX) / 2,
      y: (minY + maxY) / 2,
      z: (minZ + maxZ) / 2,
    };
    const fov = ((fg.camera() as THREE.PerspectiveCamera).fov * Math.PI) / 180,
      aspect =
        host.current.clientWidth / Math.max(host.current.clientHeight, 1);
    const distance =
      Math.max(
        (maxY - minY) / (2 * Math.tan(fov / 2)),
        (maxX - minX) / (2 * Math.tan(fov / 2) * aspect),
      ) *
        1.25 +
      (maxZ - minZ) / 2;
    setCamera({
      target,
      position: {
        x: target.x,
        y: target.y,
        z: target.z + Math.max(distance, 70),
      },
    });
  };
  const focus = (id: string) => {
    const fg = instance.current,
      node = fg?.graphData().nodes.find((n) => n.id === id);
    if (!fg || !node) return;
    const target = { x: node.x ?? 0, y: node.y ?? 0, z: node.z ?? 0 };
    setCamera({
      position: { x: target.x + 40, y: target.y + 30, z: target.z + 140 },
      target,
    });
  };
  useEffect(() => {
    if (!host.current) return;
    let fg: ForceGraph3DInstance<SceneNode, SceneLink>;
    try {
      fg = new ForceGraph3D(host.current, {
        controlType: "orbit",
      }) as unknown as ForceGraph3DInstance<SceneNode, SceneLink>;
      fg.backgroundColor("#0a0e1a")
        .showNavInfo(false)
        .numDimensions(3)
        .nodeId("id")
        .nodeRelSize(5)
        .nodeVal((node) => (node.id === latest.current.rootId ? 2.3 : 1))
        .nodeLabel((node) => {
          const el = document.createElement("div");
          el.textContent = `${name(node)} · ${node.labels.join(", ")}`;
          return el;
        })
        .linkLabel((link) => {
          const el = document.createElement("div");
          el.textContent = link.type;
          return el;
        })
        .linkDirectionalArrowLength(4)
        .linkDirectionalArrowRelPos(0.8)
        .linkOpacity(0.65)
        .linkWidth((link) =>
          link.id === latest.current.selectedEdgeId ? 3 : 1,
        )
        .linkColor((link) =>
          link.id === latest.current.selectedEdgeId ? "#fbbf24" : "#64748b",
        )
        .cooldownTicks(100)
        .onEngineTick(() => {
          engineReady.current = true;
        })
        .onEngineStop(() => {
          if (!latest.current.camera) fitGraph();
        })
        .onNodeClick((node) => {
          latest.current.onNodeSelect(node);
          focus(node.id);
        })
        .onLinkClick((link) =>
          latest.current.onEdgeSelect({
            ...link,
            source: endpoint(link.source),
            target: endpoint(link.target),
          }),
        )
        .onNodeDragEnd((node) => {
          setPins((previous) => ({
            ...previous,
            [node.id]: { x: node.x ?? 0, y: node.y ?? 0, z: node.z ?? 0 },
          }));
        });
      instance.current = fg;
      fg.d3Force("charge")?.strength(-100);
      fg.d3Force("link")?.distance(60);
      fg.d3Force("x", forceX(() => 0).strength(0.06));
      fg.d3Force("y", forceY(() => 0).strength(0.06));
      fg.d3Force("z", forceZ(() => 0).strength(0.06));
    } catch (error) {
      setFailure(
        error instanceof Error
          ? error.message
          : "Unable to initialize the 3D renderer.",
      );
      return;
    }
    const resize = new ResizeObserver(() => {
      if (host.current)
        fg.width(host.current.clientWidth).height(host.current.clientHeight);
    });
    resize.observe(host.current);
    const controls = fg.controls() as unknown as {
      target: THREE.Vector3;
      addEventListener: (event: string, callback: () => void) => void;
      removeEventListener: (event: string, callback: () => void) => void;
    };
    const saveCamera = captureCamera;
    controls.addEventListener("end", saveCamera);
    return () => {
      resize.disconnect();
      controls.removeEventListener("end", saveCamera);
      fg._destructor();
      engineReady.current = false;
      instance.current = null;
      resources.current.forEach(dispose);
      resources.current = [];
    };
  }, []);
  useEffect(() => {
    const fg = instance.current;
    if (!fg) return;
    const previous = new Map(fg.graphData().nodes.map((n) => [n.id, n]));
    fg.graphData({
      nodes: nodes.map((node) => ({
        ...node,
        ...(previous.get(node.id)
          ? {
              x: previous.get(node.id)?.x,
              y: previous.get(node.id)?.y,
              z: previous.get(node.id)?.z,
            }
          : {}),
        ...(pins[node.id]
          ? { fx: pins[node.id].x, fy: pins[node.id].y, fz: pins[node.id].z }
          : {}),
      })),
      links: edges.map((edge) => ({ ...edge })),
    });
  }, [nodes, edges]);
  useEffect(() => {
    const fg = instance.current;
    if (!fg) return;
    resources.current.forEach(dispose);
    resources.current = [];
    fg.nodeThreeObject((node) => {
      const group = new THREE.Group(),
        color = graphColor(node.labels[0] ?? "Node");
      const sphere = new THREE.Mesh(
        new THREE.SphereGeometry(node.id === rootId ? 10 : 7, 20, 16),
        new THREE.MeshLambertMaterial({
          color: node.id === selectedId ? "#ecfdf5" : color,
          emissive: color,
          emissiveIntensity: node.id === selectedId ? 0.65 : 0.16,
        }),
      );
      group.add(sphere);
      if (node.id === selectedId || node.id === rootId) {
        const ring = new THREE.Mesh(
          new THREE.TorusGeometry(9, 0.5, 8, 40),
          new THREE.MeshBasicMaterial({
            color: node.id === selectedId ? "#34d399" : "#fbbf24",
          }),
        );
        group.add(ring);
      }
      if (labels || node.id === selectedId) {
        const canvas = document.createElement("canvas");
        canvas.width = 512;
        canvas.height = 64;
        const ctx = canvas.getContext("2d");
        if (ctx) {
          ctx.font = "24px system-ui";
          ctx.textAlign = "center";
          ctx.fillStyle = "#0a0e1a";
          ctx.fillRect(0, 0, 512, 64);
          ctx.fillStyle = "#f1f5f9";
          ctx.fillText(name(node).slice(0, 36), 256, 40);
          const sprite = new THREE.Sprite(
            new THREE.SpriteMaterial({
              map: Object.assign(new THREE.CanvasTexture(canvas), {
                colorSpace: THREE.SRGBColorSpace,
              }),
              transparent: true,
              depthWrite: false,
            }),
          );
          sprite.scale.set(64, 8, 1);
          sprite.position.set(0, -13, 0);
          group.add(sprite);
        }
      }
      resources.current.push(group);
      return group;
    });
    fg.linkWidth((link) => (link.id === selectedEdgeId ? 3 : 1)).linkColor(
      (link) =>
        link.id === selectedEdgeId
          ? "#fbbf24"
          : selectedId &&
              (endpoint(link.source) === selectedId ||
                endpoint(link.target) === selectedId)
            ? "#34d399"
            : "#64748b",
    );
  }, [nodes, selectedId, selectedEdgeId, rootId, labels]);
  useEffect(() => {
    const fg = instance.current;
    if (!fg) return;
    for (const node of fg.graphData().nodes) {
      const pin = pins[node.id];
      node.fx = pin?.x;
      node.fy = pin?.y;
      node.fz = pin?.z;
    }
    if (engineReady.current) fg.d3ReheatSimulation();
  }, [JSON.stringify(pins)]);
  useEffect(() => {
    const fg = instance.current;
    if (!fg) return;
    fg.cooldownTicks(paused ? 0 : 100);
    if (!paused && engineReady.current) fg.d3ReheatSimulation();
  }, [paused]);
  useEffect(() => {
    if (!camera || !instance.current) return;
    restoringCamera.current = true;
    instance.current.cameraPosition(camera.position, camera.target, 0);
    restoringCamera.current = false;
  }, [JSON.stringify(camera)]);
  return (
    <section
      aria-label="3D graph workspace"
      className="relative rounded-xl overflow-hidden border border-norse-rune"
    >
      <div className="flex flex-wrap gap-2 items-center bg-norse-shadow px-3 py-2 border-b border-norse-rune">
        <Button variant="secondary" onClick={fitGraph}>
          Fit graph
        </Button>
        <Button
          variant="secondary"
          disabled={!selectedId}
          onClick={() => selectedId && focus(selectedId)}
        >
          Focus selected
        </Button>
        <Button variant="secondary" onClick={() => setPaused(!paused)}>
          {paused ? "Resume motion" : "Pause motion"}
        </Button>
        <Button
          variant="secondary"
          disabled={!Object.keys(pins).length}
          onClick={() => setPins({})}
        >
          Release pinned nodes
        </Button>
        <label className="text-sm text-norse-silver flex items-center gap-2">
          <input
            type="checkbox"
            checked={labels}
            onChange={(e) => setLabels(e.target.checked)}
          />
          Node labels
        </label>
      </div>
      <div className="flex flex-wrap gap-3 px-3 py-2 text-xs text-norse-silver">
        {[...new Set(nodes.flatMap((node) => node.labels))]
          .sort()
          .map((label) => (
            <span key={label} className="inline-flex items-center gap-1">
              <span
                aria-hidden="true"
                className="w-2 h-2 rounded-full"
                style={{ background: graphColor(label) }}
              />
              {label}
            </span>
          ))}
        <span>Emerald ring: selected · Gold ring: root</span>
      </div>
      {failure && <Alert type="error" message={failure} />}
      <div
        ref={host}
        className="w-full h-[480px] min-w-0"
        aria-label="Interactive 3D graph canvas"
        role="img"
      />
      <Disclosure compact id="graph-camera" title="View controls (keyboard)">
        <div className="flex flex-wrap gap-2 p-2">
          <Button
            variant="secondary"
            onClick={() => moveCamera(-Math.PI / 8, 1)}
          >
            Orbit left
          </Button>
          <Button
            variant="secondary"
            onClick={() => moveCamera(Math.PI / 8, 1)}
          >
            Orbit right
          </Button>
          <Button variant="secondary" onClick={() => moveCamera(0, 0.8)}>
            Zoom in
          </Button>
          <Button variant="secondary" onClick={() => moveCamera(0, 1.25)}>
            Zoom out
          </Button>
        </div>
      </Disclosure>
      <p className="px-3 py-2 text-xs text-norse-silver bg-norse-shadow">
        Drag the background to orbit. Scroll to zoom. Right-drag to pan. Drag a
        node to pin it. Select a node or relationship to inspect it. Keyboard
        users can use the node and relationship lists below.
      </p>
    </section>
  );
}
function dispose(object: THREE.Object3D) {
  object.traverse((child) => {
    if (child instanceof THREE.Mesh || child instanceof THREE.Sprite) {
      if (child instanceof THREE.Mesh) child.geometry.dispose();
      const materials = Array.isArray(child.material)
        ? child.material
        : [child.material];
      materials.forEach((material) => {
        if ("map" in material && (material as THREE.SpriteMaterial).map)
          (material as THREE.SpriteMaterial).map?.dispose();
        material.dispose();
      });
    }
  });
}
