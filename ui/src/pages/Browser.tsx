import { useCallback, useEffect, useId, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Terminal, Sparkles, Database } from "lucide-react";
import { useAppStore } from "../store/appStore";
import { Bifrost } from "../../Bifrost";
import { api } from "../utils/api";
import { Header } from "../components/browser/Header";
import { QueryPanel } from "../components/browser/QueryPanel";
import { SearchPanel } from "../components/browser/SearchPanel";
import { NodeDetailsPanel } from "../components/browser/NodeDetailsPanel";
import { GraphExplorerPanel, type GraphExplorerPanelControls } from "../components/browser/GraphExplorerPanel";
import { GraphExplorerControls } from "../components/browser/GraphExplorerControls";
import { DeleteConfirmModal } from "../components/modals/DeleteConfirmModal";
import { RegenerateConfirmModal } from "../components/modals/RegenerateConfirmModal";
import { BASE_PATH, joinBasePath } from "../utils/basePath";
import {
  buildNeighborhoodGraphHandoff,
  mergeBrowserUrlState,
  readBrowserUrlState,
  type BrowserViewMode,
} from "../utils/browserUrlState";

interface EmbedStats {
  running: boolean;
  processed: number;
  failed: number;
}

interface EmbedData {
  stats: EmbedStats | null;
  totalEmbeddings: number;
  pendingNodes: number;
  enabled: boolean;
}

export function Browser() {
  const [searchParams, setSearchParams] = useSearchParams();
  const searchParamsString = searchParams.toString();
  const routeState = useMemo(() => readBrowserUrlState(searchParams), [searchParamsString]); // eslint-disable-line react-hooks/exhaustive-deps
  const {
    stats,
    connected,
    fetchStats,
    fetchDatabases,
    databaseList,
    selectedDatabase,
    setSelectedDatabase,
    cypherQuery,
    setCypherQuery,
    cypherResult,
    cypherResults,
    executeCypher,
    queryLoading,
    queryError,
    queryHistory,
    searchQuery,
    setSearchQuery,
    searchResults,
    executeSearch,
    searchLoading,
    searchError,
    selectedNode,
    setSelectedNode,
    selectedNodeIds,
    toggleNodeSelection,
    selectAllNodes,
    clearNodeSelection,
    findSimilar,
    expandedSimilar,
    collapseSimilar,
  } = useAppStore();

  const [graphControls, setGraphControls] = useState<GraphExplorerPanelControls | null>(null);

  const [embedData, setEmbedData] = useState<EmbedData>({
    stats: null,
    totalEmbeddings: 0,
    pendingNodes: 0,
    enabled: false,
  });
  const [embedTriggering, setEmbedTriggering] = useState(false);
  const [embedMessage, setEmbedMessage] = useState<string | null>(null);
  const [showAIChat, setShowAIChat] = useState(false);
  const [showRegenerateConfirm, setShowRegenerateConfirm] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const navigate = useNavigate();
  const databaseSelectId = useId();

  useEffect(() => {
    const fetchEmbedStats = async () => {
      try {
        const res = await fetch(joinBasePath(BASE_PATH, "/nornicdb/embed/stats"));
        if (res.ok) {
          const data = await res.json();
          setEmbedData({
            stats: data.stats || null,
            totalEmbeddings: data.total_embeddings || 0,
            pendingNodes: data.pending_nodes || 0,
            enabled: data.enabled || false,
          });
        }
      } catch {
        // Ignore errors
      }
    };

    fetchEmbedStats();
    const interval = setInterval(fetchEmbedStats, 3000);
    return () => clearInterval(interval);
  }, []);

  const handleTriggerEmbed = async () => {
    setEmbedTriggering(true);
    setEmbedMessage(null);
    try {
      const res = await fetch(joinBasePath(BASE_PATH, "/nornicdb/embed/trigger?regenerate=true"), {
        method: "POST",
      });
      const data = await res.json();
      if (res.ok) {
        setEmbedMessage(data.message);
        if (data.stats) {
          setEmbedData((prev) => ({ ...prev, stats: data.stats }));
        }
      } else {
        setEmbedMessage(data.message || "Failed to trigger embeddings");
      }
    } catch {
      setEmbedMessage("Error triggering embeddings");
    } finally {
      setEmbedTriggering(false);
      setTimeout(() => setEmbedMessage(null), 5000);
    }
  };

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 5000);
    return () => clearInterval(interval);
  }, [fetchStats]);

  useEffect(() => {
    fetchDatabases();
  }, [fetchDatabases]);

  useEffect(() => {
    if (routeState.database) {
      if (databaseList.length > 0 && databaseList.includes(routeState.database)) {
        if (selectedDatabase !== routeState.database) {
          setSelectedDatabase(routeState.database);
        }
      }
      return;
    }

    if (selectedDatabase !== null) {
      setSelectedDatabase(null);
    }
  }, [databaseList, routeState.database, selectedDatabase, setSelectedDatabase]);

  const updateRouteState = (nextState: Parameters<typeof mergeBrowserUrlState>[1]) => {
    setSearchParams(mergeBrowserUrlState(searchParams, nextState));
  };

  const selectedNodeIdsArray = useMemo(() => Array.from(selectedNodeIds), [selectedNodeIds]);

  const handleDatabaseChange = useCallback((dbName: string) => {
    const value = dbName === "" ? null : dbName;
    setSelectedDatabase(value);
    updateRouteState({ database: value });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [setSelectedDatabase, searchParams]);

  const handleViewChange = useCallback((view: BrowserViewMode) => {
    updateRouteState({ view });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const handleGraphSelectionHandoff = useCallback(() => {
    const graph = buildNeighborhoodGraphHandoff(
      Array.from(selectedNodeIds),
      selectedNode?.node.id,
    );
    if (!graph) {
      return;
    }

    updateRouteState({
      view: "graph",
      graph,
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedNodeIds, selectedNode, searchParams]);

  const handleExploreNeighborhood = useCallback((nodeIds: string[], focusedNodeId?: string | null) => {
    const graph = buildNeighborhoodGraphHandoff(nodeIds, focusedNodeId);
    if (!graph) {
      return;
    }

    updateRouteState({
      view: "graph",
      graph,
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const handleExploreNode = useCallback((nodeId: string) => {
    handleExploreNeighborhood([nodeId], nodeId);
  }, [handleExploreNeighborhood]);

  const handleClearGraph = useCallback(() => {
    updateRouteState({ graph: null });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const handleUpdateHandoff = useCallback((graph: Parameters<typeof updateRouteState>[0]["graph"]) => {
    updateRouteState({ graph });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const handleCloseDetails = useCallback(() => {
    setSelectedNode(null);
  }, [setSelectedNode]);

  const handleExposeControls = useCallback((controls: GraphExplorerPanelControls) => {
    setGraphControls(controls);
  }, []);

  const handleDeleteNodes = async () => {
    setDeleting(true);
    setDeleteError(null);
    try {
      const result = await api.deleteNodes(Array.from(selectedNodeIds));
      setShowDeleteConfirm(false);

      if (result.success) {
        clearNodeSelection();
        if (routeState.view === "query") {
          executeCypher();
        } else if (routeState.view === "search") {
          executeSearch();
        }
      } else {
        setDeleteError(result.errors.join(", "));
      }
    } catch (err) {
      setShowDeleteConfirm(false);
      setDeleteError(err instanceof Error ? err.message : "Unknown error occurred");
    } finally {
      setDeleting(false);
    }
  };

  const handleUpdateProperties = useCallback(async (
    nodeId: string,
    props: Record<string, unknown>,
  ) => {
    return await api.updateNodeProperties(nodeId, props, selectedDatabase ?? undefined);
  }, [selectedDatabase]);

  const handleRefresh = useCallback(() => {
    if (routeState.view === "query") {
      executeCypher();
    } else if (routeState.view === "search") {
      executeSearch();
    }
  }, [routeState.view, executeCypher, executeSearch]);

  const sidebarContent = (
    <>
      {routeState.view === "query" && (
        <QueryPanel
          cypherQuery={cypherQuery}
          setCypherQuery={setCypherQuery}
          queryHistory={queryHistory}
          queryLoading={queryLoading}
          queryError={queryError}
          cypherResult={cypherResult}
          cypherResults={cypherResults}
          selectedNodeIds={selectedNodeIds}
          deleteError={deleteError}
          onExecute={(continueOnError) => executeCypher({ continueOnError })}
          onNodeSelect={(nodeData) => {
            setSelectedNode({
              node: { ...nodeData, created_at: "" },
              score: 0,
            });
          }}
          onToggleSelect={toggleNodeSelection}
          onSelectAll={(nodeIds) => selectAllNodes(nodeIds)}
          onClearSelection={clearNodeSelection}
          onDeleteClick={() => {
            setDeleteError(null);
            setShowDeleteConfirm(true);
          }}
          onExploreSelection={handleGraphSelectionHandoff}
          deleting={deleting}
        />
      )}

      {routeState.view === "search" && (
        <SearchPanel
          searchQuery={searchQuery}
          setSearchQuery={setSearchQuery}
          searchLoading={searchLoading}
          searchError={searchError}
          searchResults={searchResults}
          selectedDatabase={selectedDatabase ?? ""}
          selectedNodeIds={selectedNodeIds}
          selectedNode={selectedNode}
          deleteError={deleteError}
          expandedSimilar={expandedSimilar}
          onExecute={executeSearch}
          onNodeSelect={setSelectedNode}
          onToggleSelect={toggleNodeSelection}
          onSelectAll={(nodeIds) => selectAllNodes(nodeIds)}
          onClearSelection={clearNodeSelection}
          onDeleteClick={() => {
            setDeleteError(null);
            setShowDeleteConfirm(true);
          }}
          onExploreSelection={handleGraphSelectionHandoff}
          onFindSimilar={findSimilar}
          onCollapseSimilar={collapseSimilar}
          deleting={deleting}
        />
      )}
    </>
  );

  return (
    <div className="h-screen bg-norse-night flex flex-col overflow-hidden">
      <Header
        stats={stats}
        connected={connected}
        embedData={embedData}
        embedTriggering={embedTriggering}
        embedMessage={embedMessage}
        onRegenerateClick={() => setShowRegenerateConfirm(true)}
        onAIChatClick={() => setShowAIChat(true)}
        onSecurityClick={() => navigate("/security")}
      />

      <div className="flex-1 min-h-0 flex overflow-hidden">
        <div className="w-full max-w-[60rem] border-r border-norse-rune flex flex-col min-h-0 bg-norse-night">
          <div className="flex items-center gap-2 px-4 py-2 border-b border-norse-rune bg-norse-shadow/30 shrink-0">
            <Database className="w-4 h-4 text-norse-silver shrink-0" aria-hidden />
            <label htmlFor={databaseSelectId} className="text-sm text-norse-silver shrink-0">
              Database
            </label>
            <select
              id={databaseSelectId}
              value={selectedDatabase ?? ""}
              onChange={(e) => handleDatabaseChange(e.target.value)}
              className="flex-1 min-w-0 px-3 py-1.5 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
              title="Cypher queries and semantic search run against this database"
            >
              <option value="">Default (from server)</option>
              {databaseList.map((name: string) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>

          <div className="flex border-b border-norse-rune shrink-0">
            <button
              type="button"
              onClick={() => handleViewChange("query")}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                routeState.view === "query"
                  ? "text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50"
                  : "text-norse-silver hover:text-white"
              }`}
            >
              <Terminal className="w-4 h-4" />
              Cypher Query
            </button>
            <button
              type="button"
              onClick={() => handleViewChange("search")}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                routeState.view === "search"
                  ? "text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50"
                  : "text-norse-silver hover:text-white"
              }`}
            >
              <Sparkles className="w-4 h-4" />
              Semantic Search
            </button>
            <button
              type="button"
              onClick={() => handleViewChange("graph")}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                routeState.view === "graph"
                  ? "text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50"
                  : "text-norse-silver hover:text-white"
              }`}
            >
              <Database className="w-4 h-4" />
              Graph Explorer
            </button>
          </div>

          <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
            {routeState.view === "graph" ? (
              <GraphExplorerControls
                handoff={routeState.graph}
                selectedDatabase={selectedDatabase}
                resolvedDatabase={graphControls?.resolvedDatabase ?? null}
                graph={graphControls?.graph ?? null}
                loading={graphControls?.loading ?? false}
                selectedNodeId={selectedNode?.node.id}
                selectedNodeIds={selectedNodeIdsArray}
                filters={graphControls?.filters ?? { label: null, relationshipType: null }}
                depth={graphControls?.depth ?? 1}
                layoutMode={graphControls?.layoutMode ?? "radial"}
                labels={graphControls?.labels ?? []}
                relationshipTypes={graphControls?.relationshipTypes ?? []}
                onRefresh={graphControls?.onRefresh ?? (() => {})}
                onClearGraph={handleClearGraph}
                onUpdateHandoff={handleUpdateHandoff}
                onFiltersChange={graphControls?.onFiltersChange ?? (() => {})}
                onDepthChange={graphControls?.onDepthChange ?? (() => {})}
                onLayoutChange={graphControls?.onLayoutChange ?? (() => {})}
                onManualSeed={(nodeIds) => handleUpdateHandoff({ mode: "neighborhood", nodeIds })}
              />
            ) : (
              sidebarContent
            )}
          </div>
        </div>

        <div className="flex-1 min-w-0 min-h-0 flex overflow-hidden bg-norse-shadow/30">
          {routeState.view === "graph" ? (
            <GraphExplorerPanel
              handoff={routeState.graph}
              selectedDatabase={selectedDatabase}
              selectedNodeId={selectedNode?.node.id}
              selectedNodeIds={selectedNodeIdsArray}
              selectedNode={selectedNode}
              onNodeSelect={setSelectedNode}
              onExploreNode={handleExploreNode}
              onClearGraph={handleClearGraph}
              onUpdateHandoff={handleUpdateHandoff}
              onCloseDetails={handleCloseDetails}
              onFindSimilar={findSimilar}
              onCollapseSimilar={collapseSimilar}
              expandedSimilar={expandedSimilar}
              onUpdateProperties={handleUpdateProperties}
              onExposeControls={handleExposeControls}
            />
          ) : (
            <div className="flex-1 min-w-0 min-h-0 flex flex-col overflow-hidden">
              <NodeDetailsPanel
                selectedNode={selectedNode}
                expandedSimilar={expandedSimilar}
                onClose={() => setSelectedNode(null)}
                onFindSimilar={findSimilar}
                onCollapseSimilar={collapseSimilar}
                onNodeSelect={setSelectedNode}
                onExploreNode={(nodeId) => handleExploreNeighborhood([nodeId], nodeId)}
                onUpdateProperties={handleUpdateProperties}
                onRefresh={handleRefresh}
              />
            </div>
          )}
        </div>
      </div>

      <Bifrost isOpen={showAIChat} onClose={() => setShowAIChat(false)} />

      <RegenerateConfirmModal
        isOpen={showRegenerateConfirm}
        totalEmbeddings={embedData.totalEmbeddings}
        onConfirm={() => {
          setShowRegenerateConfirm(false);
          handleTriggerEmbed();
        }}
        onCancel={() => setShowRegenerateConfirm(false)}
      />

      <DeleteConfirmModal
        isOpen={showDeleteConfirm}
        nodeCount={selectedNodeIds.size}
        deleting={deleting}
        onConfirm={handleDeleteNodes}
        onCancel={() => {
          setShowDeleteConfirm(false);
          setDeleteError(null);
        }}
      />
    </div>
  );
}
