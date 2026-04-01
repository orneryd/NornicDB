import { useEffect, useId, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Terminal, Sparkles, Database } from "lucide-react";
import { useAppStore } from "../store/appStore";
import { Bifrost } from "../../Bifrost";
import { api } from "../utils/api";
import { Header } from "../components/browser/Header";
import { QueryPanel } from "../components/browser/QueryPanel";
import { SearchPanel } from "../components/browser/SearchPanel";
import { NodeDetailsPanel } from "../components/browser/NodeDetailsPanel";
import { DeleteConfirmModal } from "../components/modals/DeleteConfirmModal";
import { RegenerateConfirmModal } from "../components/modals/RegenerateConfirmModal";
import { BASE_PATH, joinBasePath } from "../utils/basePath";
import {
  mergeBrowserUrlState,
  readBrowserUrlState,
  type BrowserTab,
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
  const routeState = readBrowserUrlState(searchParams);
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

  const [activeTab, setActiveTab] = useState<BrowserTab>(routeState.tab);
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
    if (activeTab !== routeState.tab) {
      setActiveTab(routeState.tab);
    }
  }, [activeTab, routeState.tab]);

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

  const handleDatabaseChange = (dbName: string) => {
    const value = dbName === "" ? null : dbName;
    setSelectedDatabase(value);
    updateRouteState({ database: value });
  };

  const handleTabChange = (tab: BrowserTab) => {
    setActiveTab(tab);
    updateRouteState({ tab });
  };

  const handleDeleteNodes = async () => {
    setDeleting(true);
    setDeleteError(null);
    try {
      const result = await api.deleteNodes(Array.from(selectedNodeIds));
      setShowDeleteConfirm(false);

      if (result.success) {
        clearNodeSelection();
        if (activeTab === "query") {
          executeCypher();
        } else {
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

  const handleUpdateProperties = async (
    nodeId: string,
    props: Record<string, unknown>,
  ) => {
    return await api.updateNodeProperties(nodeId, props, selectedDatabase ?? undefined);
  };

  const handleRefresh = () => {
    if (activeTab === "query") {
      executeCypher();
    } else {
      executeSearch();
    }
  };

  return (
    <div className="min-h-screen bg-norse-night flex flex-col">
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

      <div className="flex-1 flex">
        <div className="w-1/2 border-r border-norse-rune flex flex-col">
          <div className="flex items-center gap-2 px-4 py-2 border-b border-norse-rune bg-norse-shadow/30">
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

          <div className="flex border-b border-norse-rune">
            <button
              type="button"
              onClick={() => handleTabChange("query")}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                activeTab === "query"
                  ? "text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50"
                  : "text-norse-silver hover:text-white"
              }`}
            >
              <Terminal className="w-4 h-4" />
              Cypher Query
            </button>
            <button
              type="button"
              onClick={() => handleTabChange("search")}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                activeTab === "search"
                  ? "text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50"
                  : "text-norse-silver hover:text-white"
              }`}
            >
              <Sparkles className="w-4 h-4" />
              Semantic Search
            </button>
          </div>

          {activeTab === "query" && (
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
              deleting={deleting}
            />
          )}

          {activeTab === "search" && (
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
              onFindSimilar={findSimilar}
              onCollapseSimilar={collapseSimilar}
              deleting={deleting}
            />
          )}
        </div>

        <div className="w-1/2 flex flex-col bg-norse-shadow/30">
          <NodeDetailsPanel
            selectedNode={selectedNode}
            expandedSimilar={expandedSimilar}
            onClose={() => setSelectedNode(null)}
            onFindSimilar={findSimilar}
            onCollapseSimilar={collapseSimilar}
            onNodeSelect={setSelectedNode}
            onUpdateProperties={handleUpdateProperties}
            onRefresh={handleRefresh}
          />
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
