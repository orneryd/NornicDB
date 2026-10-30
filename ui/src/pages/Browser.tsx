import { useEffect, useId, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Terminal, Sparkles, Database, Share2 } from "lucide-react";
import { useAppStore } from "../store/appStore";
import { Bifrost } from "../../Bifrost";
import { api } from "../utils/api";
import { Header } from "../components/browser/Header";
import { QueryPanel } from "../components/browser/QueryPanel";
import { SearchPanel } from "../components/browser/SearchPanel";
import { GraphExplorerPanel } from "../components/browser/GraphExplorerPanel";
import { NodeDetailsPanel } from "../components/browser/NodeDetailsPanel";
import { DeleteConfirmModal } from "../components/modals/DeleteConfirmModal";
import { RegenerateConfirmModal } from "../components/modals/RegenerateConfirmModal";
import { BASE_PATH, joinBasePath } from "../utils/basePath";
import type {
  GraphEdgePayload,
  GraphNodePayload,
  SearchResult,
} from "../utils/api";
import { graphNodeToSearchResult } from "../utils/nodeUtils";

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

  const [activeTab, setActiveTab] = useState<"query" | "search" | "graph">(
    "query",
  );
  const [selectedRelationship, setSelectedRelationship] =
    useState<GraphEdgePayload | null>(null);
  const [graphRootNodeId, setGraphRootNodeId] = useState<string | null>(null);
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

  // Fetch embed stats periodically
  useEffect(() => {
    const fetchEmbedStats = async () => {
      try {
        const res = await fetch(
          joinBasePath(BASE_PATH, "/nornicdb/embed/stats"),
        );
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
      const res = await fetch(
        joinBasePath(BASE_PATH, "/nornicdb/embed/trigger?regenerate=true"),
        {
          method: "POST",
        },
      );
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

  // Sync URL ?database= with selected database (apply URL when list is loaded)
  useEffect(() => {
    const dbFromUrl = searchParams.get("database");
    if (
      dbFromUrl &&
      databaseList.length > 0 &&
      databaseList.includes(dbFromUrl)
    ) {
      setSelectedDatabase(dbFromUrl);
    }
  }, [databaseList, searchParams, setSelectedDatabase]);

  const handleDatabaseChange = (dbName: string) => {
    const value = dbName === "" ? null : dbName;
    setSelectedDatabase(value);
    if (value) {
      setSearchParams({ database: value });
    } else {
      setSearchParams({});
    }
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
      setDeleteError(
        err instanceof Error ? err.message : "Unknown error occurred",
      );
    } finally {
      setDeleting(false);
    }
  };

  const handleUpdateProperties = async (
    nodeId: string,
    props: Record<string, unknown>,
  ) => {
    return await api.updateNodeProperties(
      nodeId,
      props,
      selectedDatabase ?? undefined,
    );
  };

  const handleSelectSearchResult = (result: SearchResult | null) => {
    setSelectedRelationship(null);
    setSelectedNode(result);
  };

  const handleSelectNodeData = (nodeData: {
    id: string;
    labels: string[];
    properties: Record<string, unknown>;
  }) => {
    handleSelectSearchResult({
      node: { ...nodeData, created_at: "" },
      score: 0,
    });
  };

  const handleSelectGraphNode = (node: GraphNodePayload) => {
    handleSelectSearchResult(graphNodeToSearchResult(node));
  };

  const handleSelectGraphEdge = (edge: GraphEdgePayload) => {
    setSelectedRelationship(edge);
  };

  const handleActivateGraphTab = () => {
    setActiveTab("graph");
    setSelectedRelationship(null);
    if (selectedNode?.node.id) {
      setGraphRootNodeId(selectedNode.node.id);
    }
  };

  useEffect(() => {
    if (activeTab === "graph" && !graphRootNodeId && selectedNode?.node.id) {
      setGraphRootNodeId(selectedNode.node.id);
    }
  }, [activeTab, graphRootNodeId, selectedNode]);

  const handleRefresh = () => {
    if (activeTab === "query") {
      executeCypher();
    } else if (activeTab === "search") {
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

      {/* Main Content */}
      <div className="flex-1 flex">
        {/* Left Panel - Query/Search */}
        <div className="w-1/2 border-r border-norse-rune flex flex-col">
          {/* Database selector - all queries run against this database */}
          <div className="flex items-center gap-2 px-4 py-2 border-b border-norse-rune bg-norse-shadow/30">
            <Database
              className="w-4 h-4 text-norse-silver shrink-0"
              aria-hidden
            />
            <label
              htmlFor={databaseSelectId}
              className="text-sm text-norse-silver shrink-0"
            >
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
          {/* Tabs */}
          <div className="flex border-b border-norse-rune">
            <button
              type="button"
              onClick={() => setActiveTab("query")}
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
              onClick={() => setActiveTab("search")}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                activeTab === "search"
                  ? "text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50"
                  : "text-norse-silver hover:text-white"
              }`}
            >
              <Sparkles className="w-4 h-4" />
              Semantic Search
            </button>
            <button
              type="button"
              onClick={handleActivateGraphTab}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                activeTab === "graph"
                  ? "text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50"
                  : "text-norse-silver hover:text-white"
              }`}
            >
              <Share2 className="w-4 h-4" />
              Graph Explorer
            </button>
          </div>

          {/* Query Panel */}
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
              onExecute={(continueOnError) =>
                executeCypher({ continueOnError })
              }
              onNodeSelect={handleSelectNodeData}
              onSelectionChange={selectAllNodes}
              onClearSelection={clearNodeSelection}
              onDeleteClick={() => {
                setDeleteError(null);
                setShowDeleteConfirm(true);
              }}
              deleting={deleting}
            />
          )}

          {/* Search Panel */}
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
              onNodeSelect={handleSelectSearchResult}
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

          {activeTab === "graph" && (
            <GraphExplorerPanel
              selectedDatabase={selectedDatabase ?? ""}
              selectedNode={selectedNode}
              rootNodeId={graphRootNodeId}
              selectedRelationship={selectedRelationship}
              onRootNodeChange={setGraphRootNodeId}
              onNodeSelect={handleSelectGraphNode}
              onRelationshipSelect={handleSelectGraphEdge}
            />
          )}
        </div>

        {/* Right Panel - Node Details */}
        <div className="w-1/2 flex flex-col bg-norse-shadow/30">
          <NodeDetailsPanel
            selectedNode={selectedNode}
            selectedRelationship={selectedRelationship}
            expandedSimilar={expandedSimilar}
            onClose={() => {
              if (selectedRelationship) {
                setSelectedRelationship(null);
                return;
              }
              setSelectedNode(null);
            }}
            onFindSimilar={findSimilar}
            onCollapseSimilar={collapseSimilar}
            onNodeSelect={handleSelectSearchResult}
            onUpdateProperties={handleUpdateProperties}
            onRefresh={handleRefresh}
          />
        </div>
      </div>

      {/* AI Assistant Chat */}
      <Bifrost isOpen={showAIChat} onClose={() => setShowAIChat(false)} />

      {/* Regenerate Embeddings Confirmation Dialog */}
      <RegenerateConfirmModal
        isOpen={showRegenerateConfirm}
        totalEmbeddings={embedData.totalEmbeddings}
        onConfirm={() => {
          setShowRegenerateConfirm(false);
          handleTriggerEmbed();
        }}
        onCancel={() => setShowRegenerateConfirm(false)}
      />

      {/* Delete Nodes Confirmation Dialog */}
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
