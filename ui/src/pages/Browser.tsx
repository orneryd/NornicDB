import { useEffect, useId, useState, useMemo } from "react";
import { useSearchParams, useParams, useNavigate } from "react-router-dom";
import { Terminal, Sparkles, Database, Share2 } from "lucide-react";
import { useAppStore } from "../store/appStore";
import { Bifrost } from "../../Bifrost";
import { api } from "../utils/api";
import { DatabaseSummary } from "../components/browser/DatabaseSummary";
import { useUrlState, useViewState } from "../utils/useUrlState";
import {graphFromQuery} from "../utils/queryGraph";
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

export function Browser({maintenance = false}: {maintenance?: boolean}) {
  const {params, patch} = useUrlState();
  const route = useParams();
  const navigate = useNavigate();
  const [resolutionError, setResolutionError] = useState<string | null>(null);
  const [searchParams] = useSearchParams();
  const {
    stats,
    connected,
    fetchStats,
    fetchDatabases,
    databaseList,
    selectedDatabase,
    setSelectedDatabase,
    setCypherQuery: storeCypherQuery,
    cypherResult,
    cypherResults,
    executeCypher: runCypher,
    queryLoading,
    queryError,
    queryHistory,
    setSearchQuery: storeSearchQuery,
    searchResults,
    executeSearch: runSearch,
    searchLoading,
    searchError,
    selectedNode,
    setSelectedNode,
    findSimilar: loadSimilar,
    expandedSimilar,
    collapseSimilar: clearSimilar,
  } = useAppStore();
  const queryGraph=useMemo(()=>graphFromQuery(cypherResult,selectedDatabase??''),[cypherResult,selectedDatabase]);
  const [cypherQuery,setCypherQuery]=useViewState('query','MATCH (n) RETURN n LIMIT 25');
  const [searchQuery,setSearchQuery]=useViewState('q','');
  const executeCypher=(options?:{continueOnError?:boolean})=>{storeCypherQuery(cypherQuery);return runCypher(options)};
  const executeSearch=()=>{storeSearchQuery(searchQuery);return runSearch()};
  const [selection,setSelection] = useViewState<string[]>('selected',[]);
  const selectedNodeIds=useMemo(()=>new Set(selection),[JSON.stringify(selection)]);
  const toggleNodeSelection=(id:string)=>setSelection(previous=>previous.includes(id)?previous.filter(value=>value!==id):[...previous,id]);
  const selectAllNodes=(ids:string[])=>setSelection(ids);
  const clearNodeSelection=()=>setSelection([]);

  const similarId=params.get('similar');
  const findSimilar=(id:string)=>patch({similar:id});
  const collapseSimilar=()=>patch({similar:null});
  useEffect(()=>{
    if(similarId)void loadSimilar(similarId);else clearSimilar();
    return ()=>clearSimilar();
  },[similarId,selectedDatabase,loadSimilar,clearSimilar]);

  const activeTab = params.get('tab') === 'search' ? 'search' : params.get('tab') === 'graph' ? 'graph' : 'query';
  const setActiveTab = (tab: string) => patch({tab: tab === 'query' ? null : tab});
  const [selectedRelationship, resolveRelationship] =
    useState<GraphEdgePayload | null>(null);
  const setSelectedRelationship=(edge:GraphEdgePayload|null)=>{resolveRelationship(edge);patch({'graph.edge':edge?.id??null})};
  const [graphRootNodeId, setGraphRootNodeId] = useViewState<string | null>('graph.root',null);
  const [embedData, setEmbedData] = useState<EmbedData>({
    stats: null,
    totalEmbeddings: 0,
    pendingNodes: 0,
    enabled: false,
  });
  const [embedTriggering, setEmbedTriggering] = useState(false);
  const [embedMessage, setEmbedMessage] = useState<string | null>(null);
  const showAIChat = params.get('assistant') === 'open';
  const setShowAIChat = (open: boolean) => patch({assistant: open ? 'open' : null});
  const showRegenerateConfirm = params.get('confirm') === 'regenerate';
  const setShowRegenerateConfirm = (open: boolean) => patch({confirm: open ? 'regenerate' : null});
  const showDeleteConfirm=params.get('confirm')==='delete';
  const setShowDeleteConfirm=(open:boolean)=>patch({confirm:open?'delete':null});
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
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
    if(!selectedDatabase){setEmbedMessage('Choose a database before regenerating embeddings.');return;}
    setEmbedTriggering(true);
    setEmbedMessage(null);
    try {
      const res = await fetch(
        joinBasePath(BASE_PATH, `/nornicdb/embed/trigger?regenerate=true&database=${encodeURIComponent(selectedDatabase)}`),
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
    setSelectedDatabase(route.database || searchParams.get("database") || null);
  }, [route.database, searchParams, setSelectedDatabase]);

  useEffect(()=>{
    if(maintenance)return;
    let current = true;
    setResolutionError(null);
    setSelectedNode(null);
    if(route.nodeId) api.getGraphNeighborhood({nodeIds:[route.nodeId],depth:0,limit:1,database:route.database})
      .then(result=>{if(current){const node=result.nodes.find(n=>n.id===route.nodeId);if(node)setSelectedNode(graphNodeToSearchResult(node));else setResolutionError('This node was not found in the selected database.');}})
      .catch(error=>{if(current)setResolutionError(error.message)});
    return ()=>{current=false};
  },[route.database,route.nodeId,maintenance,setSelectedNode]);

  const handleDatabaseChange = (dbName: string) => {
    const value = dbName === "" ? null : dbName;
    setSelectedDatabase(value);
    const next = new URLSearchParams(params);
    next.delete('database'); next.delete('results.expanded');
    next.delete('selected');next.delete('similar');next.delete('graph.root');next.delete('grid.query-results-grid');
    navigate({pathname:value ? `/workspace/${encodeURIComponent(value)}` : '/',search:next.toString()});
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
    void api.getResolvedDatabaseName(selectedDatabase ?? undefined).then(database=>navigate({pathname:`/workspace/${encodeURIComponent(database)}${result ? `/nodes/${encodeURIComponent(result.node.id)}` : ''}`,search:params.toString()}));
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
    if (!queryGraph.nodes.length&&selectedNode?.node.id) {
      setGraphRootNodeId(selectedNode.node.id);
    }
  };

  useEffect(() => {
    if (activeTab === "graph" && !queryGraph.nodes.length && !graphRootNodeId && selectedNode?.node.id) {
      setGraphRootNodeId(selectedNode.node.id);
    }
  }, [activeTab, graphRootNodeId, selectedNode,queryGraph.nodes.length]);

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
        onAIChatClick={() => setShowAIChat(true)}
      />

      {maintenance ? <section className="max-w-4xl mx-auto w-full p-6 space-y-6">
        <h1 className="text-2xl font-semibold text-white">Embeddings</h1>
        <p className="text-norse-silver">Worker statistics below are server-wide. Regeneration targets only the database you choose.</p>
        <dl className="grid grid-cols-2 gap-4 text-norse-silver">
          <div><dt>Status</dt><dd>{embedData.stats?.running ? 'Processing' : embedData.enabled ? 'Idle' : 'Disabled'}</dd></div>
          <div><dt>Embeddings</dt><dd>{embedData.totalEmbeddings.toLocaleString()}</dd></div>
          <div><dt>Pending nodes</dt><dd>{embedData.pendingNodes.toLocaleString()}</dd></div>
          <div><dt>Current session</dt><dd>{embedData.stats?.processed ?? 0} processed · {embedData.stats?.failed ?? 0} failed</dd></div>
        </dl>
        <label className="block text-sm text-norse-silver">Database to regenerate
          <select value={selectedDatabase ?? ''} onChange={event=>patch({database:event.target.value || null})} className="block mt-2 w-full rounded-lg bg-norse-stone border border-norse-rune p-2 text-white">
            <option value="">Choose a database</option>
            {databaseList.map(name=><option key={name} value={name}>{name}</option>)}
          </select>
        </label>
        <details open={params.get('maintenance') === 'open'} onToggle={e => {const value=e.currentTarget.open ? 'open' : null;if(value !== params.get('maintenance'))patch({maintenance:value});}} className="rounded-lg border border-norse-rune p-4">
          <summary className="cursor-pointer text-white">Maintenance</summary>
          <p className="my-3 text-norse-silver">Clear managed embeddings in the selected database and regenerate them in the background. Other databases keep their embeddings.</p>
          <button type="button" disabled={embedTriggering || !selectedDatabase} onClick={()=>setShowRegenerateConfirm(true)} className="px-3 py-2 rounded border border-red-500/40 text-red-400 disabled:opacity-50">{embedTriggering ? 'Starting…' : 'Regenerate database embeddings'}</button>
        </details>
        {embedMessage && <p role="status" className="text-norse-silver">{embedMessage}</p>}
      </section> : <>
      {resolutionError && <p role="alert" className="p-4 text-red-400">{resolutionError}</p>}
      {/* Main Content */}
      <div className="flex-1 flex flex-col lg:flex-row">
        {/* Left Panel - Query/Search */}
        <div className={activeTab==='graph'?"flex-1 min-w-0 border-r border-norse-rune flex flex-col":"w-full lg:w-1/2 border-r border-norse-rune flex flex-col"}>
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
          <DatabaseSummary database={selectedDatabase} refreshKey={stats?.server?.uptime_seconds} />
          {/* Tabs */}
          <div role="tablist" aria-label="Workspace mode" className="flex border-b border-norse-rune" onKeyDown={event=>{
            const tabs=['query','search','graph'];let index=tabs.indexOf(activeTab);
            if(event.key==='ArrowRight')index=(index+1)%3;else if(event.key==='ArrowLeft')index=(index+2)%3;else if(event.key==='Home')index=0;else if(event.key==='End')index=2;else return;
            event.preventDefault();setActiveTab(tabs[index]);(event.currentTarget.querySelectorAll('[role=tab]')[index] as HTMLElement)?.focus();
          }}>
            <button
              type="button"
              role="tab" id="workspace-query" aria-controls="workspace-panel" aria-selected={activeTab==='query'} tabIndex={activeTab==='query'?0:-1}
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
              role="tab" id="workspace-search" aria-controls="workspace-panel" aria-selected={activeTab==='search'} tabIndex={activeTab==='search'?0:-1}
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
              role="tab" id="workspace-graph" aria-controls="workspace-panel" aria-selected={activeTab==='graph'} tabIndex={activeTab==='graph'?0:-1}
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
          <div role="tabpanel" id="workspace-panel" aria-labelledby={`workspace-${activeTab}`} className="flex-1 flex flex-col min-h-0">
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
              queryGraph={queryGraph}
              selectedRelationship={selectedRelationship}
              onRootNodeChange={setGraphRootNodeId}
              onNodeSelect={handleSelectGraphNode}
              onRelationshipSelect={handleSelectGraphEdge}
            />
          )}
          </div>
        </div>

        {/* Right Panel - Node Details */}
        <div className={activeTab==='graph'?`w-full lg:w-80 flex flex-col bg-norse-shadow/30 ${selectedNode||selectedRelationship?'':'hidden'}`:"w-full lg:w-1/2 flex flex-col bg-norse-shadow/30"}>
          <NodeDetailsPanel
            selectedNode={selectedNode}
            selectedRelationship={selectedRelationship}
            expandedSimilar={expandedSimilar}
            onClose={() => {
              if (selectedRelationship) {
                setSelectedRelationship(null);
                return;
              }
              handleSelectSearchResult(null);
            }}
            onFindSimilar={findSimilar}
            onCollapseSimilar={collapseSimilar}
            onNodeSelect={handleSelectSearchResult}
            onUpdateProperties={handleUpdateProperties}
            onRefresh={handleRefresh}
          />
        </div>
      </div>

      </>}
      {/* AI Assistant Chat */}
      <Bifrost isOpen={showAIChat} onClose={() => setShowAIChat(false)} />

      {/* Regenerate Embeddings Confirmation Dialog */}
      <RegenerateConfirmModal
        isOpen={showRegenerateConfirm}
        totalEmbeddings={embedData.totalEmbeddings}
        database={selectedDatabase ?? undefined}
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
