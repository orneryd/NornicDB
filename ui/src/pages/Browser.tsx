import { useEffect, useState } from 'react';
import { 
  Database, Search, Play, History, Terminal, 
  Network, HardDrive, Clock, Activity, ChevronRight,
  Sparkles, X
} from 'lucide-react';
import { useAppStore } from '../store/appStore';

export function Browser() {
  const {
    stats, connected, fetchStats,
    cypherQuery, setCypherQuery, cypherResult, executeCypher, queryLoading, queryError, queryHistory,
    searchQuery, setSearchQuery, searchResults, executeSearch, searchLoading,
    selectedNode, setSelectedNode, findSimilar,
  } = useAppStore();
  
  const [activeTab, setActiveTab] = useState<'query' | 'search'>('query');
  const [showHistory, setShowHistory] = useState(false);

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 5000);
    return () => clearInterval(interval);
  }, [fetchStats]);

  const handleQuerySubmit = (e: React.FormEvent) => {
    e.preventDefault();
    executeCypher();
  };

  const handleSearchSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    executeSearch();
  };

  const formatUptime = (seconds: number) => {
    const hours = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${mins}m`;
  };

  return (
    <div className="min-h-screen bg-norse-night flex flex-col">
      {/* Header */}
      <header className="bg-norse-shadow border-b border-norse-rune px-4 py-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-nornic-primary to-nornic-secondary flex items-center justify-center">
              <Database className="w-5 h-5 text-white" />
            </div>
            <div>
              <h1 className="text-lg font-semibold text-white">NornicDB Browser</h1>
              <p className="text-xs text-norse-silver">Neo4j-compatible Graph Database</p>
            </div>
          </div>
          
          {/* Connection Status */}
          <div className="flex items-center gap-6">
            {stats?.database && (
              <>
                <div className="flex items-center gap-2 text-sm">
                  <Network className="w-4 h-4 text-norse-silver" />
                  <span className="text-norse-silver">{stats.database.nodes?.toLocaleString() ?? '?'} nodes</span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                  <HardDrive className="w-4 h-4 text-norse-silver" />
                  <span className="text-norse-silver">{stats.database.edges?.toLocaleString() ?? '?'} edges</span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                  <Clock className="w-4 h-4 text-norse-silver" />
                  <span className="text-norse-silver">{formatUptime(stats.server?.uptime_seconds ?? 0)}</span>
                </div>
              </>
            )}
            <div className={`flex items-center gap-2 px-3 py-1 rounded-full ${connected ? 'bg-nornic-primary/20 status-connected' : 'bg-red-500/20'}`}>
              <Activity className={`w-4 h-4 ${connected ? 'text-nornic-primary' : 'text-red-400'}`} />
              <span className={`text-sm ${connected ? 'text-nornic-primary' : 'text-red-400'}`}>
                {connected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex">
        {/* Left Panel - Query/Search */}
        <div className="w-1/2 border-r border-norse-rune flex flex-col">
          {/* Tabs */}
          <div className="flex border-b border-norse-rune">
            <button
              type="button"
              onClick={() => setActiveTab('query')}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                activeTab === 'query' 
                  ? 'text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50' 
                  : 'text-norse-silver hover:text-white'
              }`}
            >
              <Terminal className="w-4 h-4" />
              Cypher Query
            </button>
            <button
              type="button"
              onClick={() => setActiveTab('search')}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors ${
                activeTab === 'search' 
                  ? 'text-nornic-primary border-b-2 border-nornic-primary bg-norse-shadow/50' 
                  : 'text-norse-silver hover:text-white'
              }`}
            >
              <Sparkles className="w-4 h-4" />
              Semantic Search
            </button>
          </div>

          {/* Query Panel */}
          {activeTab === 'query' && (
            <div className="flex-1 flex flex-col p-4 gap-4">
              <form onSubmit={handleQuerySubmit} className="flex flex-col gap-3">
                <div className="relative">
                  <textarea
                    value={cypherQuery}
                    onChange={(e) => setCypherQuery(e.target.value)}
                    className="cypher-editor w-full h-32 p-3 resize-none"
                    placeholder="MATCH (n) RETURN n LIMIT 25"
                    spellCheck={false}
                  />
                  <button
                    type="button"
                    onClick={() => setShowHistory(!showHistory)}
                    className="absolute top-2 right-2 p-1.5 rounded hover:bg-norse-rune transition-colors"
                    title="Query History"
                  >
                    <History className="w-4 h-4 text-norse-silver" />
                  </button>
                </div>
                
                {showHistory && queryHistory.length > 0 && (
                  <div className="bg-norse-stone border border-norse-rune rounded-lg p-2 max-h-40 overflow-y-auto">
                    {queryHistory.map((q, i) => (
                      <button
                        key={i}
                        type="button"
                        onClick={() => { setCypherQuery(q); setShowHistory(false); }}
                        className="w-full text-left px-2 py-1 text-sm text-norse-silver hover:bg-norse-rune rounded truncate"
                      >
                        {q}
                      </button>
                    ))}
                  </div>
                )}

                <button
                  type="submit"
                  disabled={queryLoading}
                  className="flex items-center justify-center gap-2 px-4 py-2 bg-nornic-primary text-white rounded-lg hover:bg-nornic-secondary disabled:opacity-50 transition-colors"
                >
                  <Play className="w-4 h-4" />
                  {queryLoading ? 'Executing...' : 'Run Query'}
                </button>
              </form>

              {queryError && (
                <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg">
                  <p className="text-sm text-red-400 font-mono">{queryError}</p>
                </div>
              )}

              {/* Query Results */}
              {cypherResult && cypherResult.results[0] && (
                <div className="flex-1 overflow-auto">
                  <table className="result-table">
                    <thead>
                      <tr>
                        {cypherResult.results[0].columns.map((col, i) => (
                          <th key={i}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {cypherResult.results[0].data.map((row, i) => (
                        <tr 
                          key={i}
                          onClick={() => {
                            // Find first node-like object in row and select it
                            for (const cell of row.row) {
                              if (cell && typeof cell === 'object') {
                                const cellObj = cell as Record<string, unknown>;
                                if (cellObj.id || cellObj._nodeId) {
                                  const nodeData = extractNodeFromResult(cellObj);
                                  if (nodeData) {
                                    setSelectedNode({ node: { ...nodeData, created_at: '' }, score: 0 });
                                    break;
                                  }
                                }
                              }
                            }
                          }}
                          className="cursor-pointer hover:bg-nornic-primary/10"
                        >
                          {row.row.map((cell, j) => (
                            <td key={j} className="font-mono text-xs">
                              {typeof cell === 'object' 
                                ? <NodePreview data={cell} />
                                : String(cell)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  <p className="text-xs text-norse-silver mt-2 px-2">
                    {cypherResult.results[0].data.length} row(s) returned
                  </p>
                </div>
              )}
            </div>
          )}

          {/* Search Panel */}
          {activeTab === 'search' && (
            <div className="flex-1 flex flex-col p-4 gap-4">
              <form onSubmit={handleSearchSubmit} className="flex gap-2">
                <div className="relative flex-1">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-norse-fog" />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full pl-10 pr-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white placeholder-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary"
                    placeholder="Search nodes semantically..."
                  />
                </div>
                <button
                  type="submit"
                  disabled={searchLoading}
                  className="px-4 py-2 bg-nornic-primary text-white rounded-lg hover:bg-nornic-secondary disabled:opacity-50 transition-colors"
                >
                  {searchLoading ? '...' : 'Search'}
                </button>
              </form>

              {/* Search Results */}
              <div className="flex-1 overflow-auto space-y-2">
                {searchResults.map((result) => (
                  <button
                    type="button"
                    key={result.node.id}
                    onClick={() => setSelectedNode(result)}
                    className={`w-full text-left p-3 rounded-lg border transition-colors ${
                      selectedNode?.node.id === result.node.id
                        ? 'bg-nornic-primary/20 border-nornic-primary'
                        : 'bg-norse-stone border-norse-rune hover:border-norse-fog'
                    }`}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-2">
                        {result.node.labels.map((label) => (
                          <span key={label} className="px-2 py-0.5 text-xs bg-frost-ice/20 text-frost-ice rounded">
                            {label}
                          </span>
                        ))}
                      </div>
                      <span className="text-xs text-valhalla-gold">
                        Score: {result.score.toFixed(2)}
                      </span>
                    </div>
                    <p className="text-sm text-norse-silver truncate">
                      {getNodePreview(result.node.properties)}
                    </p>
                  </button>
                ))}
                
                {searchResults.length === 0 && searchQuery && !searchLoading && (
                  <p className="text-center text-norse-silver py-8">No results found</p>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Right Panel - Node Details */}
        <div className="w-1/2 flex flex-col bg-norse-shadow/30">
          {selectedNode ? (
            <>
              <div className="flex items-center justify-between p-4 border-b border-norse-rune">
                <h2 className="font-medium text-white">Node Details</h2>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => findSimilar(selectedNode.node.id)}
                    className="flex items-center gap-1 px-3 py-1 text-sm bg-frost-ice/20 text-frost-ice rounded hover:bg-frost-ice/30 transition-colors"
                  >
                    <Sparkles className="w-3 h-3" />
                    Find Similar
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedNode(null)}
                    className="p-1 hover:bg-norse-rune rounded transition-colors"
                  >
                    <X className="w-4 h-4 text-norse-silver" />
                  </button>
                </div>
              </div>
              
              <div className="flex-1 overflow-auto p-4">
                {/* Labels */}
                <div className="mb-4">
                  <h3 className="text-xs font-medium text-norse-silver mb-2">LABELS</h3>
                  <div className="flex flex-wrap gap-2">
                    {selectedNode.node.labels.map((label, i) => (
                      <span key={i} className="px-3 py-1 bg-frost-ice/20 text-frost-ice rounded-full text-sm">
                        {label}
                      </span>
                    ))}
                  </div>
                </div>

                {/* ID */}
                <div className="mb-4">
                  <h3 className="text-xs font-medium text-norse-silver mb-2">ID</h3>
                  <code className="text-sm text-valhalla-gold font-mono">{selectedNode.node.id}</code>
                </div>

                {/* Scores */}
                {(selectedNode.rrf_score || selectedNode.vector_rank || selectedNode.bm25_rank) && (
                  <div className="mb-4 flex gap-4">
                    {selectedNode.rrf_score && (
                      <div>
                        <h3 className="text-xs font-medium text-norse-silver mb-1">RRF Score</h3>
                        <span className="text-nornic-accent">{selectedNode.rrf_score.toFixed(4)}</span>
                      </div>
                    )}
                    {selectedNode.vector_rank && (
                      <div>
                        <h3 className="text-xs font-medium text-norse-silver mb-1">Vector Rank</h3>
                        <span className="text-frost-ice">#{selectedNode.vector_rank}</span>
                      </div>
                    )}
                    {selectedNode.bm25_rank && (
                      <div>
                        <h3 className="text-xs font-medium text-norse-silver mb-1">BM25 Rank</h3>
                        <span className="text-valhalla-gold">#{selectedNode.bm25_rank}</span>
                      </div>
                    )}
                  </div>
                )}

                {/* Properties */}
                <div>
                  <h3 className="text-xs font-medium text-norse-silver mb-2">PROPERTIES</h3>
                  <div className="space-y-2">
                    {Object.entries(selectedNode.node.properties).map(([key, value]) => (
                      <div key={key} className="bg-norse-stone rounded-lg p-3">
                        <div className="flex items-center gap-2 mb-1">
                          <ChevronRight className="w-3 h-3 text-norse-fog" />
                          <span className="text-sm text-frost-ice font-medium">{key}</span>
                        </div>
                        <div className="pl-5">
                          <JsonPreview data={value} expanded />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center">
              <div className="text-center text-norse-silver">
                <Database className="w-12 h-12 mx-auto mb-3 opacity-30" />
                <p>Select a node to view details</p>
                <p className="text-sm text-norse-fog mt-1">
                  Run a query or search to get started
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// Helper components
function JsonPreview({ data, expanded = false }: { data: unknown; expanded?: boolean }) {
  if (data === null) return <span className="json-null">null</span>;
  if (typeof data === 'string') {
    const displayValue = expanded ? data : data.slice(0, 100) + (data.length > 100 ? '...' : '');
    return <span className="json-string whitespace-pre-wrap">"{displayValue}"</span>;
  }
  if (typeof data === 'number') return <span className="json-number">{data}</span>;
  if (typeof data === 'boolean') return <span className="json-boolean">{String(data)}</span>;
  if (Array.isArray(data)) {
    if (!expanded && data.length > 3) {
      return <span className="text-norse-silver">[{data.length} items]</span>;
    }
    return <span className="text-norse-silver">[...]</span>;
  }
  if (typeof data === 'object') {
    const keys = Object.keys(data);
    if (!expanded && keys.length > 3) {
      return <span className="text-norse-silver">{'{'}...{keys.length} props{'}'}</span>;
    }
    return <span className="text-norse-silver">{'{'}...{'}'}</span>;
  }
  return <span>{String(data)}</span>;
}

function getNodePreview(properties: Record<string, unknown>): string {
  const previewFields = ['title', 'name', 'text', 'content', 'description', 'path'];
  for (const field of previewFields) {
    if (properties[field] && typeof properties[field] === 'string') {
      return properties[field] as string;
    }
  }
  return JSON.stringify(properties).slice(0, 100);
}

// Extract node data from Cypher result cell
function extractNodeFromResult(cell: Record<string, unknown>): { id: string; labels: string[]; properties: Record<string, unknown> } | null {
  if (!cell || typeof cell !== 'object') return null;
  
  // Get ID (could be _nodeId, id, or elementId)
  const id = (cell._nodeId || cell.id || cell.elementId) as string;
  if (!id) return null;
  
  // Get labels
  let labels: string[] = [];
  if (Array.isArray(cell.labels)) {
    labels = cell.labels as string[];
  } else if (cell.type && typeof cell.type === 'string') {
    labels = [cell.type];
  }
  
  // Properties are the rest of the fields (excluding metadata)
  const excludeKeys = new Set(['_nodeId', 'id', 'elementId', 'labels', 'meta']);
  const properties: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(cell)) {
    if (!excludeKeys.has(key)) {
      properties[key] = value;
    }
  }
  
  return { id, labels, properties };
}

// Show node preview with ID and title
function NodePreview({ data }: { data: unknown }) {
  if (data === null) return <span className="json-null">null</span>;
  if (typeof data !== 'object') return <span>{String(data)}</span>;
  
  const obj = data as Record<string, unknown>;
  
  // Check if it's a node-like object
  const id = obj._nodeId || obj.id || obj.elementId;
  const title = obj.title || obj.name || obj.text;
  const type = obj.type;
  const labels = obj.labels as string[] | undefined;
  
  if (id) {
    const idStr = String(id);
    const titleStr = title ? String(title) : '';
    return (
      <div className="flex items-center gap-2">
        {labels && labels.length > 0 ? (
          <span className="px-1.5 py-0.5 text-xs bg-frost-ice/20 text-frost-ice rounded">
            {labels[0]}
          </span>
        ) : typeof type === 'string' ? (
          <span className="px-1.5 py-0.5 text-xs bg-nornic-primary/20 text-nornic-primary rounded">
            {type}
          </span>
        ) : null}
        <span className="text-valhalla-gold text-xs">{idStr.slice(0, 20)}</span>
        {titleStr && (
          <span className="text-norse-silver truncate max-w-[200px]">
            {titleStr.slice(0, 50)}{titleStr.length > 50 ? '...' : ''}
          </span>
        )}
      </div>
    );
  }
  
  // Not a node - show key count
  const keys = Object.keys(obj);
  return <span className="text-norse-silver">{'{'}...{keys.length} props{'}'}</span>;
}
