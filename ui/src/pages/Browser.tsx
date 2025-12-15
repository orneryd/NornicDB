import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  Database, Search, Play, History, Terminal, 
  Network, HardDrive, Clock, Activity, ChevronRight,
  Sparkles, X, Zap, Loader2, MessageCircle, Shield
} from 'lucide-react';
import { useAppStore } from '../store/appStore';
import { Bifrost } from '../../Bifrost';

interface EmbedStats {
  running: boolean;
  processed: number;
  failed: number;
}

interface EmbedData {
  stats: EmbedStats | null;
  totalEmbeddings: number;
  enabled: boolean;
}

export function Browser() {
  const {
    stats, connected, fetchStats,
    cypherQuery, setCypherQuery, cypherResult, executeCypher, queryLoading, queryError, queryHistory,
    searchQuery, setSearchQuery, searchResults, executeSearch, searchLoading,
    selectedNode, setSelectedNode, findSimilar,
    expandedSimilar, collapseSimilar,
  } = useAppStore();
  
  const [activeTab, setActiveTab] = useState<'query' | 'search'>('query');
  const [showHistory, setShowHistory] = useState(false);
  const [embedData, setEmbedData] = useState<EmbedData>({ stats: null, totalEmbeddings: 0, enabled: false });
  const [embedTriggering, setEmbedTriggering] = useState(false);
  const [embedMessage, setEmbedMessage] = useState<string | null>(null);
  const [showAIChat, setShowAIChat] = useState(false);
  const [showRegenerateConfirm, setShowRegenerateConfirm] = useState(false);
  const navigate = useNavigate();

  // Fetch embed stats periodically
  useEffect(() => {
    const fetchEmbedStats = async () => {
      try {
        const res = await fetch('/nornicdb/embed/stats');
        if (res.ok) {
          const data = await res.json();
          setEmbedData({
            stats: data.stats || null,
            totalEmbeddings: data.total_embeddings || 0,
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
      // Use regenerate=true to clear existing embeddings and regenerate all
      const res = await fetch('/nornicdb/embed/trigger?regenerate=true', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        setEmbedMessage(data.message);
        if (data.stats) {
          setEmbedData(prev => ({ ...prev, stats: data.stats }));
        }
      } else {
        setEmbedMessage(data.message || 'Failed to trigger embeddings');
      }
    } catch (err) {
      setEmbedMessage('Error triggering embeddings');
    } finally {
      setEmbedTriggering(false);
      // Clear message after 5 seconds (longer for regenerate)
      setTimeout(() => setEmbedMessage(null), 5000);
    }
  };

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
            {/* NornicDB Logo - Interwoven threads with gold nexus */}
            <svg viewBox="0 0 200 180" width="44" height="40" className="flex-shrink-0" role="img" aria-hidden="true">
              {/* Three interwoven threads */}
              <path d="M 40 140 Q 30 100 50 70 Q 70 40 100 35 Q 130 30 145 55 Q 155 75 140 90"
                    fill="none" stroke="#4a9eff" strokeWidth="12" strokeLinecap="round" opacity="0.9"/>
              <path d="M 100 25 Q 100 50 85 75 Q 70 100 85 120 Q 100 140 100 165"
                    fill="none" stroke="#4a9eff" strokeWidth="12" strokeLinecap="round"/>
              <path d="M 160 140 Q 170 100 150 70 Q 130 40 100 35 Q 70 30 55 55 Q 45 75 60 90"
                    fill="none" stroke="#4a9eff" strokeWidth="12" strokeLinecap="round" opacity="0.9"/>
              {/* Central nexus - solid gold colors without gradient for simplicity */}
              <circle cx="100" cy="85" r="12" fill="#d4af37"/>
              <circle cx="100" cy="85" r="8" fill="#141824"/>
              <circle cx="100" cy="85" r="5" fill="#d4af37"/>
              {/* Destiny nodes */}
              <circle cx="55" cy="65" r="5" fill="#d4af37" opacity="0.8"/>
              <circle cx="145" cy="65" r="5" fill="#d4af37" opacity="0.8"/>
              <circle cx="100" cy="140" r="5" fill="#d4af37" opacity="0.8"/>
              {/* Connecting lines */}
              <line x1="60" y1="67" x2="93" y2="82" stroke="#d4af37" strokeWidth="1.5" opacity="0.4"/>
              <line x1="140" y1="67" x2="107" y2="82" stroke="#d4af37" strokeWidth="1.5" opacity="0.4"/>
              <line x1="100" y1="135" x2="100" y2="92" stroke="#d4af37" strokeWidth="1.5" opacity="0.4"/>
            </svg>
            <div>
              <h1 className="text-lg font-semibold text-white">NornicDB</h1>
              <p className="text-xs text-norse-silver">The Graph Database That Learns</p>
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
             <button
              type="button"
              onClick={() => setShowRegenerateConfirm(true)}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30`}
              title={'Warning: This will clear and regenerate ALL embeddings'}
            >
              <Zap className="w-4 h-4" />
              <span>Regenerate all Embeddings</span>
            </button>
            {/* Embed Button */}
            <button
              type="button"
              disabled={embedTriggering}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                embedData.stats?.running 
                  ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30' 
                  : 'bg-norse-shadow hover:bg-norse-rune text-norse-silver hover:text-white border border-norse-rune'
              }`}
              title={`Total embeddings: ${embedData.totalEmbeddings}${embedData.stats ? `, Session: ${embedData.stats.processed} processed, ${embedData.stats.failed} failed` : ''}`}
            >
              {embedTriggering || embedData.stats?.running ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Zap className="w-4 h-4" />
              )}
              <span>
                {embedData.stats?.running ? 'Embedding...' : 'Embeddings'}
              </span>
              <span className="text-xs text-valhalla-gold">({embedData.totalEmbeddings.toLocaleString()})</span>
            </button>

            <button
              type="button"
              onClick={() => setShowAIChat(true)}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all bg-valhalla-gold/20 hover:bg-valhalla-gold/30 text-valhalla-gold border border-valhalla-gold/30"
              title="Open AI Assistant"
            >
              <MessageCircle className="w-4 h-4" />
              <span>AI Assistant</span>
            </button>

            <button
              type="button"
              onClick={() => navigate('/security')}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all bg-norse-shadow hover:bg-norse-rune text-norse-silver hover:text-white border border-norse-rune"
              title="Security & API Tokens"
            >
              <Shield className="w-4 h-4" />
              <span>Security</span>
            </button>

            <div className={`flex items-center gap-2 px-3 py-1 rounded-full ${connected ? 'bg-nornic-primary/20 status-connected' : 'bg-red-500/20'}`}>
              <Activity className={`w-4 h-4 ${connected ? 'text-nornic-primary' : 'text-red-400'}`} />
              <span className={`text-sm ${connected ? 'text-nornic-primary' : 'text-red-400'}`}>
                {connected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
          </div>
        </div>
        {/* Embed Message Toast */}
        {embedMessage && (
          <div className="absolute top-16 right-4 bg-norse-shadow border border-norse-rune rounded-lg px-4 py-2 text-sm text-norse-silver shadow-lg">
            {embedMessage}
          </div>
        )}
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
                  <div key={result.node.id}>
                    {/* Main result card */}
                    <button
                      type="button"
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
                    
                    {/* Inline Similar Expansion */}
                    {expandedSimilar?.nodeId === result.node.id && (
                      <div className="ml-4 mt-2 mb-3 border-l-2 border-frost-ice/30 pl-3 animate-in slide-in-from-top-2 duration-200">
                        <div className="flex items-center justify-between mb-2">
                          <span className="text-xs font-medium text-frost-ice flex items-center gap-1">
                            <Sparkles className="w-3 h-3" />
                            Similar Items ({expandedSimilar.results.length})
                          </span>
                          <button
                            type="button"
                            onClick={() => collapseSimilar()}
                            className="text-xs text-norse-fog hover:text-white transition-colors"
                          >
                            Close
                          </button>
                        </div>
                        
                        {expandedSimilar.loading ? (
                          <div className="flex items-center gap-2 text-norse-fog text-sm py-2">
                            <Loader2 className="w-4 h-4 animate-spin" />
                            Finding similar...
                          </div>
                        ) : expandedSimilar.results.length === 0 ? (
                          <p className="text-xs text-norse-fog py-2">No similar items found</p>
                        ) : (
                          <div className="space-y-1">
                            {expandedSimilar.results.map((similar) => (
                              <button
                                type="button"
                                key={similar.node.id}
                                onClick={() => setSelectedNode(similar)}
                                className="w-full text-left p-2 rounded bg-norse-shadow/50 hover:bg-norse-shadow border border-transparent hover:border-frost-ice/20 transition-colors"
                              >
                                <div className="flex items-center justify-between">
                                  <div className="flex items-center gap-1">
                                    {similar.node.labels.slice(0, 2).map((label) => (
                                      <span key={label} className="px-1.5 py-0.5 text-xs bg-frost-ice/10 text-frost-ice/80 rounded">
                                        {label}
                                      </span>
                                    ))}
                                  </div>
                                  <span className="text-xs text-valhalla-gold/70">
                                    {similar.score.toFixed(2)}
                                  </span>
                                </div>
                                <p className="text-xs text-norse-silver/80 truncate mt-1">
                                  {getNodePreview(similar.node.properties)}
                                </p>
                              </button>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
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
                    onClick={() => {
                      // Toggle: if already expanded for this node, collapse
                      if (expandedSimilar?.nodeId === selectedNode.node.id) {
                        collapseSimilar();
                      } else {
                        findSimilar(selectedNode.node.id);
                      }
                    }}
                    className={`flex items-center gap-1 px-3 py-1 text-sm rounded transition-colors ${
                      expandedSimilar?.nodeId === selectedNode.node.id
                        ? 'bg-frost-ice text-norse-night hover:bg-frost-ice/90'
                        : 'bg-frost-ice/20 text-frost-ice hover:bg-frost-ice/30'
                    }`}
                  >
                    <Sparkles className="w-3 h-3" />
                    {expandedSimilar?.nodeId === selectedNode.node.id ? 'Hide Similar' : 'Find Similar'}
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
                    {(selectedNode.node.labels as string[]).map((label, i) => (
                      <span key={`label-${i}`} className="px-3 py-1 bg-frost-ice/20 text-frost-ice rounded-full text-sm">
                        {String(label)}
                      </span>
                    ))}
                  </div>
                </div>

                {/* ID */}
                <div className="mb-4">
                  <h3 className="text-xs font-medium text-norse-silver mb-2">ID</h3>
                  <code className="text-sm text-valhalla-gold font-mono">{selectedNode.node.id}</code>
                </div>

                {/* Embedding Status - Always show at top */}
                {'embedding' in selectedNode.node.properties && selectedNode.node.properties.embedding != null && (
                  <div className="mb-4">
                    <h3 className="text-xs font-medium text-norse-silver mb-2">EMBEDDING</h3>
                    <EmbeddingStatus embedding={selectedNode.node.properties.embedding} />
                  </div>
                )}

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

                {/* Properties (excluding embedding - shown above) */}
                <div className="mb-4">
                  <h3 className="text-xs font-medium text-norse-silver mb-2">PROPERTIES</h3>
                  <div className="space-y-2">
                    {Object.entries(selectedNode.node.properties)
                      .filter(([key]) => key !== 'embedding')
                      .map(([key, value]) => (
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

                {/* Similar Items Section - Shows when Find Similar is clicked */}
                {expandedSimilar?.nodeId === selectedNode.node.id && (
                  <div className="border-t border-norse-rune pt-4 animate-in slide-in-from-bottom-2 duration-200">
                    <div className="flex items-center gap-2 mb-3">
                      <Sparkles className="w-4 h-4 text-frost-ice" />
                      <h3 className="text-sm font-medium text-frost-ice">Similar Items</h3>
                      {!expandedSimilar.loading && (
                        <span className="text-xs text-norse-fog">({expandedSimilar.results.length} found)</span>
                      )}
                    </div>
                    
                    {expandedSimilar.loading ? (
                      <div className="flex items-center gap-2 text-norse-fog py-4">
                        <Loader2 className="w-5 h-5 animate-spin" />
                        <span>Finding similar nodes...</span>
                      </div>
                    ) : expandedSimilar.results.length === 0 ? (
                      <div className="text-center py-4 text-norse-fog">
                        <p>No similar items found</p>
                        <p className="text-xs mt-1">This node may not have an embedding yet</p>
                      </div>
                    ) : (
                      <div className="space-y-2">
                        {expandedSimilar.results.map((similar) => (
                          <button
                            type="button"
                            key={similar.node.id}
                            onClick={() => setSelectedNode(similar)}
                            className="w-full text-left p-3 rounded-lg bg-norse-stone hover:bg-norse-shadow border border-transparent hover:border-frost-ice/30 transition-colors"
                          >
                            <div className="flex items-center justify-between mb-2">
                              <div className="flex items-center gap-2 flex-wrap">
                                {similar.node.labels.slice(0, 3).map((label) => (
                                  <span key={label} className="px-2 py-0.5 text-xs bg-frost-ice/20 text-frost-ice rounded">
                                    {label}
                                  </span>
                                ))}
                              </div>
                              <span className="text-xs text-valhalla-gold font-medium">
                                {(similar.score * 100).toFixed(1)}% similar
                              </span>
                            </div>
                            <p className="text-sm text-norse-silver line-clamp-2">
                              {getNodePreview(similar.node.properties)}
                            </p>
                            <p className="text-xs text-norse-fog mt-1 font-mono">
                              {similar.node.id}
                            </p>
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                )}
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

      {/* AI Assistant Chat */}
      <Bifrost isOpen={showAIChat} onClose={() => setShowAIChat(false)} />

      {/* Regenerate Embeddings Confirmation Dialog */}
      {showRegenerateConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
          <div className="bg-norse-deep border border-norse-rune rounded-xl p-6 max-w-md mx-4 shadow-2xl">
            <div className="flex items-center gap-3 mb-4">
              <div className="p-2 bg-red-500/20 rounded-lg">
                <Zap className="w-6 h-6 text-red-400" />
              </div>
              <h3 className="text-lg font-semibold text-white">Regenerate All Embeddings?</h3>
            </div>
            <p className="text-norse-silver mb-2">
              This will <span className="text-red-400 font-medium">clear all existing embeddings</span> and regenerate them from scratch.
            </p>
            <p className="text-norse-silver text-sm mb-6">
              This operation runs in the background. You have <span className="text-valhalla-gold">{embedData.totalEmbeddings.toLocaleString()}</span> embeddings that will be regenerated.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                type="button"
                onClick={() => setShowRegenerateConfirm(false)}
                className="px-4 py-2 rounded-lg text-norse-silver hover:text-white hover:bg-norse-rune transition-all"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowRegenerateConfirm(false);
                  handleTriggerEmbed();
                }}
                className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 rounded-lg font-medium transition-all"
              >
                Yes, Regenerate All
              </button>
            </div>
          </div>
        </div>
      )}
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
    if (expanded) {
      return (
        <pre className="text-xs text-norse-silver bg-norse-shadow/50 rounded p-2 overflow-x-auto max-h-48 overflow-y-auto">
          {JSON.stringify(data, null, 2)}
        </pre>
      );
    }
    return <span className="text-norse-silver">[{data.length} items]</span>;
  }
  if (typeof data === 'object') {
    if (expanded) {
      return (
        <pre className="text-xs text-norse-silver bg-norse-shadow/50 rounded p-2 overflow-x-auto max-h-48 overflow-y-auto">
          {JSON.stringify(data, null, 2)}
        </pre>
      );
    }
    const keys = Object.keys(data);
    return <span className="text-norse-silver">{'{'}...{keys.length} props{'}'}</span>;
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

// Display embedding status nicely
function EmbeddingStatus({ embedding }: { embedding: unknown }) {
  if (!embedding || typeof embedding !== 'object') {
    return <span className="text-norse-silver">No embedding data</span>;
  }
  
  const emb = embedding as Record<string, unknown>;
  const status = emb.status as string || 'unknown';
  const dimensions = emb.dimensions as number || 0;
  const model = emb.model as string | undefined;
  
  const isReady = status === 'ready';
  const isPending = status === 'pending';
  
  return (
    <div className="bg-norse-stone rounded-lg p-3">
      <div className="flex items-center gap-3">
        {/* Status indicator */}
        <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full ${
          isReady 
            ? 'bg-nornic-primary/20' 
            : isPending 
              ? 'bg-valhalla-gold/20' 
              : 'bg-red-500/20'
        }`}>
          <div className={`w-2 h-2 rounded-full ${
            isReady 
              ? 'bg-nornic-primary animate-pulse' 
              : isPending 
                ? 'bg-valhalla-gold animate-pulse' 
                : 'bg-red-400'
          }`} />
          <span className={`text-sm font-medium ${
            isReady 
              ? 'text-nornic-primary' 
              : isPending 
                ? 'text-valhalla-gold' 
                : 'text-red-400'
          }`}>
            {isReady ? 'Ready' : isPending ? 'Generating...' : status}
          </span>
        </div>
        
        {/* Dimensions */}
        {dimensions > 0 && (
          <div className="flex items-center gap-1">
            <span className="text-xs text-norse-silver">Dimensions:</span>
            <span className="text-sm text-frost-ice font-mono">{dimensions}</span>
          </div>
        )}
        
        {/* Model */}
        {model && (
          <div className="flex items-center gap-1">
            <span className="text-xs text-norse-silver">Model:</span>
            <span className="text-sm text-nornic-accent">{model}</span>
          </div>
        )}
      </div>
      
      {isPending && (
        <p className="text-xs text-norse-fog mt-2">
          Embedding will be generated automatically by the background queue.
        </p>
      )}
    </div>
  );
}
