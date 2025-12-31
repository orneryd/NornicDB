import { create } from 'zustand';
import { api, DatabaseStats, SearchResult, CypherResponse } from '../utils/api';

// Similar results for inline expansion
interface SimilarExpansion {
  nodeId: string;
  results: SearchResult[];
  loading: boolean;
}

interface AppState {
  // Auth
  isAuthenticated: boolean;
  username: string | null;
  authLoading: boolean;
  
  // Database
  stats: DatabaseStats | null;
  connected: boolean;
  
  // Query
  cypherQuery: string;
  cypherResult: CypherResponse | null;
  queryLoading: boolean;
  queryError: string | null;
  queryHistory: string[];
  
  // Search
  searchQuery: string;
  searchResults: SearchResult[];
  searchLoading: boolean;
  
  // Selected
  selectedNode: SearchResult | null;
  selectedNodeIds: Set<string>;
  
  // Similar - inline expansion
  expandedSimilar: SimilarExpansion | null;
  
  // Actions
  checkAuth: () => Promise<void>;
  login: (username: string, password: string) => Promise<{ success: boolean; error?: string }>;
  logout: () => Promise<void>;
  fetchStats: () => Promise<void>;
  setCypherQuery: (query: string) => void;
  executeCypher: () => Promise<void>;
  setSearchQuery: (query: string) => void;
  executeSearch: () => Promise<void>;
  setSelectedNode: (node: SearchResult | null) => void;
  toggleNodeSelection: (nodeId: string) => void;
  selectAllNodes: (nodeIds: string[]) => void;
  clearNodeSelection: () => void;
  findSimilar: (nodeId: string) => Promise<void>;
  collapseSimilar: () => void;
}

export const useAppStore = create<AppState>((set, get) => ({
  // Initial state
  isAuthenticated: false,
  username: null,
  authLoading: true,
  stats: null,
  connected: false,
  cypherQuery: 'MATCH (n) RETURN n LIMIT 25',
  cypherResult: null,
  queryLoading: false,
  queryError: null,
  queryHistory: [],
  searchQuery: '',
  searchResults: [],
  searchLoading: false,
  selectedNode: null,
  selectedNodeIds: new Set<string>(),
  expandedSimilar: null,

  // Auth actions
  checkAuth: async () => {
    set({ authLoading: true });
    const result = await api.checkAuth();
    set({
      isAuthenticated: result.authenticated,
      username: result.user || null,
      authLoading: false,
    });
  },

  login: async (username, password) => {
    const result = await api.login(username, password);
    if (result.success) {
      set({ isAuthenticated: true, username });
    }
    return result;
  },

  logout: async () => {
    await api.logout();
    set({ isAuthenticated: false, username: null });
  },

  // Database actions
  fetchStats: async () => {
    try {
      const stats = await api.getStatus();
      set({ stats, connected: true });
    } catch {
      set({ connected: false });
    }
  },

  // Query actions
  setCypherQuery: (query) => set({ cypherQuery: query }),

  executeCypher: async () => {
    const { cypherQuery, queryHistory } = get();
    if (!cypherQuery.trim()) return;

    set({ queryLoading: true, queryError: null });
    try {
      const result = await api.executeCypher(cypherQuery);
      
      // Check for errors in response
      if (result.errors && result.errors.length > 0) {
        set({
          queryError: result.errors.map(e => e.message).join('\n'),
          queryLoading: false,
        });
        return;
      }
      
      // Add to history if not duplicate
      const newHistory = queryHistory.includes(cypherQuery)
        ? queryHistory
        : [cypherQuery, ...queryHistory.slice(0, 19)];
      
      set({
        cypherResult: result,
        queryLoading: false,
        queryHistory: newHistory,
      });
    } catch (err) {
      set({
        queryError: err instanceof Error ? err.message : 'Query failed',
        queryLoading: false,
      });
    }
  },

  // Search actions
  setSearchQuery: (query) => set({ searchQuery: query }),

  executeSearch: async () => {
    const { searchQuery } = get();
    if (!searchQuery.trim()) {
      set({ searchResults: [] });
      return;
    }

    set({ searchLoading: true });
    try {
      const results = await api.search(searchQuery, 20);
      set({ searchResults: results, searchLoading: false });
    } catch {
      set({ searchResults: [], searchLoading: false });
    }
  },

  // Node actions
  setSelectedNode: (node) => set({ selectedNode: node }),

  toggleNodeSelection: (nodeId) => set((state) => {
    const newSet = new Set(state.selectedNodeIds);
    if (newSet.has(nodeId)) {
      newSet.delete(nodeId);
    } else {
      newSet.add(nodeId);
    }
    return { selectedNodeIds: newSet };
  }),

  selectAllNodes: (nodeIds) => set({ selectedNodeIds: new Set(nodeIds) }),

  clearNodeSelection: () => set({ selectedNodeIds: new Set<string>() }),

  // Find similar with inline expansion (doesn't replace search results)
  findSimilar: async (nodeId) => {
    // Start loading for this node
    set({ expandedSimilar: { nodeId, results: [], loading: true } });
    try {
      const results = await api.findSimilar(nodeId, 6); // Show 6 similar items
      set({ expandedSimilar: { nodeId, results, loading: false } });
    } catch {
      set({ expandedSimilar: null });
    }
  },

  // Collapse the similar results
  collapseSimilar: () => set({ expandedSimilar: null }),
}));
