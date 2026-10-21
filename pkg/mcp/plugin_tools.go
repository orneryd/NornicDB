package mcp

import (
	"fmt"
	"log/slog"
)

// RegisterTool appends a plugin tool to the built-in tools. Registration is FIFO:
// the first handler keeps its name, and a collision is logged and rejected.
// It is safe to register while clients list or call tools.
func (s *Server) RegisterTool(tool Tool, handler ToolHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.handlers[tool.Name]; exists {
		err := fmt.Errorf("MCP tool registration failed: name collision: %s", tool.Name)
		slog.Warn(err.Error(), "tool", tool.Name, "outcome", "kept first registration")
		return err
	}
	s.handlers[tool.Name] = handler
	s.pluginTools = append(s.pluginTools, tool)
	return nil
}

func (s *Server) toolDefinitions(database string) []Tool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append(GetToolDefinitionsWithDefaultDatabase(database), s.pluginTools...)
}
