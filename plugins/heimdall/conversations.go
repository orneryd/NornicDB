package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/auth"
	guardian "github.com/orneryd/nornicdb/pkg/heimdall"
	"strings"
	"time"
	"unicode/utf8"
)

const threadColumns = "t.id AS id, t.title AS title, t.archived AS archived, t.database AS database, t.updated_at AS updated_at,t.context_tokens AS context_tokens,t.context_limit AS context_limit,t.compaction_status AS compaction_status,t.compaction_error AS compaction_error,t.summary_count AS summary_count"

func chatTime() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z") }
func chatTitle(text string) string {
	text = strings.TrimSpace(strings.SplitN(text, "\n", 2)[0])
	runes := []rune(text)
	if len(runes) > 80 {
		text = string(runes[:80])
	}
	if text == "" {
		return "New chat"
	}
	return text
}
func (p *WatcherPlugin) conversationHost() (guardian.ChatWorkspaceHost, error) {
	p.mu.RLock()
	database := p.ctx.Database
	p.mu.RUnlock()
	host, ok := database.(guardian.ChatWorkspaceHost)
	if !ok {
		return nil, fmt.Errorf("private chat persistence is unavailable")
	}
	return host, nil
}
func (p *WatcherPlugin) Conversation(ctx context.Context, request guardian.ConversationRequest) (interface{}, error) {
	host, err := p.conversationHost()
	if err != nil {
		return nil, err
	}
	workspace, err := host.ChatWorkspace(ctx, nil)
	if err != nil {
		return nil, err
	}
	if !workspace.Configured {
		return nil, guardian.ErrChatWorkspaceMissing
	}
	if strings.HasPrefix(request.Operation, "attachment_") {
		return p.conversationAttachment(ctx, host, request)
	}
	archived := false
	if request.Archived != nil {
		archived = *request.Archived
	}
	params := map[string]interface{}{"id": request.ID, "archived": archived, "now": chatTime()}
	switch request.Operation {
	case "context":
		rows, err := host.ChatQuery(ctx, "MATCH (t:AssistantThread {id:$id}) RETURN "+threadColumns, params)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, guardian.ErrConversationNotFound
		}
		return rows[0], nil
	case "compact":
		return p.startConversationCompaction(ctx, host, request.ID)
	case "list":
		return host.ChatQuery(ctx, "MATCH (t:AssistantThread) WHERE t.archived = $archived RETURN "+threadColumns+" ORDER BY t.updated_at DESC LIMIT 100", params)
	case "create":
		if !utf8.ValidString(request.Title) || len([]rune(request.Title)) > 200 {
			return nil, fmt.Errorf("chat title must be at most 200 characters")
		}
		if request.Database != nil && *request.Database != "" && !auth.RequestDatabasePermissionAllowed(ctx, *request.Database, auth.PermRead) {
			return nil, fmt.Errorf("source database access denied")
		}
		params["id"] = uuid.NewString()
		params["title"] = chatTitle(request.Title)
		params["database"] = ""
		if request.Database != nil {
			params["database"] = *request.Database
		}
		rows, err := host.ChatQuery(ctx, "CREATE (t:AssistantThread {id:$id,title:$title,database:$database,archived:false,created_at:$now,updated_at:$now,summary:''}) RETURN "+threadColumns, params)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, fmt.Errorf("chat was not created")
		}
		return rows[0], nil
	case "get":
		rows, err := host.ChatQuery(ctx, "MATCH (t:AssistantThread {id:$id}) RETURN "+threadColumns+", t.summary AS summary", params)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, fmt.Errorf("chat not found")
		}
		messages, err := host.ChatQuery(ctx, "MATCH (:AssistantThread {id:$id})-[:HAS_MESSAGE]->(m:AssistantMessage) RETURN m.id AS id,m.role AS role,m.content AS content,m.attachment_ids AS attachment_ids,m.reasoning_content AS reasoning_content,m.tool_calls AS tool_calls,m.created_at AS created_at,m.status AS status,m.request_id AS request_id ORDER BY m.created_at ASC,m.id ASC", params)
		if err != nil {
			return nil, err
		}
		metadata, err := p.conversationAttachment(ctx, host, guardian.ConversationRequest{ID: request.ID, Operation: "attachment_list"})
		if err != nil {
			return nil, err
		}
		files := map[string]guardian.ConversationAttachment{}
		for _, file := range metadata.([]guardian.ConversationAttachment) {
			files[file.ID] = file
		}
		for _, message := range messages {
			attachments := make([]guardian.ConversationAttachment, 0)
			if encoded, _ := message["attachment_ids"].(string); encoded != "" {
				var ids []string
				if err := json.Unmarshal([]byte(encoded), &ids); err != nil {
					return nil, err
				}
				for _, id := range ids {
					if file, found := files[id]; found {
						attachments = append(attachments, file)
					}
				}
			}
			message["attachments"] = attachments
			delete(message, "attachment_ids")
		}
		return map[string]interface{}{"thread": rows[0], "messages": messages}, nil
	case "update":
		params["title"] = chatTitle(request.Title)
		query := "MATCH (t:AssistantThread {id:$id}) SET t.updated_at=$now"
		if request.Archived != nil {
			query += ",t.archived=$archived"
		}
		if request.Database != nil {
			if *request.Database != "" && !auth.RequestDatabasePermissionAllowed(ctx, *request.Database, auth.PermRead) {
				return nil, fmt.Errorf("source database access denied")
			}
			params["database"] = *request.Database
			query += ",t.database=$database"
		}
		if request.Title != "" {
			query += ",t.title=$title"
		}
		rows, err := host.ChatQuery(ctx, query+" RETURN "+threadColumns, params)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, fmt.Errorf("chat not found")
		}
		return rows[0], nil
	case "delete":
		if cancel, found := p.compactions.Load(workspace.Database + "\x00" + request.ID); found {
			cancel.(context.CancelFunc)()
		}
		if request.ID == "" {
			return nil, fmt.Errorf("choose a chat to delete")
		}
		_, err := host.ChatQuery(ctx, "MATCH (t:AssistantThread {id:$id}) OPTIONAL MATCH (t)-[:HAS_ATTACHMENT]->(a:AssistantAttachment) OPTIONAL MATCH (a)-->(part) DETACH DELETE part,a", params)
		if err != nil {
			return nil, err
		}
		_, err = host.ChatQuery(ctx, "MATCH (t:AssistantThread {id:$id}) OPTIONAL MATCH (t)-[:HAS_MESSAGE]->(m:AssistantMessage) DETACH DELETE m,t", params)
		return map[string]bool{"deleted": err == nil}, err
	case "search":
		return p.searchPrivateHistory(ctx, host, request.Query, "")
	default:
		return nil, fmt.Errorf("unknown conversation operation")
	}
}

func (p *WatcherPlugin) searchPrivateHistory(ctx context.Context, host guardian.ChatWorkspaceHost, query, exclude string) (interface{}, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return []interface{}{}, nil
	}
	result, err := host.ChatDiscover(ctx, query, 12)
	if err != nil {
		return nil, err
	}
	// Resolve each retrieved message to its owning thread using the same private
	// workspace. Search never accepts a caller-supplied database or owner.
	matches := make([]map[string]interface{}, 0)
	for _, hit := range result.Results {
		params := map[string]interface{}{"message": hit.ID, "exclude": exclude}
		rows, lookupErr := host.ChatQuery(ctx, "MATCH (t:AssistantThread)-[:HAS_MESSAGE]->(m:AssistantMessage) WHERE id(m)=$message AND t.id <> $exclude RETURN t.id AS conversation_id,t.title AS title,t.archived AS archived,m.id AS message_id,m.content AS content LIMIT 1", params)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if len(rows) == 0 {
			continue
		}
		rows[0]["method"] = result.Method
		matches = append(matches, rows[0])
		if len(matches) == 6 {
			break
		}
	}
	return matches, nil
}
