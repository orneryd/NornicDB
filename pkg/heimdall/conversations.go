package heimdall

import (
	"context"
	"errors"
)

var ErrChatWorkspaceMissing = errors.New("create a private chat workspace first")
var ErrChatAuthenticationRequired = errors.New("sign in to use private chats")
var ErrConversationBusy = errors.New("this chat already has a response running")
var ErrConversationNotFound = errors.New("chat not found")

// ConversationTurn is owned by the plugin until Release. Complete persists the
// assistant response before the transport signals a successful completion.
type ConversationTurn struct {
	Context  context.Context
	Messages []ChatMessage
	Database string
	Complete func(context.Context, ConversationReply) error
	Release  func()
}

type ConversationReply struct {
	Content, Reasoning, Status  string
	Tools                       []ConversationTool
	ContextTokens, ContextLimit int
}

type ConversationTurnBackend interface {
	OpenTurn(context.Context, string, string, string) (*ConversationTurn, error)
}

func OpenConversationTurn(ctx context.Context, conversationID, messageID, content string) (*ConversationTurn, error) {
	work, release, accepted := GetSubsystemManager().acquireCallbackWork(ctx)
	if !accepted {
		return nil, errors.New("assistant plugins are stopping")
	}
	for _, plugin := range ListHeimdallPlugins() {
		if backend, ok := plugin.Plugin.(ConversationTurnBackend); ok {
			turn, err := backend.OpenTurn(work, conversationID, messageID, content)
			if err != nil {
				release()
				return nil, err
			}
			pluginRelease := turn.Release
			turn.Context = work
			turn.Release = func() { defer release(); pluginRelease() }
			return turn, nil
		}
	}
	release()
	return nil, errors.New("no conversation plugin is configured")
}

// ChatWorkspaceHost is an optional DatabaseRouter capability. The host grants
// access only to the authenticated owner's dedicated database. Plugins own the
// conversation schema, retention, retrieval and context lifecycle.
type ChatWorkspaceHost interface {
	ChatWorkspace(ctx context.Context, createName *string) (ChatWorkspace, error)
	ChatQuery(ctx context.Context, query string, params map[string]interface{}) ([]map[string]interface{}, error)
	ChatDiscover(ctx context.Context, query string, limit int) (*DiscoverResult, error)
}
type ChatWorkspace struct {
	Configured        bool   `json:"configured"`
	Database          string `json:"database,omitempty"`
	SuggestedDatabase string `json:"suggested_database"`
	Username          string `json:"username"`
}
type ConversationRequest struct {
	AttachmentID string                        `json:"-"`
	Attachment   *ConversationAttachmentUpload `json:"-"`
	Operation    string                        `json:"operation"`
	ID           string                        `json:"id,omitempty"`
	Title        string                        `json:"title,omitempty"`
	Query        string                        `json:"query,omitempty"`
	Archived     *bool                         `json:"archived,omitempty"`
	Database     *string                       `json:"database,omitempty"`
}

const MaxConversationAttachmentBytes = 8 << 20

type ConversationAttachmentUpload struct {
	Name, ContentType string
	Data              []byte
}
type ConversationAttachment struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	ContentType     string `json:"content_type"`
	Size            int    `json:"size"`
	EstimatedTokens int    `json:"estimated_tokens"`
	Readable        bool   `json:"readable"`
	Chunks          int    `json:"chunks"`
}

type ConversationAttachmentDownload struct {
	Metadata ConversationAttachment
	Data     []byte
}
type attachmentScopeKey struct{}

func WithConversationAttachments(ctx context.Context, ids []string) context.Context {
	return context.WithValue(ctx, attachmentScopeKey{}, append([]string(nil), ids...))
}
func ConversationAttachmentsFromContext(ctx context.Context) []string {
	ids, _ := ctx.Value(attachmentScopeKey{}).([]string)
	return append([]string(nil), ids...)
}

type conversationScopeKey struct{}

func WithConversationScope(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, conversationScopeKey{}, id)
}
func ConversationIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(conversationScopeKey{}).(string)
	return id
}

// ConversationBackend is implemented by the bundled plugin, not the transport.
type ConversationBackend interface {
	Conversation(ctx context.Context, request ConversationRequest) (interface{}, error)
}

// CallConversationOperation uses the same host callback lifetime as other hooks.
func CallConversationOperation(ctx context.Context, request ConversationRequest) (interface{}, error) {
	work, release, accepted := GetSubsystemManager().acquireCallbackWork(ctx)
	if !accepted {
		return nil, errors.New("assistant plugins are stopping")
	}
	defer release()
	for _, plugin := range ListHeimdallPlugins() {
		if backend, ok := plugin.Plugin.(ConversationBackend); ok {
			return backend.Conversation(work, request)
		}
	}
	return nil, errors.New("no conversation plugin is configured")
}
