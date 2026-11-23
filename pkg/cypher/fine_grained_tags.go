package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
)

func isFineGrainedTagStatement(ctx context.Context, query string) bool {
	if auth.FineGrainedRequestFromContext(ctx) == nil || cypherVersionFromContext(ctx) != "25" ||
		!(startsWithKeywords(ctx, query, "ALTER", "USER") || startsWithKeywords(ctx, query, "ALTER", "USERS")) {
		return false
	}
	return findKeywordIndex(ctx, query, "TAGS") >= 0 || findKeywordIndex(ctx, query, "TAG") >= 0
}

func executeFineGrainedUserTags(ctx context.Context, request *auth.FineGrainedRequest, query string) (*ExecuteResult, error) {
	if !request.AllowsDBMS("dbms.user.alter", "") || !request.AllowsDBMS("dbms.user.metadata.set", "") {
		return nil, auth.ErrFineGrainedForbidden
	}
	cursor := fineGrainedPrivilegeCursor{text: query}
	cursor.skipGap()
	cursor.takeKeyword("ALTER")
	if !cursor.takeKeyword("USERS") {
		cursor.takeKeyword("USER")
	}
	names, err := cursor.readNameList(false, "expected a username", "")
	if err != nil {
		return nil, err
	}
	ifExists := cursor.takeKeyword("IF")
	if ifExists && !cursor.takeKeyword("EXISTS") {
		return nil, fineGrainedPrivilegeSyntaxError("expected IF EXISTS")
	}
	var change auth.FineGrainedTagChange
	var all bool
	switch {
	case cursor.takeKeyword("SET"):
		change = auth.ReplaceFineGrainedTags
	case cursor.takeKeyword("ADD"):
		change = auth.AddFineGrainedTags
	case cursor.takeKeyword("REMOVE"):
		change = auth.RemoveFineGrainedTags
		if cursor.takeKeyword("ALL") {
			change, all = auth.ReplaceFineGrainedTags, true
		}
	default:
		return nil, fineGrainedPrivilegeSyntaxError("expected SET, ADD, or REMOVE before tags")
	}
	if !cursor.takeKeyword("TAGS") && !cursor.takeKeyword("TAG") {
		return nil, fineGrainedPrivilegeSyntaxError("expected TAG or TAGS")
	}
	if all && cursor.at == len(query) {
		return &ExecuteResult{}, request.Runtime.ChangeUserTags(ctx, names, change, nil, ifExists)
	}
	tags, valid := fineGrainedTagList(strings.TrimSpace(query[cursor.at:]))
	if !valid || all {
		return nil, fineGrainedPrivilegeSyntaxError("tags require a string or a list of strings")
	}
	return &ExecuteResult{}, request.Runtime.ChangeUserTags(ctx, names, change, tags, ifExists)
}

func fineGrainedTagList(source string) ([]string, bool) {
	if values, list := parseLiteralListForPipeline(source); list {
		tags := make([]string, len(values))
		for i, value := range values {
			tag, ok := value.(string)
			if !ok {
				return nil, false
			}
			tags[i] = tag
		}
		return tags, true
	}
	value, valid := parseLiteralValue(source)
	tag, stringValue := value.(string)
	if !valid || !stringValue {
		return nil, false
	}
	return []string{tag}, true
}
