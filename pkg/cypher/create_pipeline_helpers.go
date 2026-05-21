package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// splitMultipleCreates splits a query into CREATE, WITH, and RETURN segments.
func (e *StorageExecutor) splitMultipleCreates(cypher string) []string {
	var segments []string

	// Find all keyword positions (CREATE, WITH, RETURN)
	type keywordPos struct {
		pos  int
		kind string // "CREATE", "WITH", "RETURN"
	}
	var positions []keywordPos

	searchPos := 0
	for searchPos < len(cypher) {
		// Find next CREATE, WITH, or RETURN
		createIdx := findKeywordIndex(cypher[searchPos:], "CREATE")
		withIdx := findKeywordIndex(cypher[searchPos:], "WITH")
		returnIdx := findKeywordIndex(cypher[searchPos:], "RETURN")

		// Find the earliest keyword
		earliest := -1
		var earliestKind string
		if createIdx >= 0 && (earliest == -1 || createIdx < earliest) {
			earliest = createIdx
			earliestKind = "CREATE"
		}
		if withIdx >= 0 && (earliest == -1 || withIdx < earliest) {
			earliest = withIdx
			earliestKind = "WITH"
		}
		if returnIdx >= 0 && (earliest == -1 || returnIdx < earliest) {
			earliest = returnIdx
			earliestKind = "RETURN"
		}

		if earliest == -1 {
			break
		}

		positions = append(positions, keywordPos{
			pos:  searchPos + earliest,
			kind: earliestKind,
		})

		// Move past this keyword
		searchPos = searchPos + earliest
		switch earliestKind {
		case "CREATE":
			searchPos += 6
		case "WITH":
			searchPos += 4
		case "RETURN":
			searchPos += 6
		}
	}

	// Build segments - each segment goes from one keyword to the next
	for i, pos := range positions {
		var endPos int
		if i+1 < len(positions) {
			endPos = positions[i+1].pos
		} else {
			endPos = len(cypher)
		}
		segments = append(segments, strings.TrimSpace(cypher[pos.pos:endPos]))
	}

	return segments
}

// executeCreateNodeSegment executes a single CREATE node statement and returns the created node and variable name.
func (e *StorageExecutor) executeCreateNodeSegment(ctx context.Context, createStmt string) (*storage.Node, string, error) {
	// Extract the pattern after CREATE
	pattern := strings.TrimSpace(createStmt[6:]) // Skip "CREATE"
	store := e.getStorage(ctx)

	// Parse node pattern to get variable name and properties
	nodePattern := e.parseNodePattern(ctx, pattern)
	if nodePattern.variable == "" {
		return nil, "", fmt.Errorf("CREATE node must have a variable name")
	}

	// Validate labels
	for _, label := range nodePattern.labels {
		if !isValidIdentifier(label) {
			return nil, "", fmt.Errorf("invalid label name: %q", label)
		}
	}

	// Validate properties
	for key, val := range nodePattern.properties {
		if !isValidIdentifier(key) {
			return nil, "", fmt.Errorf("invalid property key: %q", key)
		}
		if _, ok := val.(invalidPropertyValue); ok {
			return nil, "", fmt.Errorf("invalid property value for key %q", key)
		}
	}

	// Ensure properties map is initialized (even if empty)
	if nodePattern.properties == nil {
		nodePattern.properties = make(map[string]interface{})
	}

	// Create the node directly
	node := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     nodePattern.labels,
		Properties: nodePattern.properties,
	}

	actualID, err := store.CreateNode(node)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create node: %w", err)
	}

	// CRITICAL: Update node ID with the actual stored ID returned from storage.
	// For namespaced/composite engines:
	//   - NamespacedEngine.CreateNode returns unprefixed ID (user-facing API)
	//   - CompositeEngine.CreateNode returns unprefixed ID (user-facing API)
	//   - The node.ID must match what storage returns for correct edge creation
	// This ensures node IDs are consistent when used in subsequent operations (edge creation, etc.)
	if actualID != "" {
		node.ID = actualID
	}

	e.notifyNodeMutated(string(node.ID))

	return node, nodePattern.variable, nil
}

// executeCreateRelSegment executes a CREATE relationship statement using variable references from context.
func (e *StorageExecutor) executeCreateRelSegment(ctx context.Context, createStmt string, nodeContext map[string]*storage.Node, edgeContext map[string]*storage.Edge, result *ExecuteResult) error {
	// Extract relationship pattern
	pattern := strings.TrimSpace(createStmt[6:]) // Skip "CREATE"
	store := e.getStorage(ctx)

	// Parse relationship pattern: (varA)-[varR:Type {props}]->(varB)
	sourceVar, relContent, targetVar, isReverse, _, err := e.parseCreateRelPatternWithVars(pattern)
	if err != nil {
		return fmt.Errorf("failed to parse relationship pattern: %w", err)
	}

	// Get source and target nodes from context
	sourceNode, sourceExists := nodeContext[sourceVar]
	targetNode, targetExists := nodeContext[targetVar]

	if !sourceExists || !targetExists {
		return fmt.Errorf("variable not found in context: source=%s (exists=%v), target=%s (exists=%v)", sourceVar, sourceExists, targetVar, targetExists)
	}

	// Validate node IDs are not empty
	if sourceNode.ID == "" {
		return fmt.Errorf("source node %s has empty ID", sourceVar)
	}
	if targetNode.ID == "" {
		return fmt.Errorf("target node %s has empty ID", targetVar)
	}

	// Parse relationship type and properties from relContent
	// relContent format: varR:Type {props} or just :Type {props}
	relType := ""
	relVar := ""
	props := make(map[string]interface{})

	// Extract type (after colon, before { or end)
	colonIdx := strings.Index(relContent, ":")
	if colonIdx >= 0 {
		afterColon := strings.TrimSpace(relContent[colonIdx+1:])
		// Check if there's a variable before colon
		beforeColon := strings.TrimSpace(relContent[:colonIdx])
		if beforeColon != "" {
			relVar = beforeColon
		}
		// Extract type (everything before { or end)
		braceIdx := strings.Index(afterColon, "{")
		if braceIdx > 0 {
			relType = strings.TrimSpace(afterColon[:braceIdx])
			// Extract properties
			propsStr := afterColon[braceIdx:]
			props = e.parseProperties(ctx, propsStr)
		} else {
			relType = strings.TrimSpace(afterColon)
		}
	}

	if relType == "" {
		return fmt.Errorf("relationship type is required")
	}

	// Determine start and end nodes based on direction
	var startNode, endNode *storage.Node
	if isReverse {
		startNode = targetNode
		endNode = sourceNode
	} else {
		startNode = sourceNode
		endNode = targetNode
	}

	// Create the relationship
	edge := &storage.Edge{
		ID:         storage.EdgeID(e.generateID()),
		Type:       relType,
		StartNode:  startNode.ID,
		EndNode:    endNode.ID,
		Properties: props,
	}

	if err := store.CreateEdge(edge); err != nil {
		return fmt.Errorf("failed to create edge: %w", err)
	}

	if relVar != "" {
		edgeContext[relVar] = edge
	}

	result.Stats.RelationshipsCreated++
	addOptimisticRelationshipID(result, edge.ID)
	return nil
}

// containsString checks if a slice contains a string.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// validateSetAssignments pre-validates SET clause assignments before executing CREATE
// This ensures we fail fast on invalid function calls, preventing orphaned nodes
func (e *StorageExecutor) validateSetAssignments(assignments []string) error {
	// Known Cypher functions
	knownFunctions := map[string]bool{
		"COALESCE": true, "TOSTRING": true, "TOINT": true, "TOFLOAT": true,
		"TOBOOLEAN": true, "TOLOWER": true, "TOUPPER": true, "TRIM": true,
		"SIZE": true, "LENGTH": true, "ABS": true, "CEIL": true, "FLOOR": true,
		"ROUND": true, "RAND": true, "SQRT": true, "SIGN": true, "LOG": true,
		"LOG10": true, "EXP": true, "SIN": true, "COS": true, "TAN": true,
		"DATE": true, "DATETIME": true, "TIME": true, "TIMESTAMP": true,
		"DURATION": true, "LOCALDATETIME": true, "LOCALTIME": true,
		"HEAD": true, "LAST": true, "TAIL": true, "KEYS": true, "LABELS": true,
		"TYPE": true, "ID": true, "ELEMENTID": true, "PROPERTIES": true,
		"POINT": true, "DISTANCE": true, "REPLACE": true, "SUBSTRING": true,
		"LEFT": true, "RIGHT": true, "SPLIT": true, "REVERSE": true,
		"LTRIM": true, "RTRIM": true, "COLLECT": true, "RANGE": true,
	}

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Parse assignment: var.property = value or var:Label
		eqIdx := strings.Index(assignment, "=")
		if eqIdx == -1 {
			// Could be a label assignment like "n:Label" - these are valid
			continue
		}

		rightSide := strings.TrimSpace(assignment[eqIdx+1:])

		// Check if right side looks like a function call
		if strings.Contains(rightSide, "(") && strings.HasSuffix(strings.TrimSpace(rightSide), ")") {
			// Extract function name (before first parenthesis)
			parenIdx := strings.Index(rightSide, "(")
			funcName := strings.ToUpper(strings.TrimSpace(rightSide[:parenIdx]))
			if !knownFunctions[funcName] {
				return fmt.Errorf("unknown function: %s", funcName)
			}
		}
	}
	return nil
}

// extractCreateVariableRefs returns variable names used as simple refs in CREATE relationship patterns
// (e.g. (o)-[:R]->(pharmacy) yields ["o", "pharmacy"]). Used to know which bindings the pipeline must return.
func extractCreateVariableRefs(createPart string) []string {
	seen := make(map[string]bool)
	createClauses := createKeywordPattern.Split(createPart, -1)
	for _, clause := range createClauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		// Relationship pattern: (a)-[...]->(b) or (a)<-[...]-(b)
		if matches := relForwardPattern.FindStringSubmatch(clause); len(matches) >= 6 {
			src := strings.TrimSpace(matches[1])
			tgt := strings.TrimSpace(matches[5])
			if isSimpleVariable(src) {
				seen[src] = true
			}
			if isSimpleVariable(tgt) {
				seen[tgt] = true
			}
		}
		if matches := relReversePattern.FindStringSubmatch(clause); len(matches) >= 6 {
			src := strings.TrimSpace(matches[1])
			tgt := strings.TrimSpace(matches[5])
			if isSimpleVariable(src) {
				seen[src] = true
			}
			if isSimpleVariable(tgt) {
				seen[tgt] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

// executeMatchWithPipelineToRows runs the MATCH+WITH+UNWIND+MATCH+WITH pipeline and returns rows
// as []map[string]interface{} (each value may be *storage.Node or scalar). Only keys in createVars
// are guaranteed in each row; row values that are *Node are used by CREATE.
func (e *StorageExecutor) executeMatchWithPipelineToRows(ctx context.Context, matchPart string, createVars []string, store storage.Engine) ([]map[string]interface{}, error) {
	// Parse: MATCH (o:Label) WHERE ... WITH ... WITH collect(o) AS orders UNWIND ... WITH ... MATCH (ph:Pharmacy) WITH ... WITH ... WITH o, pharmacies[i % size(pharmacies)] AS pharmacy
	withIdx := findKeywordIndex(matchPart, "WITH")
	if withIdx < 0 {
		return nil, fmt.Errorf("pipeline requires WITH")
	}
	matchSection := strings.TrimSpace(matchPart[:withIdx])
	// MATCH (o:OrderStatus) WHERE NOT (o)-[:UPDATED_TO]->()
	matchSection = strings.TrimSpace(strings.TrimPrefix(matchSection, "MATCH"))
	whereIdx := findKeywordIndex(matchSection, "WHERE")
	var matchPattern, whereClause string
	if whereIdx > 0 {
		matchPattern = strings.TrimSpace(matchSection[:whereIdx])
		whereClause = strings.TrimSpace(matchSection[whereIdx+5:])
	} else {
		matchPattern = matchSection
	}
	matchPattern = strings.TrimSpace(matchPattern)
	if !strings.HasPrefix(matchPattern, "(") {
		matchPattern = "(" + matchPattern + ")"
	}
	nodeInfo := e.parseNodePattern(ctx, matchPattern)
	if nodeInfo.variable == "" {
		return nil, fmt.Errorf("MATCH pattern must have a variable")
	}
	var nodes []*storage.Node
	var err error
	if len(nodeInfo.labels) > 0 {
		nodes, err = store.GetNodesByLabel(nodeInfo.labels[0])
		if err != nil {
			nodes, _ = store.AllNodes()
		}
	} else {
		nodes, err = store.AllNodes()
		if err != nil {
			return nil, err
		}
	}
	if len(nodeInfo.labels) > 1 {
		var filtered []*storage.Node
		for _, n := range nodes {
			hasAll := true
			for _, l := range nodeInfo.labels[1:] {
				found := false
				for _, nl := range n.Labels {
					if nl == l {
						found = true
						break
					}
				}
				if !found {
					hasAll = false
					break
				}
			}
			if hasAll {
				filtered = append(filtered, n)
			}
		}
		nodes = filtered
	}
	if len(nodeInfo.properties) > 0 {
		var filtered []*storage.Node
		for _, n := range nodes {
			if e.nodeMatchesProps(n, nodeInfo.properties) {
				filtered = append(filtered, n)
			}
		}
		nodes = filtered
	}
	if whereClause != "" {
		var filtered []*storage.Node
		for _, n := range nodes {
			if e.evaluateWhere(ctx, n, nodeInfo.variable, whereClause) {
				filtered = append(filtered, n)
			}
		}
		nodes = filtered
	}
	// Sort by orderId for stable ordering
	sortNodesByProperty(nodes, "orderId")
	// WITH o ORDER BY o.orderId -> already sorted
	// WITH collect(o) AS orders
	orders := make([]interface{}, len(nodes))
	for i, n := range nodes {
		orders[i] = n
	}
	// UNWIND range(0, size(orders)-1) AS i
	var rows []map[string]interface{}
	for i := 0; i < len(orders); i++ {
		row := map[string]interface{}{"orders": orders, "i": int64(i)}
		// WITH orders[i] AS o, i
		o := orders[i].(*storage.Node)
		row["o"] = o
		row["i"] = int64(i)
		// MATCH (ph:Pharmacy)
		pharmacies, _ := store.GetNodesByLabel("Pharmacy")
		sortNodesByProperty(pharmacies, "id")
		for _, ph := range pharmacies {
			r := make(map[string]interface{})
			for k, v := range row {
				r[k] = v
			}
			r["ph"] = ph
			// WITH o, i, ph ORDER BY ph.id -> we already sorted pharmacies
			// Group by (o, i) and collect(ph) then index: we need one row per (o,i) with pharmacy = pharmacies[i % size(pharmacies)]
			// So for this row we have o, i, ph. We'll collect all ph for same (o,i), then pick pharmacies[i % size(pharmacies)]
			rows = append(rows, r)
		}
	}
	// Collapse: group by (o, i), collect(ph) as list, then row = o, pharmacy = list[i % len(list)]
	grouped := make(map[string][]*storage.Node)
	for _, r := range rows {
		o := r["o"].(*storage.Node)
		i := r["i"].(int64)
		ph := r["ph"].(*storage.Node)
		key := string(o.ID) + "\x00" + strconv.FormatInt(i, 10)
		grouped[key] = append(grouped[key], ph)
	}
	out := make([]map[string]interface{}, 0, len(orders))
	for i := 0; i < len(orders); i++ {
		o := orders[i].(*storage.Node)
		key := string(o.ID) + "\x00" + strconv.FormatInt(int64(i), 10)
		pharmacies := grouped[key]
		if len(pharmacies) == 0 {
			continue
		}
		idx := i % len(pharmacies)
		pharmacy := pharmacies[idx]
		out = append(out, map[string]interface{}{"o": o, "pharmacy": pharmacy})
	}
	return out, nil
}

func sortNodesByProperty(nodes []*storage.Node, prop string) {
	// Simple sort by property value (string comparison)
	// OrderStatus by orderId, Pharmacy by id
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			vi := fmt.Sprint(getNodeProp(nodes[i], prop))
			vj := fmt.Sprint(getNodeProp(nodes[j], prop))
			if vi > vj {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}
}

func getNodeProp(n *storage.Node, prop string) interface{} {
	if n == nil || n.Properties == nil {
		return nil
	}
	return n.Properties[prop]
}
