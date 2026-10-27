package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// DDL value readers use the shared allocation-free keyword owner in
// internal/cyphertext and retain their own value grammar.

func kpSkipSpaces(s string, i int) int {
	for i < len(s) && (cyphertext.IsWhitespaceAt(s, i)) {
		i++
	}
	return i
}

func kpScanIdent(s string, i int) (string, int) {
	name, ok := cyphertext.ScanIdentifier(s, i)
	if !ok {
		return "", i
	}
	return name.Value, name.End
}

func kpExpectByte(s string, i int, b byte) int {
	if i >= len(s) || s[i] != b {
		return -1
	}
	return i + 1
}

func kpScanQuotedString(s string, i int) (string, int) {
	if i >= len(s) {
		return "", i
	}
	if !isCypherStringQuoteByte(s[i]) {
		return "", i
	}
	end, closed := scanCypherQuotedText(s, i, s[i])
	if !closed {
		return "", -1
	}
	value, decoded := cyphertext.DecodeString(s[i:end], cyphertext.AcceptedSource)
	if !decoded {
		return "", -1
	}
	return value, end
}

// kpScanDecimal shares signed decimal extent/admission while preserving the
// requested magnitude domain: int64 values never pass through float64.
func kpScanDecimal[T int64 | float64](s string, i int) (T, int, bool) {
	var zero T
	start := i
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	token, ok := cyphertext.ScanNumber(s, i)
	_, integer := any(zero).(int64)
	if !ok || token.Invalid || token.Base != 10 || token.Grouped || token.Suffix != 0 || integer && (token.Fraction || token.Exponent) {
		return zero, start, false
	}
	if integer {
		value, err := strconv.ParseInt(s[start:token.End], 10, 64)
		if err != nil {
			return zero, start, false
		}
		return T(value), token.End, true
	}
	value, err := strconv.ParseFloat(s[start:token.End], 64)
	if err != nil {
		return zero, start, false
	}
	return T(value), token.End, true
}

func kpScanBool(s string, i int) (bool, int, bool) {
	if j := cyphertext.KeywordEnd(s, i, "TRUE", cyphertext.CypherWord); j > 0 {
		return true, j, true
	}
	if j := cyphertext.KeywordEnd(s, i, "FALSE", cyphertext.CypherWord); j > 0 {
		return false, j, true
	}
	return false, i, false
}

// kpScanBraceBlock scans a balanced {...} block and returns its inner content.
func kpScanBraceBlock(s string, i int) (string, int) {
	i = kpSkipSpaces(s, i)
	if i >= len(s) || s[i] != '{' {
		return "", -1
	}
	depth := 1
	start := i + 1
	i++
	for i < len(s) && depth > 0 {
		if end, quoted := skipCypherLexicalToken(s, i); quoted {
			i = end
			continue
		}
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
		}
		if depth > 0 {
			i++
		}
	}
	if depth != 0 {
		return "", -1
	}
	return s[start:i], i + 1
}

// DDL command types

type CreateDecayProfileBundleCmd struct {
	Bundle knowledgepolicy.DecayProfileBundle
}

type CreateDecayProfileBindingCmd struct {
	Binding knowledgepolicy.DecayProfileBinding
}

type AlterDecayProfileCmd struct {
	Name    string
	Updates map[string]interface{}
}

type AlterDecayProfileBindingCmd struct {
	Binding knowledgepolicy.DecayProfileBinding
}

type DropDecayProfileCmd struct {
	Name     string
	IfExists bool
}

type ShowDecayProfilesCmd struct{}

type CreatePromotionProfileCmd struct {
	Profile knowledgepolicy.PromotionProfileDef
}

type AlterPromotionProfileCmd struct {
	Name    string
	Updates map[string]interface{}
}

type DropPromotionProfileCmd struct {
	Name     string
	IfExists bool
}

type ShowPromotionProfilesCmd struct{}

type CreatePromotionPolicyCmd struct {
	Policy      knowledgepolicy.PromotionPolicyDef
	IfNotExists bool
}

type AlterPromotionPolicyCmd struct {
	Name    string
	Updates map[string]interface{}
}

type AlterPromotionPolicyDefinitionCmd struct {
	Policy knowledgepolicy.PromotionPolicyDef
}

type DropPromotionPolicyCmd struct {
	Name     string
	IfExists bool
}

type ShowPromotionPoliciesCmd struct{}

func kpRequireStatementEnd(statement string, index int, command string) error {
	index = kpSkipSpaces(statement, index)
	if index < len(statement) && statement[index] == ';' {
		index = kpSkipSpaces(statement, index+1)
	}
	if index != len(statement) {
		return localizedError(localization.CypherKnowledgePolicyExpectedAfter("end of statement", command), nil)
	}
	return nil
}

func isKnowledgePolicyDDLStatement(statement string) bool {
	for _, operation := range []string{"CREATE", "ALTER", "DROP"} {
		if startsWithKeywords(statement, operation, "DECAY PROFILE") ||
			startsWithKeywords(statement, operation, "PROMOTION PROFILE") ||
			startsWithKeywords(statement, operation, "PROMOTION POLICY") {
			return true
		}
	}
	return startsWithKeywords(statement, "SHOW", "DECAY PROFILES") ||
		startsWithKeywords(statement, "SHOW", "PROMOTION PROFILES") ||
		startsWithKeywords(statement, "SHOW", "PROMOTION POLICIES")
}

type policyDDLSource struct {
	*cyphertext.Source
	original bool
}

func (source policyDDLSource) window(start, end int) policyDDLSource {
	return policyDDLSource{Source: source.Window(start, end), original: source.original}
}
func (source policyDDLSource) storedText(start, end int) (string, error) {
	text, ok := source.OriginalText(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExtent, nil)
	if !ok {
		return "", fmt.Errorf("invalid policy source span: %w", storage.ErrInvalidData)
	}
	text = strings.TrimSpace(text)
	if !source.original {
		return cyphertext.EmitRawSource(text), nil
	}
	return text, nil
}

// ParseKnowledgePolicyDDL attempts to parse a knowledge-layer DDL statement.
// Returns (command, true, nil) on success, (nil, false, nil) if the input
// is not a knowledge-layer DDL statement, or (nil, false, err) on parse error.
// The original context and explicit source stage preserve stored policy spelling.
func ParseKnowledgePolicyDDL(ctx context.Context, stmt string, stage SourceStage) (command interface{}, owned bool, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	defer func() {
		if err := ctx.Err(); err != nil {
			command, owned, retErr = nil, false, err
		}
	}()
	var retained *cyphertext.Source
	original := stage == RawSource
	if stage == AcceptedSource {
		retained, original = cyphertext.RetainedSource(ctx, stmt, cyphertext.AcceptedSource)
	}
	if retained == nil {
		retained = cyphertext.NewSource(ctx, stmt, stage)
	}
	if retained.Err != nil {
		return nil, false, retained.Err
	}
	source := policyDDLSource{Source: retained, original: original}
	left := len(source.Text) - len(strings.TrimLeftFunc(source.Text, cyphertext.IsWhitespace))
	source = source.window(left, left+len(strings.TrimSpace(source.Text)))
	s := source.Text
	i := 0
	i = kpSkipSpaces(s, i)

	if j := cyphertext.KeywordEnd(s, i, "CREATE", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := cyphertext.KeywordEnd(s, j, "DECAY", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILE", cyphertext.CypherWord); l > 0 {
				return parseCreateDecayProfile(s, l)
			}
		}
		if k := cyphertext.KeywordEnd(s, j, "PROMOTION", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILE", cyphertext.CypherWord); l > 0 {
				return parseCreatePromotionProfile(s, l)
			}
			if l := cyphertext.KeywordEnd(s, k, "POLICY", cyphertext.CypherWord); l > 0 {
				return parseCreatePromotionPolicy(source, l)
			}
		}
		return nil, false, nil
	}

	if j := cyphertext.KeywordEnd(s, i, "ALTER", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := cyphertext.KeywordEnd(s, j, "DECAY", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILE", cyphertext.CypherWord); l > 0 {
				return parseAlterDecayProfile(s, l)
			}
		}
		if k := cyphertext.KeywordEnd(s, j, "PROMOTION", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILE", cyphertext.CypherWord); l > 0 {
				return parseAlterPromotionProfile(s, l)
			}
			if l := cyphertext.KeywordEnd(s, k, "POLICY", cyphertext.CypherWord); l > 0 {
				return parseAlterPromotionPolicy(source, l)
			}
		}
		return nil, false, nil
	}

	if j := cyphertext.KeywordEnd(s, i, "DROP", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := cyphertext.KeywordEnd(s, j, "DECAY", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILE", cyphertext.CypherWord); l > 0 {
				return parseDropDecayProfile(s, l)
			}
		}
		if k := cyphertext.KeywordEnd(s, j, "PROMOTION", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILE", cyphertext.CypherWord); l > 0 {
				return parseDropPromotionProfile(s, l)
			}
			if l := cyphertext.KeywordEnd(s, k, "POLICY", cyphertext.CypherWord); l > 0 {
				return parseDropPromotionPolicy(s, l)
			}
		}
		return nil, false, nil
	}

	if j := cyphertext.KeywordEnd(s, i, "SHOW", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := cyphertext.KeywordEnd(s, j, "DECAY", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILES", cyphertext.CypherWord); l > 0 {
				return &ShowDecayProfilesCmd{}, true, nil
			}
		}
		if k := cyphertext.KeywordEnd(s, j, "PROMOTION", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILES", cyphertext.CypherWord); l > 0 {
				return &ShowPromotionProfilesCmd{}, true, nil
			}
			if l := cyphertext.KeywordEnd(s, k, "POLICIES", cyphertext.CypherWord); l > 0 {
				return &ShowPromotionPoliciesCmd{}, true, nil
			}
		}
		return nil, false, nil
	}

	return nil, false, nil
}

func parseCreateDecayProfile(s string, i int) (interface{}, bool, error) {
	name, i, err := kpParseKnowledgeName(s, i, "CREATE DECAY PROFILE", false)
	if err != nil {
		return nil, false, err
	}
	i = kpSkipSpaces(s, i)

	if j := cyphertext.KeywordEnd(s, i, "OPTIONS", cyphertext.CypherWord); j > 0 {
		return parseDecayProfileBundleOptions(name, s, j)
	}

	if j := cyphertext.KeywordEnd(s, i, "FOR", cyphertext.CypherWord); j > 0 {
		return parseDecayProfileBinding(name, s, j)
	}

	return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfterProfileName("OPTIONS or FOR", name), nil)
}

func kpScanQuotedName(s string, i int) (string, int) {
	if i >= len(s) {
		return "", i
	}
	if isCypherStringQuoteByte(s[i]) {
		val, j := kpScanQuotedString(s, i)
		if j < 0 {
			return "", i
		}
		return val, j
	}
	return "", i
}

// kpScanName tries a quoted string first, then falls back to a bare identifier.
func kpScanName(s string, i int) (string, int) {
	if name, j := kpScanQuotedName(s, i); name != "" {
		return name, j
	}
	return kpScanIdent(s, i)
}

func parseDecayProfileBundleOptions(name, s string, i int) (interface{}, bool, error) {
	body, j := kpScanBraceBlock(s, i)
	if j < 0 {
		return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("{", "OPTIONS"), nil)
	}
	if err := kpRequireStatementEnd(s, j, "CREATE DECAY PROFILE "+name); err != nil {
		return nil, false, err
	}

	bundle := knowledgepolicy.DecayProfileBundle{
		Name:         name,
		Enabled:      true,
		DecayEnabled: true,
		ScoreFrom:    knowledgepolicy.ScoreFromCreated,
		Scope:        knowledgepolicy.ScopeNode,
	}

	updates, err := parseDecayProfileOptions(body)
	if err != nil {
		return nil, false, err
	}
	bundle, err = storage.ApplyDecayProfileOptions(bundle, updates)
	if err != nil {
		return nil, false, err
	}
	return &CreateDecayProfileBundleCmd{Bundle: bundle}, true, nil
}

func parseDecayProfileOptions(body string) (map[string]interface{}, error) {
	updates := make(map[string]interface{})
	err := parseOptionsMap(body, func(key, rawVal string) error {
		switch lowerASCII(key) {
		case "halflifeseconds":
			n, err := strconv.ParseInt(rawVal, 10, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("halfLifeSeconds", rawVal, false), err)
			}
			updates["halfLifeSeconds"] = n
		case "visibilitythreshold":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("visibilityThreshold", rawVal, false), err)
			}
			updates["visibilityThreshold"] = f
		case "scorefloor":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("scoreFloor", rawVal, false), err)
			}
			updates["scoreFloor"] = f
		case "function":
			fn := knowledgepolicy.DecayFunction(strings.Trim(rawVal, "'\""))
			if !knowledgepolicy.ValidDecayFunctions[fn] {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("decay function", rawVal, true), nil)
			}
			updates["function"] = string(fn)
		case "scope":
			sc := knowledgepolicy.ScopeType(upperASCII(strings.Trim(rawVal, "'\"")))
			if !knowledgepolicy.ValidScopeTypes[sc] {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("scope", rawVal, true), nil)
			}
			updates["scope"] = string(sc)
		case "decayenabled":
			b, err := strconv.ParseBool(rawVal)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("decayEnabled", rawVal, false), err)
			}
			updates["decayEnabled"] = b
		case "scorefrom":
			mode := knowledgepolicy.ScoreFromMode(upperASCII(strings.Trim(rawVal, "'\"")))
			if !knowledgepolicy.ValidScoreFromModes[mode] {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("scoreFrom", rawVal, true), nil)
			}
			updates["scoreFrom"] = string(mode)
		case "scorefromproperty":
			updates["scoreFromProperty"] = strings.Trim(rawVal, "'\"")
		case "enabled":
			b, err := strconv.ParseBool(rawVal)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("enabled", rawVal, false), err)
			}
			updates["enabled"] = b
		default:
			return localizedError(localization.CypherKnowledgePolicyUnknownOption(key), nil)
		}
		return nil
	})
	return updates, err
}

func parseDecayProfileBinding(name, s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)

	binding := knowledgepolicy.DecayProfileBinding{
		Name: name,
	}

	var err error
	binding, i, err = parseForTarget(s, i, binding)
	if err != nil {
		return nil, false, err
	}

	i = kpSkipSpaces(s, i)

	if j := cyphertext.KeywordEnd(s, i, "APPLY", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		body, k := kpScanBraceBlock(s, j)
		if k < 0 {
			return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("{", "APPLY"), nil)
		}
		i = k

		if err := parseBindingApplyBlock(body, &binding); err != nil {
			return nil, false, err
		}
	}
	if err := kpRequireStatementEnd(s, i, "DECAY PROFILE "+name); err != nil {
		return nil, false, err
	}

	return &CreateDecayProfileBindingCmd{Binding: binding}, true, nil
}

func parseForTarget(s string, i int, binding knowledgepolicy.DecayProfileBinding) (knowledgepolicy.DecayProfileBinding, int, error) {
	i = kpSkipSpaces(s, i)

	if i >= len(s) || s[i] != '(' {
		return binding, i, localizedError(localization.CypherKnowledgePolicyExpectedAfter("target pattern", "FOR"), nil)
	}

	parenStart := i
	i++
	i = kpSkipSpaces(s, i)

	if i < len(s) && s[i] == ')' {
		i++
		j := kpSkipSpaces(s, i)
		if j < len(s) && s[j] == '-' {
			return parseEdgeTarget(s, parenStart, binding)
		}
		binding.IsWildcard = true
		return binding, i, nil
	}

	_, i = kpScanIdent(s, i)
	i = kpSkipSpaces(s, i)

	if i < len(s) && s[i] == ':' {
		labels := []string{}
		for i < len(s) && s[i] == ':' {
			i++
			label, j := kpScanIdent(s, i)
			if label == "" {
				return binding, i, localizedError(localization.CypherKnowledgePolicyLabelExpectedAfterColon(), nil)
			}
			labels = append(labels, label)
			i = j
		}
		binding.TargetLabels = labels
	}

	i = kpSkipSpaces(s, i)
	if i < len(s) && s[i] == ')' {
		i++
	}

	return binding, i, nil
}

func parseEdgeTarget(s string, i int, binding knowledgepolicy.DecayProfileBinding) (knowledgepolicy.DecayProfileBinding, int, error) {
	if i >= len(s) || s[i] != '(' {
		return binding, i, localizedError(localization.CypherKnowledgePolicyEdgePatternExpectedFor("'('"), nil)
	}
	i++
	i = kpSkipSpaces(s, i)
	if i < len(s) && s[i] == ')' {
		i++
	}
	i = kpSkipSpaces(s, i)

	if i >= len(s) || s[i] != '-' {
		return binding, i, localizedError(localization.CypherKnowledgePolicyEdgePatternExpectedIn("'-'"), nil)
	}
	i++
	if i >= len(s) || s[i] != '[' {
		return binding, i, localizedError(localization.CypherKnowledgePolicyEdgePatternExpectedIn("'['"), nil)
	}
	i++
	i = kpSkipSpaces(s, i)

	_, i = kpScanIdent(s, i)
	i = kpSkipSpaces(s, i)

	if i < len(s) && s[i] == ':' {
		i++
		edgeType, j := kpScanIdent(s, i)
		if edgeType == "" {
			return binding, i, localizedError(localization.CypherKnowledgePolicyEdgeTypeExpectedAfterColon(), nil)
		}
		binding.TargetEdgeType = edgeType
		binding.IsEdge = true
		i = j
	}

	i = kpSkipSpaces(s, i)
	if i < len(s) && s[i] == ']' {
		i++
	}
	if i < len(s) && s[i] == '-' {
		i++
	}

	i = kpSkipSpaces(s, i)
	if i < len(s) && s[i] == '(' {
		i++
		i = kpSkipSpaces(s, i)
		if i < len(s) && s[i] == ')' {
			i++
		}
	}

	return binding, i, nil
}

func parseBindingApplyBlock(body string, binding *knowledgepolicy.DecayProfileBinding) error {
	i := 0
	i = kpSkipSpaces(body, i)

	for i < len(body) {
		i = kpSkipSpaces(body, i)
		if i >= len(body) {
			break
		}

		if j := cyphertext.KeywordEnd(body, i, "DECAY", cyphertext.CypherWord); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := cyphertext.KeywordEnd(body, j, "PROFILE", cyphertext.CypherWord); k > 0 {
				k = kpSkipSpaces(body, k)
				ref, l := kpScanName(body, k)
				if ref == "" {
					return localizedError(localization.CypherKnowledgePolicyProfileNameExpectedAfter("DECAY PROFILE"), nil)
				}
				binding.ProfileRef = ref
				i = l
				continue
			}
			if k := cyphertext.KeywordEnd(body, j, "VISIBILITY", cyphertext.CypherWord); k > 0 {
				k = kpSkipSpaces(body, k)
				if l := cyphertext.KeywordEnd(body, k, "THRESHOLD", cyphertext.CypherWord); l > 0 {
					k = kpSkipSpaces(body, l)
				}
				f, l, ok := kpScanDecimal[float64](body, k)
				if !ok {
					return localizedError(localization.CypherKnowledgePolicyNumberExpectedAfter("DECAY VISIBILITY THRESHOLD"), nil)
				}
				binding.VisibilityThreshold = &f
				i = l
				continue
			}
			if k := cyphertext.KeywordEnd(body, j, "HALF", cyphertext.CypherWord); k > 0 {
				k = kpSkipSpaces(body, k)
				if l := cyphertext.KeywordEnd(body, k, "LIFE", cyphertext.CypherWord); l > 0 {
					l = kpSkipSpaces(body, l)
					n, m, ok := kpScanDecimal[int64](body, l)
					if !ok {
						return localizedError(localization.CypherKnowledgePolicySecondsExpectedAfter("DECAY HALF LIFE"), nil)
					}
					binding.HalfLifeSeconds = n
					i = m
					continue
				}
			}
			if k := cyphertext.KeywordEnd(body, j, "FLOOR", cyphertext.CypherWord); k > 0 {
				k = kpSkipSpaces(body, k)
				f, l, ok := kpScanDecimal[float64](body, k)
				if !ok {
					return localizedError(localization.CypherKnowledgePolicyNumberExpectedAfter("DECAY FLOOR"), nil)
				}
				binding.ScoreFloor = f
				i = l
				continue
			}
		}

		if j := cyphertext.KeywordEnd(body, i, "NO", cyphertext.CypherWord); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := cyphertext.KeywordEnd(body, j, "DECAY", cyphertext.CypherWord); k > 0 {
				binding.NoDecay = true
				i = k
				continue
			}
		}

		// Plan syntax: <var>.<prop> directives (e.g., n.tenantId NO DECAY)
		if cyphertext.NameStartAt(body, i) {
			ident, j := kpScanIdent(body, i)
			if ident != "" && j < len(body) && body[j] == '.' {
				propName, k := kpScanIdent(body, j+1)
				if propName != "" {
					rule, l, err := parsePropertyRuleDirectives(body, k)
					if err != nil {
						return err
					}
					rule.PropertyPath = propName
					merged := false
					for idx := range binding.PropertyRules {
						if binding.PropertyRules[idx].PropertyPath == propName {
							mergePropertyRule(&binding.PropertyRules[idx], &rule)
							merged = true
							break
						}
					}
					if !merged {
						rule.Order = len(binding.PropertyRules)
						binding.PropertyRules = append(binding.PropertyRules, rule)
					}
					i = l
					continue
				}
			}
		}

		i++
	}

	return nil
}

func mergePropertyRule(existing, incoming *knowledgepolicy.DecayProfilePropertyRule) {
	if incoming.NoDecay {
		existing.NoDecay = true
	}
	if incoming.ProfileRef != "" {
		existing.ProfileRef = incoming.ProfileRef
	}
	if incoming.HalfLifeSeconds != 0 {
		existing.HalfLifeSeconds = incoming.HalfLifeSeconds
	}
	if incoming.ScoreFloor != 0 {
		existing.ScoreFloor = incoming.ScoreFloor
	}
}

func parsePropertyRuleDirectives(s string, i int) (knowledgepolicy.DecayProfilePropertyRule, int, error) {
	rule := knowledgepolicy.DecayProfilePropertyRule{}
	i = kpSkipSpaces(s, i)

	for i < len(s) {
		i = kpSkipSpaces(s, i)
		if i >= len(s) {
			break
		}

		if k := cyphertext.KeywordEnd(s, i, "NO", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "DECAY", cyphertext.CypherWord); l > 0 {
				rule.NoDecay = true
				i = l
				continue
			}
		}

		if k := cyphertext.KeywordEnd(s, i, "DECAY", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := cyphertext.KeywordEnd(s, k, "PROFILE", cyphertext.CypherWord); l > 0 {
				l = kpSkipSpaces(s, l)
				ref, m := kpScanName(s, l)
				if ref != "" {
					rule.ProfileRef = ref
					i = m
					continue
				}
			}
			if l := cyphertext.KeywordEnd(s, k, "HALF", cyphertext.CypherWord); l > 0 {
				l = kpSkipSpaces(s, l)
				if m := cyphertext.KeywordEnd(s, l, "LIFE", cyphertext.CypherWord); m > 0 {
					m = kpSkipSpaces(s, m)
					n, o, ok := kpScanDecimal[int64](s, m)
					if ok {
						rule.HalfLifeSeconds = n
						i = o
						continue
					}
				}
			}
			if l := cyphertext.KeywordEnd(s, k, "FLOOR", cyphertext.CypherWord); l > 0 {
				l = kpSkipSpaces(s, l)
				f, m, ok := kpScanDecimal[float64](s, l)
				if ok {
					rule.ScoreFloor = f
					i = m
					continue
				}
			}
		}

		break
	}

	return rule, i, nil
}

// kpParseKnowledgeName trims spaces, scans a profile/policy name and returns
// the shared name-required error when the next token is not a name.
func kpParseKnowledgeName(s string, i int, statement string, isPolicy bool) (string, int, error) {
	i = kpSkipSpaces(s, i)
	name, i := kpScanName(s, i)
	if name == "" {
		if isPolicy {
			return "", i, localizedError(localization.CypherKnowledgePolicyPolicyNameExpectedAfter(statement), nil)
		}
		return "", i, localizedError(localization.CypherKnowledgePolicyProfileNameExpectedAfter(statement), nil)
	}
	return name, i, nil
}

// kpParseSetOptions parses `SET OPTIONS { … }` at s[i] into a raw-value updates
// map and returns the index after the block. ok is false when no SET keyword is
// present (i is left at the clause start); updates is nil when SET is present
// without an OPTIONS block, so callers can tell a parsed (possibly empty) block
// apart from a missing one.
func kpParseSetOptions(s string, i int) (updates map[string]interface{}, next int, ok bool, err error) {
	return kpParseSetOptionsWith(s, i, func(body string) (map[string]interface{}, error) {
		parsed := make(map[string]interface{})
		err := parseOptionsMap(body, func(key, rawVal string) error {
			parsed[key] = parseRawValue(rawVal)
			return nil
		})
		return parsed, err
	})
}

func kpParseSetOptionsWith(s string, i int, decode func(string) (map[string]interface{}, error)) (updates map[string]interface{}, next int, ok bool, err error) {
	j := cyphertext.KeywordEnd(s, i, "SET", cyphertext.CypherWord)
	if j < 0 {
		return nil, i, false, nil
	}
	j = kpSkipSpaces(s, j)
	k := cyphertext.KeywordEnd(s, j, "OPTIONS", cyphertext.CypherWord)
	if k < 0 {
		return nil, j, true, nil
	}
	body, l := kpScanBraceBlock(s, k)
	if l < 0 {
		return nil, j, true, localizedError(localization.CypherKnowledgePolicyExpectedAfter("{", "SET OPTIONS"), nil)
	}
	updates, err = decode(body)
	if err != nil {
		return nil, j, true, err
	}
	return updates, l, true, nil
}

// kpParseDrop parses the shared `DROP [IF EXISTS] <name>` tail of the profile
// and policy statements.
func kpParseDrop(s string, i int, statement string, isPolicy bool, build func(name string, ifExists bool) interface{}) (interface{}, bool, error) {
	ifExists := false
	i = kpSkipSpaces(s, i)
	if j := cyphertext.KeywordEnd(s, i, "IF", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := cyphertext.KeywordEnd(s, j, "EXISTS", cyphertext.CypherWord); k > 0 {
			ifExists = true
			i = kpSkipSpaces(s, k)
		}
	}
	name, i, err := kpParseKnowledgeName(s, i, statement, isPolicy)
	if err != nil {
		return nil, false, err
	}
	return build(name, ifExists), true, nil
}

func parseAlterDecayProfile(s string, i int) (interface{}, bool, error) {
	name, i, err := kpParseKnowledgeName(s, i, "ALTER DECAY PROFILE", false)
	if err != nil {
		return nil, false, err
	}

	i = kpSkipSpaces(s, i)
	if j := cyphertext.KeywordEnd(s, i, "FOR", cyphertext.CypherWord); j > 0 {
		cmd, _, err := parseDecayProfileBinding(name, s, j)
		if err != nil {
			return nil, false, err
		}
		return &AlterDecayProfileBindingCmd{Binding: cmd.(*CreateDecayProfileBindingCmd).Binding}, true, nil
	}

	updates, next, ok, err := kpParseSetOptionsWith(s, i, parseDecayProfileOptions)
	if err != nil {
		return nil, false, err
	}
	if ok && updates != nil {
		if err := kpRequireStatementEnd(s, next, "ALTER DECAY PROFILE "+name); err != nil {
			return nil, false, err
		}
		return &AlterDecayProfileCmd{Name: name, Updates: updates}, true, nil
	}

	return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("SET OPTIONS", "ALTER DECAY PROFILE "+name), nil)
}

func parseDropDecayProfile(s string, i int) (interface{}, bool, error) {
	return kpParseDrop(s, i, "DROP DECAY PROFILE", false, func(name string, ifExists bool) interface{} {
		return &DropDecayProfileCmd{Name: name, IfExists: ifExists}
	})
}

func parseCreatePromotionProfile(s string, i int) (interface{}, bool, error) {
	name, i, err := kpParseKnowledgeName(s, i, "CREATE PROMOTION PROFILE", false)
	if err != nil {
		return nil, false, err
	}

	i = kpSkipSpaces(s, i)
	if j := cyphertext.KeywordEnd(s, i, "OPTIONS", cyphertext.CypherWord); j < 0 {
		return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfterProfileName("OPTIONS", name), nil)
	} else {
		i = j
	}

	body, j := kpScanBraceBlock(s, i)
	if j < 0 {
		return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("{", "OPTIONS"), nil)
	}
	if err := kpRequireStatementEnd(s, j, "CREATE PROMOTION PROFILE "+name); err != nil {
		return nil, false, err
	}

	profile := knowledgepolicy.PromotionProfileDef{
		Name:    name,
		Enabled: true,
		Scope:   knowledgepolicy.ScopeNode,
	}

	updates, err := parsePromotionProfileOptions(body)
	if err != nil {
		return nil, false, err
	}
	profile, err = storage.ApplyPromotionProfileOptions(profile, updates)
	if err != nil {
		return nil, false, err
	}
	return &CreatePromotionProfileCmd{Profile: profile}, true, nil
}

func parsePromotionProfileOptions(body string) (map[string]interface{}, error) {
	updates := make(map[string]interface{})
	err := parseOptionsMap(body, func(key, rawVal string) error {
		switch lowerASCII(key) {
		case "scope":
			sc := knowledgepolicy.ScopeType(upperASCII(strings.Trim(rawVal, "'\"")))
			if !knowledgepolicy.ValidScopeTypes[sc] {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("scope", rawVal, true), nil)
			}
			updates["scope"] = string(sc)
		case "multiplier":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("multiplier", rawVal, false), err)
			}
			updates["multiplier"] = f
		case "scorefloor":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("scoreFloor", rawVal, false), err)
			}
			updates["scoreFloor"] = f
		case "scorecap":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("scoreCap", rawVal, false), err)
			}
			updates["scoreCap"] = f
		case "enabled":
			b, err := strconv.ParseBool(rawVal)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("enabled", rawVal, false), err)
			}
			updates["enabled"] = b
		default:
			return localizedError(localization.CypherKnowledgePolicyUnknownOption(key), nil)
		}
		return nil
	})
	return updates, err
}

func parseAlterPromotionProfile(s string, i int) (interface{}, bool, error) {
	name, i, err := kpParseKnowledgeName(s, i, "ALTER PROMOTION PROFILE", false)
	if err != nil {
		return nil, false, err
	}

	i = kpSkipSpaces(s, i)
	updates, next, ok, err := kpParseSetOptionsWith(s, i, parsePromotionProfileOptions)
	if err != nil {
		return nil, false, err
	}
	if ok && updates != nil {
		if err := kpRequireStatementEnd(s, next, "ALTER PROMOTION PROFILE "+name); err != nil {
			return nil, false, err
		}
		return &AlterPromotionProfileCmd{Name: name, Updates: updates}, true, nil
	}

	return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("SET OPTIONS", "ALTER PROMOTION PROFILE "+name), nil)
}

func parseDropPromotionProfile(s string, i int) (interface{}, bool, error) {
	return kpParseDrop(s, i, "DROP PROMOTION PROFILE", false, func(name string, ifExists bool) interface{} {
		return &DropPromotionProfileCmd{Name: name, IfExists: ifExists}
	})
}

func parseCreatePromotionPolicy(source policyDDLSource, i int) (interface{}, bool, error) {
	s := source.Text
	i = kpSkipSpaces(s, i)
	parseGuard := func(index int) (int, bool, error) {
		if next := cyphertext.KeywordEnd(s, index, "IF", cyphertext.CypherWord); next > 0 {
			next = kpSkipSpaces(s, next)
			if next = cyphertext.KeywordEnd(s, next, "NOT", cyphertext.CypherWord); next > 0 {
				next = kpSkipSpaces(s, next)
				if next = cyphertext.KeywordEnd(s, next, "EXISTS", cyphertext.CypherWord); next > 0 {
					return kpSkipSpaces(s, next), true, nil
				}
			}
			return index, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("NOT EXISTS", "IF"), nil)
		}
		return index, false, nil
	}
	i, ifNotExists, err := parseGuard(i)
	if err != nil {
		return nil, false, err
	}
	name, i, err := kpParseKnowledgeName(s, i, "CREATE PROMOTION POLICY", true)
	if err != nil {
		return nil, false, err
	}
	i = kpSkipSpaces(s, i)
	if !ifNotExists {
		i, ifNotExists, err = parseGuard(i)
		if err != nil {
			return nil, false, err
		}
	}

	if cyphertext.KeywordEnd(s, i, "FOR", cyphertext.CypherWord) < 0 {
		return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("FOR", "CREATE PROMOTION POLICY "+name), nil)
	}
	policy, err := parsePromotionPolicyDefinition(name, source, i)
	if err != nil {
		return nil, false, err
	}

	return &CreatePromotionPolicyCmd{Policy: policy, IfNotExists: ifNotExists}, true, nil
}

func parsePromotionPolicyDefinition(name string, source policyDDLSource, i int) (knowledgepolicy.PromotionPolicyDef, error) {
	s := source.Text
	i = kpSkipSpaces(s, i)
	policy := knowledgepolicy.PromotionPolicyDef{Name: name, Enabled: true}

	if j := cyphertext.KeywordEnd(s, i, "FOR", cyphertext.CypherWord); j > 0 {
		i = kpSkipSpaces(s, j)
		binding := knowledgepolicy.DecayProfileBinding{Name: name}
		var err error
		binding, i, err = parseForTarget(s, i, binding)
		if err != nil {
			return policy, err
		}
		policy.TargetLabels = binding.TargetLabels
		policy.TargetEdgeType = binding.TargetEdgeType
		policy.IsWildcard = binding.IsWildcard
		policy.IsEdge = binding.IsEdge
	}

	i = kpSkipSpaces(s, i)

	if j := cyphertext.KeywordEnd(s, i, "APPLY", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		body, k := kpScanBraceBlock(s, j)
		if k < 0 {
			return policy, localizedError(localization.CypherKnowledgePolicyExpectedAfter("{", "APPLY"), nil)
		}
		i = k

		if err := parsePolicyApplyBlock(source.window(k-1-len(body), k-1), &policy); err != nil {
			return policy, err
		}
	}
	if err := kpRequireStatementEnd(s, i, "PROMOTION POLICY "+name); err != nil {
		return policy, err
	}

	return policy, nil
}

func parsePolicyApplyBlock(source policyDDLSource, policy *knowledgepolicy.PromotionPolicyDef) error {
	body := source.Text
	i := 0
	for i < len(body) {
		i = kpSkipSpaces(body, i)
		if i >= len(body) {
			break
		}

		if j := cyphertext.KeywordEnd(body, i, "ON", cyphertext.CypherWord); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := cyphertext.KeywordEnd(body, j, "ACCESS", cyphertext.CypherWord); k > 0 {
				k = kpSkipSpaces(body, k)
				accessBody, l := kpScanBraceBlock(body, k)
				if l < 0 {
					return localizedError(localization.CypherKnowledgePolicyExpectedAfter("{", "ON ACCESS"), nil)
				}
				onAccess, err := parseOnAccessBlock(source.window(l-1-len(accessBody), l-1))
				if err != nil {
					return err
				}
				policy.OnAccess = onAccess
				i = l
				continue
			}
		}

		if j := cyphertext.KeywordEnd(body, i, "WHEN", cyphertext.CypherWord); j > 0 {
			clause, l, err := parsePolicyWhenClause(source, j)
			if err != nil {
				return err
			}
			clause.Order = len(policy.WhenClauses)
			policy.WhenClauses = append(policy.WhenClauses, clause)
			i = l
			continue
		}

		i++
	}

	return nil
}

func parseOnAccessBlock(source policyDDLSource) (*knowledgepolicy.PromotionPolicyOnAccess, error) {
	body := source.Text
	onAccess := &knowledgepolicy.PromotionPolicyOnAccess{}
	i := 0

	for i < len(body) {
		i = kpSkipSpaces(body, i)
		if i >= len(body) {
			break
		}

		var kalmanCfg *knowledgepolicy.KalmanConfig

		if j := cyphertext.KeywordEnd(body, i, "WITH", cyphertext.CypherWord); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := cyphertext.KeywordEnd(body, j, "KALMAN", cyphertext.CypherWord); k > 0 {
				k = kpSkipSpaces(body, k)

				kalmanCfg = &knowledgepolicy.KalmanConfig{
					Mode:          knowledgepolicy.KalmanModeAuto,
					Q:             0.1,
					R:             88.0,
					VarianceScale: 10.0,
					WindowSize:    32,
				}

				if k < len(body) && body[k] == '{' {
					cfgBody, l := kpScanBraceBlock(body, k)
					if l < 0 {
						return nil, localizedError(localization.CypherKnowledgePolicyKalmanClosingBraceExpected(), nil)
					}
					if err := parseKalmanConfigBlock(cfgBody, kalmanCfg); err != nil {
						return nil, err
					}
					k = l
				}

				i = kpSkipSpaces(body, k)
			} else {
				i = j
				continue
			}
		}

		if j := cyphertext.KeywordEnd(body, i, "SET", cyphertext.CypherWord); j > 0 {
			j = kpSkipSpaces(body, j)
			expr, l := scanToNextStatement(body, j)
			if expr == "" {
				return nil, localizedError(localization.CypherKnowledgePolicyExpressionExpectedAfterSet(), nil)
			}
			rawExpression, err := source.storedText(j, j+len(expr))
			if err != nil {
				return nil, err
			}
			mut := knowledgepolicy.OnAccessMutation{
				Expression: rawExpression,
				Kalman:     kalmanCfg,
			}
			onAccess.Mutations = append(onAccess.Mutations, mut)
			i = l
			continue
		}

		i++
	}

	return onAccess, nil
}

func parseKalmanConfigBlock(body string, cfg *knowledgepolicy.KalmanConfig) error {
	hasR := false
	if err := parseOptionsMap(body, func(key, rawVal string) error {
		switch lowerASCII(key) {
		case "q":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("q", rawVal, false), err)
			}
			cfg.Q = f
		case "r":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("r", rawVal, false), err)
			}
			cfg.R = f
			hasR = true
		case "variancescale":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("varianceScale", rawVal, false), err)
			}
			cfg.VarianceScale = f
		case "windowsize":
			n, err := strconv.Atoi(rawVal)
			if err != nil {
				return localizedError(localization.CypherKnowledgePolicyInvalidValue("windowSize", rawVal, false), err)
			}
			cfg.WindowSize = n
		default:
			return localizedError(localization.CypherKnowledgePolicyUnknownKalmanConfigKey(key), nil)
		}
		return nil
	}); err != nil {
		return err
	}

	if hasR {
		cfg.Mode = knowledgepolicy.KalmanModeManual
	}

	return nil
}

func scanToNextStatement(s string, i int) (string, int) {
	start := i
	boundary := len(s)
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	for _, keyword := range []string{"SET", "WITH"} {
		if next := keywordIndexFrom(s, keyword, start, opts); next >= 0 && next < boundary {
			boundary = next
		}
	}
	for i < boundary {
		if end, opaque := cyphertext.SkipToken(s, i); opaque {
			i = end
			continue
		}
		var closer rune
		switch s[i] {
		case '(':
			closer = ')'
		case '[':
			closer = ']'
		case '{':
			closer = '}'
		}
		if closer != 0 {
			if end := cyphertext.FindMatchingDelimiter(s, i, rune(s[i]), closer, cyphertext.OpeningDelimiter); end >= 0 {
				i = end + 1
				continue
			}
		}
		if s[i] == '\n' || s[i] == ';' {
			return s[start:i], i + 1
		}
		i++
	}
	return s[start:boundary], boundary
}

func parsePolicyWhenClause(source policyDDLSource, i int) (knowledgepolicy.PromotionPolicyWhenClause, int, error) {
	s := source.Text
	i = kpSkipSpaces(s, i)
	clause := knowledgepolicy.PromotionPolicyWhenClause{}

	predStart := i
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	opts.Boundary = keywordBoundaryWhitespace
	applyIdx := keywordIndexFrom(s, "APPLY", predStart, opts)

	if applyIdx < 0 {
		return clause, i, localizedError(localization.CypherKnowledgePolicyExpectedAfter("APPLY", "WHEN predicate"), nil)
	}

	rawPredicate, err := source.storedText(predStart, applyIdx)
	if err != nil {
		return clause, i, err
	}
	clause.Predicate = rawPredicate
	i = applyIdx

	if j := cyphertext.KeywordEnd(s, i, "APPLY", cyphertext.CypherWord); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := cyphertext.KeywordEnd(s, j, "PROFILE", cyphertext.CypherWord); k > 0 {
			k = kpSkipSpaces(s, k)
			ref, l := kpScanQuotedString(s, k)
			if ref == "" {
				ref, l = kpScanIdent(s, k)
			}
			if ref == "" {
				return clause, i, localizedError(localization.CypherKnowledgePolicyProfileNameExpectedAfter("APPLY PROFILE"), nil)
			}
			clause.ProfileRef = ref
			i = l
		} else {
			return clause, i, localizedError(localization.CypherKnowledgePolicyExpectedAfter("PROFILE", "APPLY"), nil)
		}
	}

	return clause, i, nil
}

func parseAlterPromotionPolicy(source policyDDLSource, i int) (interface{}, bool, error) {
	s := source.Text
	name, i, err := kpParseKnowledgeName(s, i, "ALTER PROMOTION POLICY", true)
	if err != nil {
		return nil, false, err
	}

	i = kpSkipSpaces(s, i)
	if cyphertext.KeywordEnd(s, i, "FOR", cyphertext.CypherWord) > 0 {
		policy, err := parsePromotionPolicyDefinition(name, source, i)
		if err != nil {
			return nil, false, err
		}
		return &AlterPromotionPolicyDefinitionCmd{Policy: policy}, true, nil
	}

	updates := make(map[string]interface{})
	if parsed, next, ok, err := kpParseSetOptions(s, i); err != nil {
		return nil, false, err
	} else if ok {
		if parsed == nil {
			return nil, false, localizedError(localization.CypherKnowledgePolicyExpectedAfter("OPTIONS { ... }", "SET"), nil)
		}
		if err := storage.ValidatePromotionPolicyOptions(parsed); err != nil {
			return nil, false, err
		}
		i = kpSkipSpaces(s, next)
		updates = parsed
	}

	if j := cyphertext.KeywordEnd(s, i, "ENABLE", cyphertext.CypherWord); j > 0 {
		updates["enabled"] = true
		i = j
	} else if j := cyphertext.KeywordEnd(s, i, "DISABLE", cyphertext.CypherWord); j > 0 {
		updates["enabled"] = false
		i = j
	}
	if err := kpRequireStatementEnd(s, i, "ALTER PROMOTION POLICY "+name); err != nil {
		return nil, false, err
	}

	return &AlterPromotionPolicyCmd{Name: name, Updates: updates}, true, nil
}

func parseDropPromotionPolicy(s string, i int) (interface{}, bool, error) {
	return kpParseDrop(s, i, "DROP PROMOTION POLICY", true, func(name string, ifExists bool) interface{} {
		return &DropPromotionPolicyCmd{Name: name, IfExists: ifExists}
	})
}

// parseOptionsMap parses a comma- or newline-separated list of key: value pairs.
func parseOptionsMap(body string, handler func(key, rawVal string) error) error {
	i := 0
	for i < len(body) {
		i = kpSkipSpaces(body, i)
		if i >= len(body) {
			break
		}

		key, j := kpScanIdent(body, i)
		if key == "" {
			if body[i] == ',' || body[i] == ';' {
				i++
				continue
			}
			i++
			continue
		}
		i = kpSkipSpaces(body, j)

		if i < len(body) && body[i] == ':' {
			i++
		} else if i < len(body) && body[i] == '=' {
			i++
		}

		i = kpSkipSpaces(body, i)

		rawVal, l := scanOptionValue(body, i)
		i = l

		if err := handler(key, strings.TrimSpace(rawVal)); err != nil {
			return err
		}

		i = kpSkipSpaces(body, i)
		if i < len(body) && (body[i] == ',' || body[i] == ';') {
			i++
		}
	}
	return nil
}

func scanOptionValue(s string, i int) (string, int) {
	if i >= len(s) {
		return "", i
	}

	if isCypherStringQuoteByte(s[i]) {
		val, j := kpScanQuotedString(s, i)
		if j >= 0 {
			return val, j
		}
	}

	start := i
	for i < len(s) && s[i] != ',' && s[i] != ';' && s[i] != '\n' && s[i] != '}' {
		i++
	}
	return s[start:i], i
}

func parseRawValue(raw string) interface{} {
	raw = strings.TrimSpace(raw)

	if b, err := strconv.ParseBool(raw); err == nil {
		return b
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}

	return strings.Trim(raw, "'\"")
}
