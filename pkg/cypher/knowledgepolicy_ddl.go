package cypher

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// Allocation-free keyword scanning helpers for DDL parsing.
// Same pattern as pkg/storage/constraint_contracts.go (cc* prefix),
// using kp* prefix to avoid collisions.

func kpSkipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func kpIsIdentStart(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '_'
}

func kpIsIdentByte(b byte) bool {
	return kpIsIdentStart(b) || (b >= '0' && b <= '9')
}

func kpScanIdent(s string, i int) (string, int) {
	if i >= len(s) || !kpIsIdentStart(s[i]) {
		return "", i
	}
	start := i
	for i < len(s) && kpIsIdentByte(s[i]) {
		i++
	}
	return s[start:i], i
}

func kpMatchKeywordAt(s string, i int, kw string) int {
	if i+len(kw) > len(s) {
		return -1
	}
	for k := 0; k < len(kw); k++ {
		sc, kc := s[i+k], kw[k]
		if sc >= 'a' && sc <= 'z' {
			sc -= 'a' - 'A'
		}
		if kc >= 'a' && kc <= 'z' {
			kc -= 'a' - 'A'
		}
		if sc != kc {
			return -1
		}
	}
	end := i + len(kw)
	if end < len(s) && kpIsIdentByte(s[end]) {
		return -1
	}
	return end
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
	q := s[i]
	if q != '\'' && q != '"' {
		return "", i
	}
	i++
	start := i
	for i < len(s) && s[i] != q {
		if s[i] == '\\' {
			i++
		}
		i++
	}
	if i >= len(s) {
		return "", -1
	}
	val := s[start:i]
	return val, i + 1
}

func kpScanNumber(s string, i int) (float64, int, bool) {
	if i >= len(s) {
		return 0, i, false
	}
	start := i
	if s[i] == '-' || s[i] == '+' {
		i++
	}
	if i >= len(s) || (s[i] < '0' && s[i] != '.') || s[i] > '9' {
		return 0, start, false
	}
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	f, err := strconv.ParseFloat(s[start:i], 64)
	if err != nil {
		return 0, start, false
	}
	return f, i, true
}

func kpScanInt(s string, i int) (int64, int, bool) {
	if i >= len(s) {
		return 0, i, false
	}
	start := i
	if s[i] == '-' || s[i] == '+' {
		i++
	}
	if i >= len(s) || s[i] < '0' || s[i] > '9' {
		return 0, start, false
	}
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	n, err := strconv.ParseInt(s[start:i], 10, 64)
	if err != nil {
		return 0, start, false
	}
	return n, i, true
}

func kpScanBool(s string, i int) (bool, int, bool) {
	if j := kpMatchKeywordAt(s, i, "TRUE"); j > 0 {
		return true, j, true
	}
	if j := kpMatchKeywordAt(s, i, "FALSE"); j > 0 {
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
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
		case '\'', '"':
			_, j := kpScanQuotedString(s, i)
			if j < 0 {
				return "", -1
			}
			i = j
			continue
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
	Policy knowledgepolicy.PromotionPolicyDef
}

type AlterPromotionPolicyCmd struct {
	Name    string
	Updates map[string]interface{}
}

type DropPromotionPolicyCmd struct {
	Name     string
	IfExists bool
}

type ShowPromotionPoliciesCmd struct{}

// ParseKnowledgePolicyDDL attempts to parse a knowledge-layer DDL statement.
// Returns (command, true, nil) on success, (nil, false, nil) if the input
// is not a knowledge-layer DDL statement, or (nil, false, err) on parse error.
func ParseKnowledgePolicyDDL(stmt string) (interface{}, bool, error) {
	s := strings.TrimSpace(stmt)
	i := 0
	i = kpSkipSpaces(s, i)

	if j := kpMatchKeywordAt(s, i, "CREATE"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "DECAY"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILE"); l > 0 {
				return parseCreateDecayProfile(s, l)
			}
		}
		if k := kpMatchKeywordAt(s, j, "PROMOTION"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILE"); l > 0 {
				return parseCreatePromotionProfile(s, l)
			}
			if l := kpMatchKeywordAt(s, k, "POLICY"); l > 0 {
				return parseCreatePromotionPolicy(s, l)
			}
		}
		return nil, false, nil
	}

	if j := kpMatchKeywordAt(s, i, "ALTER"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "DECAY"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILE"); l > 0 {
				return parseAlterDecayProfile(s, l)
			}
		}
		if k := kpMatchKeywordAt(s, j, "PROMOTION"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILE"); l > 0 {
				return parseAlterPromotionProfile(s, l)
			}
			if l := kpMatchKeywordAt(s, k, "POLICY"); l > 0 {
				return parseAlterPromotionPolicy(s, l)
			}
		}
		return nil, false, nil
	}

	if j := kpMatchKeywordAt(s, i, "DROP"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "DECAY"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILE"); l > 0 {
				return parseDropDecayProfile(s, l)
			}
		}
		if k := kpMatchKeywordAt(s, j, "PROMOTION"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILE"); l > 0 {
				return parseDropPromotionProfile(s, l)
			}
			if l := kpMatchKeywordAt(s, k, "POLICY"); l > 0 {
				return parseDropPromotionPolicy(s, l)
			}
		}
		return nil, false, nil
	}

	if j := kpMatchKeywordAt(s, i, "SHOW"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "DECAY"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILES"); l > 0 {
				return &ShowDecayProfilesCmd{}, true, nil
			}
		}
		if k := kpMatchKeywordAt(s, j, "PROMOTION"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "PROFILES"); l > 0 {
				return &ShowPromotionProfilesCmd{}, true, nil
			}
			if l := kpMatchKeywordAt(s, k, "POLICIES"); l > 0 {
				return &ShowPromotionPoliciesCmd{}, true, nil
			}
		}
		return nil, false, nil
	}

	return nil, false, nil
}

func parseCreateDecayProfile(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)
	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected profile name after CREATE DECAY PROFILE")
	}
	i = kpSkipSpaces(s, i)

	if j := kpMatchKeywordAt(s, i, "OPTIONS"); j > 0 {
		return parseDecayProfileBundleOptions(name, s, j)
	}

	if j := kpMatchKeywordAt(s, i, "FOR"); j > 0 {
		return parseDecayProfileBinding(name, s, j)
	}

	return nil, false, fmt.Errorf("expected OPTIONS or FOR after profile name %q", name)
}

func kpScanQuotedName(s string, i int) (string, int) {
	if i >= len(s) {
		return "", i
	}
	if s[i] == '\'' || s[i] == '"' {
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
		return nil, false, fmt.Errorf("expected { after OPTIONS")
	}
	_ = j

	bundle := knowledgepolicy.DecayProfileBundle{
		Name:    name,
		Enabled: true,
	}

	if err := parseOptionsMap(body, func(key, rawVal string) error {
		switch strings.ToLower(key) {
		case "halflifeseconds":
			n, err := strconv.ParseInt(rawVal, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid halfLifeSeconds: %s", rawVal)
			}
			bundle.HalfLifeSeconds = n
		case "visibilitythreshold":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid visibilityThreshold: %s", rawVal)
			}
			bundle.VisibilityThreshold = f
		case "scorefloor":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid scoreFloor: %s", rawVal)
			}
			bundle.ScoreFloor = f
		case "function":
			fn := knowledgepolicy.DecayFunction(strings.Trim(rawVal, "'\""))
			if !knowledgepolicy.ValidDecayFunctions[fn] {
				return fmt.Errorf("invalid decay function: %q", rawVal)
			}
			bundle.Function = fn
		case "scope":
			sc := knowledgepolicy.ScopeType(strings.ToUpper(strings.Trim(rawVal, "'\"")))
			if !knowledgepolicy.ValidScopeTypes[sc] {
				return fmt.Errorf("invalid scope: %q", rawVal)
			}
			bundle.Scope = sc
		case "decayenabled":
			b, err := strconv.ParseBool(rawVal)
			if err != nil {
				return fmt.Errorf("invalid decayEnabled: %s", rawVal)
			}
			bundle.DecayEnabled = b
		case "scorefrom":
			mode := knowledgepolicy.ScoreFromMode(strings.ToUpper(strings.Trim(rawVal, "'\"")))
			if !knowledgepolicy.ValidScoreFromModes[mode] {
				return fmt.Errorf("invalid scoreFrom: %q", rawVal)
			}
			bundle.ScoreFrom = mode
		case "scorefromproperty":
			bundle.ScoreFromProperty = strings.Trim(rawVal, "'\"")
		case "enabled":
			b, err := strconv.ParseBool(rawVal)
			if err != nil {
				return fmt.Errorf("invalid enabled: %s", rawVal)
			}
			bundle.Enabled = b
		default:
			return fmt.Errorf("unknown option: %q", key)
		}
		return nil
	}); err != nil {
		return nil, false, err
	}

	return &CreateDecayProfileBundleCmd{Bundle: bundle}, true, nil
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

	if j := kpMatchKeywordAt(s, i, "APPLY"); j > 0 {
		j = kpSkipSpaces(s, j)
		body, k := kpScanBraceBlock(s, j)
		if k < 0 {
			return nil, false, fmt.Errorf("expected { after APPLY")
		}
		_ = k

		if err := parseBindingApplyBlock(body, &binding); err != nil {
			return nil, false, err
		}
	}

	return &CreateDecayProfileBindingCmd{Binding: binding}, true, nil
}

func parseForTarget(s string, i int, binding knowledgepolicy.DecayProfileBinding) (knowledgepolicy.DecayProfileBinding, int, error) {
	i = kpSkipSpaces(s, i)

	if i >= len(s) || s[i] != '(' {
		return binding, i, nil
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
				return binding, i, fmt.Errorf("expected label after ':'")
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
		return binding, i, fmt.Errorf("expected '(' for edge pattern")
	}
	i++
	i = kpSkipSpaces(s, i)
	if i < len(s) && s[i] == ')' {
		i++
	}
	i = kpSkipSpaces(s, i)

	if i >= len(s) || s[i] != '-' {
		return binding, i, fmt.Errorf("expected '-' in edge pattern")
	}
	i++
	if i >= len(s) || s[i] != '[' {
		return binding, i, fmt.Errorf("expected '[' in edge pattern")
	}
	i++
	i = kpSkipSpaces(s, i)

	_, i = kpScanIdent(s, i)
	i = kpSkipSpaces(s, i)

	if i < len(s) && s[i] == ':' {
		i++
		edgeType, j := kpScanIdent(s, i)
		if edgeType == "" {
			return binding, i, fmt.Errorf("expected edge type after ':'")
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

		if j := kpMatchKeywordAt(body, i, "DECAY"); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := kpMatchKeywordAt(body, j, "PROFILE"); k > 0 {
				k = kpSkipSpaces(body, k)
				ref, l := kpScanName(body, k)
				if ref == "" {
					return fmt.Errorf("expected profile name after DECAY PROFILE")
				}
				binding.ProfileRef = ref
				i = l
				continue
			}
		}

		if j := kpMatchKeywordAt(body, i, "NO"); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := kpMatchKeywordAt(body, j, "DECAY"); k > 0 {
				binding.NoDecay = true
				i = k
				continue
			}
		}

		if j := kpMatchKeywordAt(body, i, "VISIBILITY"); j > 0 {
			j = kpSkipSpaces(body, j)
			f, l, ok := kpScanNumber(body, j)
			if !ok {
				return fmt.Errorf("expected number after VISIBILITY")
			}
			binding.VisibilityThreshold = &f
			i = l
			continue
		}

		if j := kpMatchKeywordAt(body, i, "PROPERTY"); j > 0 {
			rule, l, err := parsePropertyRule(body, j)
			if err != nil {
				return err
			}
			rule.Order = len(binding.PropertyRules)
			binding.PropertyRules = append(binding.PropertyRules, rule)
			i = l
			continue
		}

		i++
	}

	return nil
}

func parsePropertyRule(s string, i int) (knowledgepolicy.DecayProfilePropertyRule, int, error) {
	i = kpSkipSpaces(s, i)
	propPath, j := kpScanIdent(s, i)
	if propPath == "" {
		propPath, j = kpScanQuotedString(s, i)
		if propPath == "" {
			return knowledgepolicy.DecayProfilePropertyRule{}, i, fmt.Errorf("expected property path after PROPERTY")
		}
	}

	rule := knowledgepolicy.DecayProfilePropertyRule{
		PropertyPath: propPath,
	}

	i = kpSkipSpaces(s, j)

	for i < len(s) {
		i = kpSkipSpaces(s, i)
		if i >= len(s) {
			break
		}

		if k := kpMatchKeywordAt(s, i, "NO"); k > 0 {
			k = kpSkipSpaces(s, k)
			if l := kpMatchKeywordAt(s, k, "DECAY"); l > 0 {
				rule.NoDecay = true
				i = l
				continue
			}
		}

		if k := kpMatchKeywordAt(s, i, "PROFILE"); k > 0 {
			k = kpSkipSpaces(s, k)
			ref, l := kpScanName(s, k)
			if ref != "" {
				rule.ProfileRef = ref
				i = l
				continue
			}
		}

		if k := kpMatchKeywordAt(s, i, "HALFLIFE"); k > 0 {
			k = kpSkipSpaces(s, k)
			n, l, ok := kpScanInt(s, k)
			if ok {
				rule.HalfLifeSeconds = n
				i = l
				continue
			}
		}

		if k := kpMatchKeywordAt(s, i, "FLOOR"); k > 0 {
			k = kpSkipSpaces(s, k)
			f, l, ok := kpScanNumber(s, k)
			if ok {
				rule.ScoreFloor = f
				i = l
				continue
			}
		}

		break
	}

	return rule, i, nil
}

func parseAlterDecayProfile(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)
	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected profile name after ALTER DECAY PROFILE")
	}

	i = kpSkipSpaces(s, i)
	if j := kpMatchKeywordAt(s, i, "SET"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "OPTIONS"); k > 0 {
			body, l := kpScanBraceBlock(s, k)
			if l < 0 {
				return nil, false, fmt.Errorf("expected { after SET OPTIONS")
			}
			_ = l
			updates := make(map[string]interface{})
			if err := parseOptionsMap(body, func(key, rawVal string) error {
				updates[key] = parseRawValue(rawVal)
				return nil
			}); err != nil {
				return nil, false, err
			}
			return &AlterDecayProfileCmd{Name: name, Updates: updates}, true, nil
		}
	}

	return nil, false, fmt.Errorf("expected SET OPTIONS after ALTER DECAY PROFILE %s", name)
}

func parseDropDecayProfile(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)

	ifExists := false
	if j := kpMatchKeywordAt(s, i, "IF"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "EXISTS"); k > 0 {
			ifExists = true
			i = kpSkipSpaces(s, k)
		}
	}

	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected profile name after DROP DECAY PROFILE")
	}

	return &DropDecayProfileCmd{Name: name, IfExists: ifExists}, true, nil
}

func parseCreatePromotionProfile(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)
	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected profile name after CREATE PROMOTION PROFILE")
	}

	i = kpSkipSpaces(s, i)
	if j := kpMatchKeywordAt(s, i, "OPTIONS"); j < 0 {
		return nil, false, fmt.Errorf("expected OPTIONS after profile name %q", name)
	} else {
		i = j
	}

	body, j := kpScanBraceBlock(s, i)
	if j < 0 {
		return nil, false, fmt.Errorf("expected { after OPTIONS")
	}
	_ = j

	profile := knowledgepolicy.PromotionProfileDef{
		Name:    name,
		Enabled: true,
	}

	if err := parseOptionsMap(body, func(key, rawVal string) error {
		switch strings.ToLower(key) {
		case "scope":
			sc := knowledgepolicy.ScopeType(strings.ToUpper(strings.Trim(rawVal, "'\"")))
			if !knowledgepolicy.ValidScopeTypes[sc] {
				return fmt.Errorf("invalid scope: %q", rawVal)
			}
			profile.Scope = sc
		case "multiplier":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid multiplier: %s", rawVal)
			}
			profile.Multiplier = f
		case "scorefloor":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid scoreFloor: %s", rawVal)
			}
			profile.ScoreFloor = f
		case "scorecap":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid scoreCap: %s", rawVal)
			}
			profile.ScoreCap = f
		case "enabled":
			b, err := strconv.ParseBool(rawVal)
			if err != nil {
				return fmt.Errorf("invalid enabled: %s", rawVal)
			}
			profile.Enabled = b
		default:
			return fmt.Errorf("unknown option: %q", key)
		}
		return nil
	}); err != nil {
		return nil, false, err
	}

	return &CreatePromotionProfileCmd{Profile: profile}, true, nil
}

func parseAlterPromotionProfile(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)
	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected profile name after ALTER PROMOTION PROFILE")
	}

	i = kpSkipSpaces(s, i)
	if j := kpMatchKeywordAt(s, i, "SET"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "OPTIONS"); k > 0 {
			body, l := kpScanBraceBlock(s, k)
			if l < 0 {
				return nil, false, fmt.Errorf("expected { after SET OPTIONS")
			}
			_ = l
			updates := make(map[string]interface{})
			if err := parseOptionsMap(body, func(key, rawVal string) error {
				updates[key] = parseRawValue(rawVal)
				return nil
			}); err != nil {
				return nil, false, err
			}
			return &AlterPromotionProfileCmd{Name: name, Updates: updates}, true, nil
		}
	}

	return nil, false, fmt.Errorf("expected SET OPTIONS after ALTER PROMOTION PROFILE %s", name)
}

func parseDropPromotionProfile(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)

	ifExists := false
	if j := kpMatchKeywordAt(s, i, "IF"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "EXISTS"); k > 0 {
			ifExists = true
			i = kpSkipSpaces(s, k)
		}
	}

	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected profile name after DROP PROMOTION PROFILE")
	}

	return &DropPromotionProfileCmd{Name: name, IfExists: ifExists}, true, nil
}

func parseCreatePromotionPolicy(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)
	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected policy name after CREATE PROMOTION POLICY")
	}

	i = kpSkipSpaces(s, i)

	policy := knowledgepolicy.PromotionPolicyDef{
		Name:    name,
		Enabled: true,
	}

	if j := kpMatchKeywordAt(s, i, "FOR"); j > 0 {
		i = kpSkipSpaces(s, j)
		binding := knowledgepolicy.DecayProfileBinding{Name: name}
		var err error
		binding, i, err = parseForTarget(s, i, binding)
		if err != nil {
			return nil, false, err
		}
		policy.TargetLabels = binding.TargetLabels
		policy.TargetEdgeType = binding.TargetEdgeType
		policy.IsWildcard = binding.IsWildcard
		policy.IsEdge = binding.IsEdge
	}

	i = kpSkipSpaces(s, i)

	if j := kpMatchKeywordAt(s, i, "APPLY"); j > 0 {
		j = kpSkipSpaces(s, j)
		body, k := kpScanBraceBlock(s, j)
		if k < 0 {
			return nil, false, fmt.Errorf("expected { after APPLY")
		}
		_ = k

		if err := parsePolicyApplyBlock(body, &policy); err != nil {
			return nil, false, err
		}
	}

	return &CreatePromotionPolicyCmd{Policy: policy}, true, nil
}

func parsePolicyApplyBlock(body string, policy *knowledgepolicy.PromotionPolicyDef) error {
	i := 0
	for i < len(body) {
		i = kpSkipSpaces(body, i)
		if i >= len(body) {
			break
		}

		if j := kpMatchKeywordAt(body, i, "ON"); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := kpMatchKeywordAt(body, j, "ACCESS"); k > 0 {
				k = kpSkipSpaces(body, k)
				accessBody, l := kpScanBraceBlock(body, k)
				if l < 0 {
					return fmt.Errorf("expected { after ON ACCESS")
				}
				onAccess, err := parseOnAccessBlock(accessBody)
				if err != nil {
					return err
				}
				policy.OnAccess = onAccess
				i = l
				continue
			}
		}

		if j := kpMatchKeywordAt(body, i, "WHEN"); j > 0 {
			clause, l, err := parsePolicyWhenClause(body, j)
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

func parseOnAccessBlock(body string) (*knowledgepolicy.PromotionPolicyOnAccess, error) {
	onAccess := &knowledgepolicy.PromotionPolicyOnAccess{}
	i := 0

	for i < len(body) {
		i = kpSkipSpaces(body, i)
		if i >= len(body) {
			break
		}

		var kalmanCfg *knowledgepolicy.KalmanConfig

		if j := kpMatchKeywordAt(body, i, "WITH"); j > 0 {
			j = kpSkipSpaces(body, j)
			if k := kpMatchKeywordAt(body, j, "KALMAN"); k > 0 {
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
						return nil, fmt.Errorf("expected } in KALMAN config block")
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

		if j := kpMatchKeywordAt(body, i, "SET"); j > 0 {
			j = kpSkipSpaces(body, j)
			expr, l := scanToNextStatement(body, j)
			if expr == "" {
				return nil, fmt.Errorf("expected expression after SET")
			}
			mut := knowledgepolicy.OnAccessMutation{
				Expression: strings.TrimSpace(expr),
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
		switch strings.ToLower(key) {
		case "q":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid q: %s", rawVal)
			}
			cfg.Q = f
		case "r":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid r: %s", rawVal)
			}
			cfg.R = f
			hasR = true
		case "variancescale":
			f, err := strconv.ParseFloat(rawVal, 64)
			if err != nil {
				return fmt.Errorf("invalid varianceScale: %s", rawVal)
			}
			cfg.VarianceScale = f
		case "windowsize":
			n, err := strconv.ParseInt(rawVal, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid windowSize: %s", rawVal)
			}
			cfg.WindowSize = int(n)
		default:
			return fmt.Errorf("unknown Kalman config key: %q", key)
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
	for i < len(s) {
		if s[i] == '\n' || s[i] == ';' {
			expr := s[start:i]
			return expr, i + 1
		}
		if j := kpMatchKeywordAt(s, i, "SET"); j > 0 {
			return s[start:i], i
		}
		if j := kpMatchKeywordAt(s, i, "WITH"); j > 0 {
			return s[start:i], i
		}
		i++
	}
	return s[start:i], i
}

func parsePolicyWhenClause(s string, i int) (knowledgepolicy.PromotionPolicyWhenClause, int, error) {
	i = kpSkipSpaces(s, i)
	clause := knowledgepolicy.PromotionPolicyWhenClause{}

	predStart := i
	applyIdx := -1
	for j := i; j < len(s); j++ {
		if k := kpMatchKeywordAt(s, j, "APPLY"); k > 0 {
			applyIdx = j
			break
		}
	}

	if applyIdx < 0 {
		return clause, i, fmt.Errorf("expected APPLY after WHEN predicate")
	}

	clause.Predicate = strings.TrimSpace(s[predStart:applyIdx])
	i = applyIdx

	if j := kpMatchKeywordAt(s, i, "APPLY"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "PROFILE"); k > 0 {
			k = kpSkipSpaces(s, k)
			ref, l := kpScanQuotedString(s, k)
			if ref == "" {
				ref, l = kpScanIdent(s, k)
			}
			if ref == "" {
				return clause, i, fmt.Errorf("expected profile name after APPLY PROFILE")
			}
			clause.ProfileRef = ref
			i = l
		} else {
			return clause, i, fmt.Errorf("expected PROFILE after APPLY")
		}
	}

	return clause, i, nil
}

func parseAlterPromotionPolicy(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)
	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected policy name after ALTER PROMOTION POLICY")
	}

	i = kpSkipSpaces(s, i)
	updates := make(map[string]interface{})

	if j := kpMatchKeywordAt(s, i, "SET"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "OPTIONS"); k > 0 {
			body, l := kpScanBraceBlock(s, k)
			if l < 0 {
				return nil, false, fmt.Errorf("expected { after SET OPTIONS")
			}
			_ = l
			if err := parseOptionsMap(body, func(key, rawVal string) error {
				updates[key] = parseRawValue(rawVal)
				return nil
			}); err != nil {
				return nil, false, err
			}
		}
	}

	if j := kpMatchKeywordAt(s, i, "ENABLE"); j > 0 {
		updates["enabled"] = true
		i = j
	}
	if j := kpMatchKeywordAt(s, i, "DISABLE"); j > 0 {
		updates["enabled"] = false
		i = j
	}

	return &AlterPromotionPolicyCmd{Name: name, Updates: updates}, true, nil
}

func parseDropPromotionPolicy(s string, i int) (interface{}, bool, error) {
	i = kpSkipSpaces(s, i)

	ifExists := false
	if j := kpMatchKeywordAt(s, i, "IF"); j > 0 {
		j = kpSkipSpaces(s, j)
		if k := kpMatchKeywordAt(s, j, "EXISTS"); k > 0 {
			ifExists = true
			i = kpSkipSpaces(s, k)
		}
	}

	name, i := kpScanName(s, i)
	if name == "" {
		return nil, false, fmt.Errorf("expected policy name after DROP PROMOTION POLICY")
	}

	return &DropPromotionPolicyCmd{Name: name, IfExists: ifExists}, true, nil
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

	if s[i] == '\'' || s[i] == '"' {
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
