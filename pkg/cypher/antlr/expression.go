// Package antlr - Expression evaluation from AST nodes.
// All expression evaluation walks the AST - no string parsing.
package antlr

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/pkg/util"
)

// ExpressionEvaluator evaluates expressions from AST nodes
type ExpressionEvaluator struct {
	params    map[string]interface{}
	variables map[string]interface{}
	row       map[string]interface{} // Current row context for property lookups
}

// FunctionLookup is a callback to find custom/plugin functions by name
// Returns the function implementation and true if found
var FunctionLookup func(name string) (interface{}, bool)

// NewExpressionEvaluator creates a new expression evaluator
func NewExpressionEvaluator(params, variables map[string]interface{}) *ExpressionEvaluator {
	return &ExpressionEvaluator{
		params:    params,
		variables: variables,
		row:       make(map[string]interface{}),
	}
}

// SetRow sets the current row context for property lookups
func (e *ExpressionEvaluator) SetRow(row map[string]interface{}) {
	e.row = row
}

// EvaluateWhere evaluates a WHERE expression and returns true/false
func (e *ExpressionEvaluator) EvaluateWhere(expr IExpressionContext) bool {
	if expr == nil {
		return true
	}

	// Walk down the expression tree
	// Expression -> XorExpression(s) with OR between them
	xors := expr.AllXorExpression()
	if len(xors) == 0 {
		return true
	}

	// OR logic between xor expressions
	if len(xors) > 1 {
		for _, xor := range xors {
			if e.evaluateXor(xor) {
				return true
			}
		}
		return false
	}

	return e.evaluateXor(xors[0])
}

// evaluateXor evaluates XOR expression from AST
func (e *ExpressionEvaluator) evaluateXor(xor IXorExpressionContext) bool {
	if xor == nil {
		return true
	}

	ands := xor.AllAndExpression()
	if len(ands) == 0 {
		return true
	}

	if len(ands) == 1 {
		return e.evaluateAnd(ands[0])
	}

	// XOR logic
	result := e.evaluateAnd(ands[0])
	for i := 1; i < len(ands); i++ {
		result = result != e.evaluateAnd(ands[i])
	}
	return result
}

// evaluateAnd evaluates AND expression from AST
func (e *ExpressionEvaluator) evaluateAnd(and IAndExpressionContext) bool {
	if and == nil {
		return true
	}

	nots := and.AllNotExpression()
	if len(nots) == 0 {
		return true
	}

	// AND logic - all must be true
	for _, not := range nots {
		if !e.evaluateNot(not) {
			return false
		}
	}
	return true
}

// evaluateNot evaluates NOT expression from AST
func (e *ExpressionEvaluator) evaluateNot(not INotExpressionContext) bool {
	if not == nil {
		return true
	}

	// Check for NOT token
	hasNot := not.NOT() != nil

	comp := not.ComparisonExpression()
	if comp == nil {
		return !hasNot
	}

	result := e.evaluateComparison(comp)

	if hasNot {
		return !result
	}
	return result
}

// evaluateComparison evaluates comparison expression from AST
// Grammar: comparisonExpression : addSubExpression (comparisonSigns addSubExpression)*
func (e *ExpressionEvaluator) evaluateComparison(comp IComparisonExpressionContext) bool {
	if comp == nil {
		return true
	}

	adds := comp.AllAddSubExpression()
	if len(adds) == 0 {
		return true
	}

	// Get comparison signs
	signs := comp.AllComparisonSigns()

	// If no comparison signs, this might be a standalone boolean expression
	// like "n.name CONTAINS 'li'" or "n.email IS NULL" or "(nested AND expression)"
	if len(signs) == 0 {
		// Check if this is a parenthesized expression that needs recursive evaluation
		if parentExpr := e.findParenthesizedExprInAddSub(adds[0]); parentExpr != nil {
			// Recursively evaluate the inner expression as a boolean
			return e.EvaluateWhere(parentExpr)
		}

		// Check if this is an atomic expression with string/list/null predicates
		atomic := e.findAtomicInAddSub(adds[0])
		if atomic != nil {
			if result, handled := e.evaluateAtomicAsBool(atomic); handled {
				return result
			}
		}
		// Fall back to truthiness check
		leftVal := e.evaluateAddSub(adds[0])
		return e.isTruthy(leftVal)
	}

	// Get left value
	leftVal := e.evaluateAddSub(adds[0])

	// Evaluate each comparison
	currentLeft := leftVal
	for i, sign := range signs {
		if i+1 >= len(adds) {
			break
		}
		rightVal := e.evaluateAddSub(adds[i+1])

		// Check comparison operator from AST tokens
		result := e.evaluateComparisonSign(currentLeft, sign, rightVal)
		if !result {
			return false
		}

		// Chain comparisons: a < b < c means a < b AND b < c
		currentLeft = rightVal
	}

	return true
}

// findAtomicInAddSub walks down to find the AtomicExpression
func (e *ExpressionEvaluator) findAtomicInAddSub(add IAddSubExpressionContext) IAtomicExpressionContext {
	if add == nil {
		return nil
	}

	mults := add.AllMultDivExpression()
	if len(mults) != 1 {
		return nil
	}

	powers := mults[0].AllPowerExpression()
	if len(powers) != 1 {
		return nil
	}

	unarys := powers[0].AllUnaryAddSubExpression()
	if len(unarys) != 1 {
		return nil
	}

	return unarys[0].AtomicExpression()
}

// findParenthesizedExprInAddSub walks down to find a parenthesized expression that contains a nested boolean
func (e *ExpressionEvaluator) findParenthesizedExprInAddSub(add IAddSubExpressionContext) IExpressionContext {
	atomic := e.findAtomicInAddSub(add)
	if atomic == nil {
		return nil
	}

	propOrLabel := atomic.PropertyOrLabelExpression()
	if propOrLabel == nil {
		return nil
	}

	propExpr := propOrLabel.PropertyExpression()
	if propExpr == nil {
		return nil
	}

	atom := propExpr.Atom()
	if atom == nil {
		return nil
	}

	paren := atom.ParenthesizedExpression()
	if paren == nil {
		return nil
	}

	return paren.Expression()
}

// evaluateComparisonSign evaluates a comparison sign from AST tokens
func (e *ExpressionEvaluator) evaluateComparisonSign(leftVal interface{}, sign IComparisonSignsContext, rightVal interface{}) bool {
	if sign == nil {
		return true
	}

	// Check comparison operators using AST tokens
	if sign.ASSIGN() != nil { // =
		return e.valuesEqual(leftVal, rightVal)
	}
	if sign.NOT_EQUAL() != nil { // <> or !=
		return !e.valuesEqual(leftVal, rightVal)
	}
	if sign.LT() != nil { // <
		return e.compareValues(leftVal, rightVal) < 0
	}
	if sign.GT() != nil { // >
		return e.compareValues(leftVal, rightVal) > 0
	}
	if sign.LE() != nil { // <=
		return e.compareValues(leftVal, rightVal) <= 0
	}
	if sign.GE() != nil { // >=
		return e.compareValues(leftVal, rightVal) >= 0
	}

	return true
}

// evaluateAddSub evaluates an AddSubExpression
func (e *ExpressionEvaluator) evaluateAddSub(add IAddSubExpressionContext) interface{} {
	if add == nil {
		return nil
	}

	mults := add.AllMultDivExpression()
	if len(mults) == 0 {
		return nil
	}

	if len(mults) == 1 {
		return e.evaluateMultDiv(mults[0])
	}

	// Handle + and - operations
	result := e.toFloat64(e.evaluateMultDiv(mults[0]))
	// Check for PLUS/SUB tokens between expressions
	plusTokens := add.AllPLUS()
	subTokens := add.AllSUB()

	for i := 1; i < len(mults); i++ {
		val := e.toFloat64(e.evaluateMultDiv(mults[i]))
		// Simple heuristic: if we have SUB tokens, treat as subtraction
		if len(subTokens) > 0 && i <= len(subTokens) {
			result -= val
		} else {
			result += val
		}
	}
	_ = plusTokens // Used for type checking
	return maybeInt64(result)
}

// evaluateMultDiv evaluates a MultDivExpression
func (e *ExpressionEvaluator) evaluateMultDiv(mult IMultDivExpressionContext) interface{} {
	if mult == nil {
		return nil
	}

	powers := mult.AllPowerExpression()
	if len(powers) == 0 {
		return nil
	}

	if len(powers) == 1 {
		return e.evaluatePower(powers[0])
	}

	result := e.toFloat64(e.evaluatePower(powers[0]))

	// Get operator tokens in order from children
	children := mult.GetChildren()
	opIndex := 0

	for i := 1; i < len(powers); i++ {
		val := e.toFloat64(e.evaluatePower(powers[i]))

		// Find the operator between powers[i-1] and powers[i]
		for opIndex < len(children) {
			child := children[opIndex]
			opIndex++
			if term, ok := child.(antlr.TerminalNode); ok {
				tokenType := term.GetSymbol().GetTokenType()
				switch tokenType {
				case CypherParserMULT:
					result *= val
					goto nextPower
				case CypherParserDIV:
					if val != 0 {
						result /= val
					}
					goto nextPower
				case CypherParserMOD:
					if val != 0 {
						result = float64(int64(result) % int64(val))
					}
					goto nextPower
				}
			}
		}
	nextPower:
	}
	return maybeInt64(result)
}

// evaluatePower evaluates a PowerExpression
func (e *ExpressionEvaluator) evaluatePower(power IPowerExpressionContext) interface{} {
	if power == nil {
		return nil
	}

	unarys := power.AllUnaryAddSubExpression()
	if len(unarys) == 0 {
		return nil
	}

	return e.evaluateUnary(unarys[0])
}

// evaluateUnary evaluates a UnaryAddSubExpression
func (e *ExpressionEvaluator) evaluateUnary(unary IUnaryAddSubExpressionContext) interface{} {
	if unary == nil {
		return nil
	}

	// Check for unary minus
	hasMinus := unary.SUB() != nil

	atomic := unary.AtomicExpression()
	if atomic == nil {
		return nil
	}

	val := e.evaluateAtomic(atomic)

	if hasMinus {
		return -e.toFloat64(val)
	}
	return val
}

// evaluateAtomic evaluates an AtomicExpression
// Grammar: atomicExpression : propertyOrLabelExpression (stringExpression | listExpression | nullExpression)*
func (e *ExpressionEvaluator) evaluateAtomic(atomic IAtomicExpressionContext) interface{} {
	if atomic == nil {
		return nil
	}

	propOrLabel := atomic.PropertyOrLabelExpression()
	if propOrLabel == nil {
		return nil
	}

	baseVal := e.evaluatePropertyOrLabel(propOrLabel)

	// Check for NullExpression (IS NULL / IS NOT NULL)
	// Returns boolean true/false for the null check
	nullExprs := atomic.AllNullExpression()
	if len(nullExprs) > 0 {
		for _, nullExpr := range nullExprs {
			isNot := nullExpr.NOT() != nil
			if isNot {
				// IS NOT NULL - true if value is not null
				return baseVal != nil
			} else {
				// IS NULL - true if value is null
				return baseVal == nil
			}
		}
	}

	// Check for StringExpression (STARTS WITH, ENDS WITH, CONTAINS)
	strExprs := atomic.AllStringExpression()
	if len(strExprs) > 0 {
		for _, strExpr := range strExprs {
			prefix := strExpr.StringExpPrefix()
			if prefix == nil {
				continue
			}
			operand := strExpr.PropertyOrLabelExpression()
			if operand == nil {
				continue
			}
			operandVal := e.evaluatePropertyOrLabel(operand)

			baseStr, baseOk := baseVal.(string)
			operandStr, operandOk := operandVal.(string)
			if !baseOk || !operandOk {
				return false
			}

			if prefix.STARTS() != nil {
				return strings.HasPrefix(baseStr, operandStr)
			} else if prefix.ENDS() != nil {
				return strings.HasSuffix(baseStr, operandStr)
			} else if prefix.CONTAINS() != nil {
				return strings.Contains(baseStr, operandStr)
			}
		}
	}

	// Check for ListExpression (IN list)
	listExprs := atomic.AllListExpression()
	if len(listExprs) > 0 {
		for _, listExpr := range listExprs {
			if listExpr.IN() != nil {
				listPropOrLabel := listExpr.PropertyOrLabelExpression()
				if listPropOrLabel != nil {
					listVal := e.evaluatePropertyOrLabel(listPropOrLabel)
					if list, ok := listVal.([]interface{}); ok {
						for _, item := range list {
							if e.valuesEqual(baseVal, item) {
								return true
							}
						}
					}
					return false
				}
			}
		}
	}

	return baseVal
}

// evaluateAtomicAsBool evaluates atomic expression as boolean (for WHERE)
// Handles IS NULL, IS NOT NULL, IN list, STARTS WITH, ENDS WITH, CONTAINS
func (e *ExpressionEvaluator) evaluateAtomicAsBool(atomic IAtomicExpressionContext) (bool, bool) {
	if atomic == nil {
		return true, false // No constraint, not handled
	}

	propOrLabel := atomic.PropertyOrLabelExpression()
	if propOrLabel == nil {
		return true, false
	}

	baseVal := e.evaluatePropertyOrLabel(propOrLabel)

	// Check for NullExpression (IS NULL / IS NOT NULL)
	nullExprs := atomic.AllNullExpression()
	if len(nullExprs) > 0 {
		for _, nullExpr := range nullExprs {
			isNot := nullExpr.NOT() != nil
			if isNot {
				if baseVal == nil {
					return false, true // IS NOT NULL but value is null
				}
			} else {
				if baseVal != nil {
					return false, true // IS NULL but value is not null
				}
			}
		}
		return true, true
	}

	// Check for StringExpression (STARTS WITH, ENDS WITH, CONTAINS)
	strExprs := atomic.AllStringExpression()
	if len(strExprs) > 0 {
		for _, strExpr := range strExprs {
			prefix := strExpr.StringExpPrefix()
			if prefix == nil {
				continue
			}
			operand := strExpr.PropertyOrLabelExpression()
			if operand == nil {
				continue
			}
			operandVal := e.evaluatePropertyOrLabel(operand)

			baseStr, baseOk := baseVal.(string)
			operandStr, operandOk := operandVal.(string)
			if !baseOk || !operandOk {
				return false, true
			}

			if prefix.STARTS() != nil { // STARTS WITH
				if len(baseStr) < len(operandStr) || baseStr[:len(operandStr)] != operandStr {
					return false, true
				}
			}
			if prefix.ENDS() != nil { // ENDS WITH
				if len(baseStr) < len(operandStr) || baseStr[len(baseStr)-len(operandStr):] != operandStr {
					return false, true
				}
			}
			if prefix.CONTAINS() != nil { // CONTAINS
				found := false
				for i := 0; i <= len(baseStr)-len(operandStr); i++ {
					if baseStr[i:i+len(operandStr)] == operandStr {
						found = true
						break
					}
				}
				if !found {
					return false, true
				}
			}
		}
		return true, true
	}

	// Check for ListExpression (IN list)
	listExprs := atomic.AllListExpression()
	if len(listExprs) > 0 {
		for _, listExpr := range listExprs {
			if listExpr.IN() != nil {
				operand := listExpr.PropertyOrLabelExpression()
				if operand == nil {
					continue
				}
				listVal := e.evaluatePropertyOrLabel(operand)
				if list, ok := listVal.([]interface{}); ok {
					found := false
					for _, item := range list {
						if e.valuesEqual(baseVal, item) {
							found = true
							break
						}
					}
					if !found {
						return false, true
					}
				}
			}
		}
		return true, true
	}

	return true, false // Not handled as bool
}

// evaluatePropertyOrLabel evaluates property access from AST
func (e *ExpressionEvaluator) evaluatePropertyOrLabel(propOrLabel IPropertyOrLabelExpressionContext) interface{} {
	if propOrLabel == nil {
		return nil
	}

	propExpr := propOrLabel.PropertyExpression()
	if propExpr == nil {
		return nil
	}

	atom := propExpr.Atom()
	if atom == nil {
		return nil
	}

	baseVal := e.evaluateAtom(atom)

	// Check for property access via DOT tokens
	dots := propExpr.AllDOT()
	if len(dots) == 0 {
		return baseVal
	}

	// Get property names from Name nodes
	names := propExpr.AllName()
	currentVal := baseVal
	for _, name := range names {
		propName := name.GetText()
		if nodeMap, ok := currentVal.(map[string]interface{}); ok {
			currentVal = nodeMap[propName]
		} else {
			return nil
		}
	}

	return currentVal
}

// evaluateAtom evaluates an Atom
func (e *ExpressionEvaluator) evaluateAtom(atom IAtomContext) interface{} {
	if atom == nil {
		return nil
	}

	// Check for COUNT(*)
	if countAll := atom.CountAll(); countAll != nil {
		// This is COUNT(*) - handled specially in aggregation
		return "COUNT(*)"
	}

	// Check for literal
	if lit := atom.Literal(); lit != nil {
		return e.evaluateLiteral(lit)
	}

	// Check for parameter
	if param := atom.Parameter(); param != nil {
		if sym := param.Symbol(); sym != nil {
			if val, ok := e.params[sym.GetText()]; ok {
				return val
			}
		}
		return nil
	}

	// Check for symbol (variable reference)
	if sym := atom.Symbol(); sym != nil {
		varName := sym.GetText()
		if val, ok := e.row[varName]; ok {
			return val
		}
		if val, ok := e.variables[varName]; ok {
			return val
		}
		return nil
	}

	// Check for function invocation
	if funcInvoc := atom.FunctionInvocation(); funcInvoc != nil {
		return e.evaluateFunctionInvocation(funcInvoc)
	}

	// Check for parenthesized expression
	if paren := atom.ParenthesizedExpression(); paren != nil {
		if innerExpr := paren.Expression(); innerExpr != nil {
			return e.Evaluate(innerExpr)
		}
	}

	// List and Map literals are handled in Literal, not directly in Atom

	return nil
}

// evaluateLiteral evaluates a literal from AST
func (e *ExpressionEvaluator) evaluateLiteral(lit ILiteralContext) interface{} {
	if lit == nil {
		return nil
	}

	// Boolean - check AST tokens
	if boolLit := lit.BoolLit(); boolLit != nil {
		if boolLit.TRUE() != nil {
			return true
		}
		return false
	}

	// Null - check AST token
	if lit.NULL_W() != nil {
		return nil
	}

	// Number
	if numLit := lit.NumLit(); numLit != nil {
		if floatLit := numLit.FLOAT(); floatLit != nil {
			if f, err := strconv.ParseFloat(floatLit.GetText(), 64); err == nil {
				return f
			}
		}
		if intLit := numLit.IntegerLit(); intLit != nil {
			if i, err := strconv.ParseInt(intLit.GetText(), 10, 64); err == nil {
				return i
			}
		}
	}

	// String - get text and remove quotes
	if strLit := lit.StringLit(); strLit != nil {
		text := strLit.GetText()
		if len(text) >= 2 {
			return text[1 : len(text)-1]
		}
		return text
	}

	// Char
	if charLit := lit.CharLit(); charLit != nil {
		text := charLit.GetText()
		if len(text) >= 2 {
			return text[1 : len(text)-1]
		}
		return text
	}

	// List
	if listLit := lit.ListLit(); listLit != nil {
		if exprChain := listLit.ExpressionChain(); exprChain != nil {
			var result []interface{}
			for _, expr := range exprChain.AllExpression() {
				result = append(result, e.Evaluate(expr))
			}
			return result
		}
		return []interface{}{}
	}

	// Map
	if mapLit := lit.MapLit(); mapLit != nil {
		result := make(map[string]interface{})
		for _, pair := range mapLit.AllMapPair() {
			if name := pair.Name(); name != nil {
				if expr := pair.Expression(); expr != nil {
					result[name.GetText()] = e.Evaluate(expr)
				}
			}
		}
		return result
	}

	return nil
}

// evaluateFunctionInvocation evaluates a function call
func (e *ExpressionEvaluator) evaluateFunctionInvocation(funcInvoc IFunctionInvocationContext) interface{} {
	if funcInvoc == nil {
		return nil
	}

	// Get function name
	invocName := funcInvoc.InvocationName()
	if invocName == nil {
		return nil
	}

	// Get arguments
	var args []interface{}
	if exprChain := funcInvoc.ExpressionChain(); exprChain != nil {
		for _, expr := range exprChain.AllExpression() {
			args = append(args, e.Evaluate(expr))
		}
	}

	// Get function name from symbols
	symbols := invocName.AllSymbol()
	if len(symbols) == 0 {
		return nil
	}

	// Check for aggregation functions by token type
	firstSym := symbols[0]

	// COUNT token
	if firstSym.COUNT() != nil {
		// Aggregation - return marker
		return &AggregationMarker{FuncName: "COUNT", Args: args}
	}

	// SUM token
	if firstSym.SUM() != nil {
		return &AggregationMarker{FuncName: "SUM", Args: args}
	}

	// AVG token
	if firstSym.AVG() != nil {
		return &AggregationMarker{FuncName: "AVG", Args: args}
	}

	// MIN token
	if firstSym.MIN() != nil {
		return &AggregationMarker{FuncName: "MIN", Args: args}
	}

	// MAX token
	if firstSym.MAX() != nil {
		return &AggregationMarker{FuncName: "MAX", Args: args}
	}

	// COLLECT token
	if firstSym.COLLECT() != nil {
		return &AggregationMarker{FuncName: "COLLECT", Args: args}
	}

	// Build full function name (may have multiple parts like "apoc.coll.sum")
	var funcNameParts []string
	for _, sym := range symbols {
		funcNameParts = append(funcNameParts, sym.GetText())
	}
	funcName := strings.Join(funcNameParts, ".")

	// Try to call plugin/custom function
	if FunctionLookup != nil {
		if fn, found := FunctionLookup(funcName); found {
			return e.callPluginFunction(fn, args)
		}
	}

	// Built-in functions
	return e.evaluateBuiltInFunction(funcName, args)
}

// callPluginFunction calls a plugin function with the given arguments
func (e *ExpressionEvaluator) callPluginFunction(fn interface{}, args []interface{}) interface{} {
	// fn should be a function that takes []interface{} and returns interface{}
	switch f := fn.(type) {
	case func([]interface{}) interface{}:
		return f(args)
	case func(...interface{}) interface{}:
		return f(args...)
	}

	// Use reflection for other function signatures
	fnValue := reflect.ValueOf(fn)
	if fnValue.Kind() != reflect.Func {
		return nil
	}

	fnType := fnValue.Type()
	numIn := fnType.NumIn()

	// Build argument values
	inArgs := make([]reflect.Value, numIn)
	for i := 0; i < numIn; i++ {
		paramType := fnType.In(i)

		var arg interface{}
		if i < len(args) {
			arg = args[i]
		}

		// Convert arg to the expected type
		argValue := convertToType(arg, paramType)
		if !argValue.IsValid() {
			argValue = reflect.Zero(paramType)
		}
		inArgs[i] = argValue
	}

	// Call the function
	results := fnValue.Call(inArgs)
	if len(results) > 0 {
		return results[0].Interface()
	}
	return nil
}

// convertToType converts a value to the specified reflect.Type
func convertToType(val interface{}, targetType reflect.Type) reflect.Value {
	if val == nil {
		return reflect.Zero(targetType)
	}

	srcValue := reflect.ValueOf(val)
	srcType := srcValue.Type()

	// If types match directly
	if srcType == targetType || srcType.AssignableTo(targetType) {
		return srcValue
	}

	// Convertible types
	if srcType.ConvertibleTo(targetType) {
		return srcValue.Convert(targetType)
	}

	// Handle numeric conversions
	switch targetType.Kind() {
	case reflect.Float64:
		switch v := val.(type) {
		case int64:
			return reflect.ValueOf(float64(v))
		case int:
			return reflect.ValueOf(float64(v))
		case float32:
			return reflect.ValueOf(float64(v))
		case float64:
			return reflect.ValueOf(v)
		}
	case reflect.Int64:
		switch v := val.(type) {
		case float64:
			if out, ok := util.SafeFloat64ToInt64(v); ok {
				return reflect.ValueOf(out)
			}
		case int:
			return reflect.ValueOf(int64(v))
		case int64:
			return reflect.ValueOf(v)
		}
	case reflect.Int:
		switch v := val.(type) {
		case float64:
			if out, ok := util.SafeFloat64ToInt(v); ok {
				return reflect.ValueOf(out)
			}
		case int64:
			if out, ok := util.SafeInt64ToInt(v); ok {
				return reflect.ValueOf(out)
			}
		case int:
			return reflect.ValueOf(v)
		}
	}

	return reflect.Zero(targetType)
}

// evaluateBuiltInFunction evaluates built-in functions by name
func (e *ExpressionEvaluator) evaluateBuiltInFunction(name string, args []interface{}) interface{} {
	nameLower := strings.ToLower(name)

	switch nameLower {
	case "tostring":
		if len(args) > 0 {
			return fmt.Sprintf("%v", args[0])
		}
	case "tointeger", "toint":
		if len(args) > 0 {
			return e.toInt64(args[0])
		}
	case "tofloat":
		if len(args) > 0 {
			return e.toFloat64(args[0])
		}
	case "toboolean":
		if len(args) > 0 {
			return e.toBool(args[0])
		}
	case "size", "length":
		if len(args) > 0 {
			return e.size(args[0])
		}
	case "head":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok && len(list) > 0 {
				return list[0]
			}
		}
	case "tail":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok && len(list) > 1 {
				return list[1:]
			}
		}
	case "last":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok && len(list) > 0 {
				return list[len(list)-1]
			}
		}
	case "reverse":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok {
				reversed := make([]interface{}, len(list))
				for i, v := range list {
					reversed[len(list)-1-i] = v
				}
				return reversed
			}
		}
	case "range":
		if len(args) >= 2 {
			start, ok := util.SafeFloat64ToInt(e.toFloat64(args[0]))
			if !ok {
				return nil
			}
			end, ok := util.SafeFloat64ToInt(e.toFloat64(args[1]))
			if !ok {
				return nil
			}
			step := 1
			if len(args) >= 3 {
				if parsedStep, ok := util.SafeFloat64ToInt(e.toFloat64(args[2])); ok {
					step = parsedStep
				}
			}
			if step == 0 {
				step = 1
			}
			var result []interface{}
			if step > 0 {
				for i := start; i <= end; i += step {
					result = append(result, int64(i))
				}
			} else {
				for i := start; i >= end; i += step {
					result = append(result, int64(i))
				}
			}
			return result
		}
	case "coalesce":
		for _, arg := range args {
			if arg != nil {
				return arg
			}
		}
	case "abs":
		if len(args) > 0 {
			v := e.toFloat64(args[0])
			if v < 0 {
				v = -v
			}
			return maybeInt64(v)
		}
	case "ceil":
		if len(args) > 0 {
			if out, ok := util.SafeFloat64ToInt64(math.Ceil(e.toFloat64(args[0]))); ok {
				return out
			}
			return int64(0)
		}
	case "floor":
		if len(args) > 0 {
			if out, ok := util.SafeFloat64ToInt64(math.Floor(e.toFloat64(args[0]))); ok {
				return out
			}
			return int64(0)
		}
	case "round":
		if len(args) > 0 {
			v := e.toFloat64(args[0])
			if out, ok := util.SafeFloat64ToInt64(math.Round(v)); ok {
				return out
			}
			return int64(0)
		}
	case "trim":
		if len(args) > 0 {
			return strings.TrimSpace(fmt.Sprintf("%v", args[0]))
		}
	case "ltrim":
		if len(args) > 0 {
			return strings.TrimLeft(fmt.Sprintf("%v", args[0]), " \t\n\r")
		}
	case "rtrim":
		if len(args) > 0 {
			return strings.TrimRight(fmt.Sprintf("%v", args[0]), " \t\n\r")
		}
	case "toupper":
		if len(args) > 0 {
			return strings.ToUpper(fmt.Sprintf("%v", args[0]))
		}
	case "tolower":
		if len(args) > 0 {
			return strings.ToLower(fmt.Sprintf("%v", args[0]))
		}
	case "replace":
		if len(args) >= 3 {
			s := fmt.Sprintf("%v", args[0])
			old := fmt.Sprintf("%v", args[1])
			new := fmt.Sprintf("%v", args[2])
			return strings.ReplaceAll(s, old, new)
		}
	case "substring":
		if len(args) >= 2 {
			s := fmt.Sprintf("%v", args[0])
			start, ok := util.SafeFloat64ToInt(e.toFloat64(args[1]))
			if !ok {
				return ""
			}
			if start < 0 {
				start = 0
			}
			if start >= len(s) {
				return ""
			}
			if len(args) >= 3 {
				length, ok := util.SafeFloat64ToInt(e.toFloat64(args[2]))
				if !ok || length < 0 {
					return ""
				}
				if start+length > len(s) {
					length = len(s) - start
				}
				return s[start : start+length]
			}
			return s[start:]
		}
	case "left":
		if len(args) >= 2 {
			s := fmt.Sprintf("%v", args[0])
			n, ok := util.SafeFloat64ToInt(e.toFloat64(args[1]))
			if !ok || n < 0 {
				return ""
			}
			if n >= len(s) {
				return s
			}
			return s[:n]
		}
	case "right":
		if len(args) >= 2 {
			s := fmt.Sprintf("%v", args[0])
			n, ok := util.SafeFloat64ToInt(e.toFloat64(args[1]))
			if !ok || n < 0 {
				return ""
			}
			if n >= len(s) {
				return s
			}
			return s[len(s)-n:]
		}
	case "split":
		if len(args) >= 2 {
			s := fmt.Sprintf("%v", args[0])
			sep := fmt.Sprintf("%v", args[1])
			parts := strings.Split(s, sep)
			result := make([]interface{}, len(parts))
			for i, p := range parts {
				result[i] = p
			}
			return result
		}
	// APOC-like collection functions
	case "apoc.coll.sum":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok {
				sum := float64(0)
				for _, v := range list {
					sum += e.toFloat64(v)
				}
				return maybeInt64(sum)
			}
		}
	case "apoc.coll.avg":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok && len(list) > 0 {
				sum := float64(0)
				for _, v := range list {
					sum += e.toFloat64(v)
				}
				return sum / float64(len(list))
			}
		}
	case "apoc.coll.min":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok && len(list) > 0 {
				min := e.toFloat64(list[0])
				for _, v := range list[1:] {
					val := e.toFloat64(v)
					if val < min {
						min = val
					}
				}
				return maybeInt64(min)
			}
		}
	case "apoc.coll.max":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok && len(list) > 0 {
				max := e.toFloat64(list[0])
				for _, v := range list[1:] {
					val := e.toFloat64(v)
					if val > max {
						max = val
					}
				}
				return maybeInt64(max)
			}
		}
	case "apoc.coll.reverse":
		if len(args) > 0 {
			if list, ok := args[0].([]interface{}); ok {
				reversed := make([]interface{}, len(list))
				for i, v := range list {
					reversed[len(list)-1-i] = v
				}
				return reversed
			}
		}

	// Graph element functions
	case "type":
		// type(relationship) - returns the type of a relationship
		if len(args) > 0 {
			if relMap, ok := args[0].(map[string]interface{}); ok {
				if t, ok := relMap["_type"]; ok {
					return t
				}
			}
		}
		return nil

	case "id":
		// id(node) or id(relationship) - returns the internal ID
		if len(args) > 0 {
			if nodeMap, ok := args[0].(map[string]interface{}); ok {
				if id, ok := nodeMap["_nodeId"]; ok {
					return id
				}
				if id, ok := nodeMap["_edgeId"]; ok {
					return id
				}
			}
		}
		return nil

	case "labels":
		// labels(node) - returns labels of a node
		if len(args) > 0 {
			if nodeMap, ok := args[0].(map[string]interface{}); ok {
				if labels, ok := nodeMap["_labels"]; ok {
					return labels
				}
			}
		}
		return nil

	case "keys":
		// keys(map or node) - returns keys of a map/node properties
		if len(args) > 0 {
			if m, ok := args[0].(map[string]interface{}); ok {
				var keys []interface{}
				for k := range m {
					// Skip internal properties
					if !strings.HasPrefix(k, "_") {
						keys = append(keys, k)
					}
				}
				return keys
			}
		}
		return nil

	case "properties":
		// properties(node or relationship) - returns properties as map
		if len(args) > 0 {
			if m, ok := args[0].(map[string]interface{}); ok {
				props := make(map[string]interface{})
				for k, v := range m {
					// Skip internal properties
					if !strings.HasPrefix(k, "_") {
						props[k] = v
					}
				}
				return props
			}
		}
		return nil

	case "timestamp":
		// timestamp() - returns current timestamp in milliseconds
		return time.Now().UnixMilli()

	case "date":
		// date() - returns current date as string
		return time.Now().Format("2006-01-02")

	case "datetime":
		// datetime() - returns current datetime as typed value
		return time.Now().UTC()

	case "exists":
		// exists(property) - returns true if property exists and is not null
		if len(args) > 0 {
			return args[0] != nil
		}
		return false
	}

	return nil
}

// toInt64 converts a value to int64
func (e *ExpressionEvaluator) toInt64(val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		if out, ok := util.SafeFloat64ToInt64(v); ok {
			return out
		}
	case float32:
		if out, ok := util.SafeFloat64ToInt64(float64(v)); ok {
			return out
		}
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

// toBool converts a value to bool
func (e *ExpressionEvaluator) toBool(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case string:
		return strings.ToLower(v) == "true"
	case int64:
		return v != 0
	case float64:
		return v != 0
	}
	return false
}

// size returns the size/length of a value
func (e *ExpressionEvaluator) size(val interface{}) int64 {
	switch v := val.(type) {
	case []interface{}:
		return int64(len(v))
	case string:
		return int64(len(v))
	case map[string]interface{}:
		return int64(len(v))
	}
	return 0
}

// Evaluate evaluates any expression and returns its value
func (e *ExpressionEvaluator) Evaluate(expr IExpressionContext) interface{} {
	if expr == nil {
		return nil
	}

	xors := expr.AllXorExpression()
	if len(xors) != 1 {
		return nil
	}

	ands := xors[0].AllAndExpression()
	if len(ands) != 1 {
		return nil
	}

	nots := ands[0].AllNotExpression()
	if len(nots) != 1 {
		return nil
	}

	comp := nots[0].ComparisonExpression()
	if comp == nil {
		return nil
	}

	adds := comp.AllAddSubExpression()
	if len(adds) != 1 {
		return nil
	}

	return e.evaluateAddSub(adds[0])
}

// AggregationMarker marks an aggregation function for later processing
type AggregationMarker struct {
	FuncName string
	Args     []interface{}
	Distinct bool
}

// IsAggregation checks if a value is an aggregation marker
func IsAggregation(val interface{}) (*AggregationMarker, bool) {
	if marker, ok := val.(*AggregationMarker); ok {
		return marker, true
	}
	return nil, false
}

// Helper functions

func (e *ExpressionEvaluator) isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	}
	return true
}

func (e *ExpressionEvaluator) valuesEqual(a, b interface{}) bool {
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		case int:
			return av == int64(bv)
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return av == bv
		case int64:
			return av == float64(bv)
		case int:
			return av == float64(bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	}
	return false
}

func (e *ExpressionEvaluator) compareValues(a, b interface{}) int {
	aNum, aIsNum := e.toNumericOk(a)
	bNum, bIsNum := e.toNumericOk(b)
	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// String comparison fallback
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	return 0
}

func (e *ExpressionEvaluator) toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case float64:
		return v
	case float32:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 0
}

func (e *ExpressionEvaluator) toNumericOk(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	}
	return 0, false
}

// maybeInt64 returns int64 if the float64 is a whole number, otherwise returns float64
func maybeInt64(f float64) interface{} {
	if out, ok := util.SafeFloat64ToInt64(f); ok && f == float64(out) {
		return out
	}
	return f
}

// ExtractPropertyAccess extracts variable and property name from a property expression AST
func ExtractPropertyAccess(propExpr IPropertyExpressionContext) (varName string, propNames []string) {
	if propExpr == nil {
		return "", nil
	}

	atom := propExpr.Atom()
	if atom == nil {
		return "", nil
	}

	// Get variable name from symbol
	if sym := atom.Symbol(); sym != nil {
		varName = sym.GetText()
	}

	// Get property names from DOT accesses
	for _, name := range propExpr.AllName() {
		propNames = append(propNames, name.GetText())
	}

	return varName, propNames
}

// NodePatternInfo holds extracted node pattern info from AST
type NodePatternInfo struct {
	Variable   string
	Labels     []string
	Properties map[string]interface{}
}

// ExtractNodePattern extracts node pattern info from AST
func ExtractNodePattern(node INodePatternContext, eval *ExpressionEvaluator) NodePatternInfo {
	info := NodePatternInfo{
		Properties: make(map[string]interface{}),
	}

	if node == nil {
		return info
	}

	// Get variable from Symbol AST node
	if sym := node.Symbol(); sym != nil {
		info.Variable = sym.GetText()
	}

	// Get labels from NodeLabels AST node
	if nodeLabels := node.NodeLabels(); nodeLabels != nil {
		for _, name := range nodeLabels.AllName() {
			info.Labels = append(info.Labels, name.GetText())
		}
	}

	// Get properties from Properties AST node
	if propsCtx := node.Properties(); propsCtx != nil {
		info.Properties = ExtractProperties(propsCtx, eval)
	}

	return info
}

// ExtractProperties extracts properties map from AST
func ExtractProperties(propsCtx IPropertiesContext, eval *ExpressionEvaluator) map[string]interface{} {
	result := make(map[string]interface{})

	if propsCtx == nil {
		return result
	}

	if mapLit := propsCtx.MapLit(); mapLit != nil {
		for _, pair := range mapLit.AllMapPair() {
			if nameCtx := pair.Name(); nameCtx != nil {
				key := nameCtx.GetText()
				if key != "" {
					if exprCtx := pair.Expression(); exprCtx != nil {
						result[key] = eval.Evaluate(exprCtx)
					}
				}
			}
		}
	}

	return result
}

// RelationshipPatternInfo holds extracted relationship pattern info from AST
type RelationshipPatternInfo struct {
	Variable   string
	Type       string
	Properties map[string]interface{}
	IsForward  bool // true for -[r]-> false for <-[r]-
}

// ExtractRelationshipPattern extracts relationship pattern info from AST
func ExtractRelationshipPattern(rel IRelationshipPatternContext, eval *ExpressionEvaluator) RelationshipPatternInfo {
	info := RelationshipPatternInfo{
		Properties: make(map[string]interface{}),
		IsForward:  true, // default
	}

	if rel == nil {
		return info
	}

	// Check direction from AST tokens
	// Pattern: LT SUB detail SUB GT? or SUB detail SUB GT
	if rel.LT() != nil {
		info.IsForward = false // <-[r]-
	}

	// Get relationship detail
	if detail := rel.RelationDetail(); detail != nil {
		// Get variable from Symbol
		if sym := detail.Symbol(); sym != nil {
			info.Variable = sym.GetText()
		}

		// Get type from RelationshipTypes
		if types := detail.RelationshipTypes(); types != nil {
			for _, name := range types.AllName() {
				info.Type = name.GetText()
				break // Take first type
			}
		}

		// Get properties
		if propsCtx := detail.Properties(); propsCtx != nil {
			info.Properties = ExtractProperties(propsCtx, eval)
		}
	}

	return info
}

// ProjectionItemInfo holds extracted projection item info from AST
type ProjectionItemInfo struct {
	Expression    IExpressionContext
	Alias         string
	IsAggregation bool
	AggFunc       string
	AggArgs       []IExpressionContext
	IsDistinct    bool
}

// ExtractProjectionItem extracts projection item info from AST
func ExtractProjectionItem(item IProjectionItemContext) ProjectionItemInfo {
	info := ProjectionItemInfo{}

	if item == nil {
		return info
	}

	info.Expression = item.Expression()

	// Get alias from Symbol (AS alias)
	if sym := item.Symbol(); sym != nil {
		info.Alias = sym.GetText()
	}

	// If no alias, use expression text as column name
	if info.Alias == "" && info.Expression != nil {
		info.Alias = info.Expression.GetText()
	}

	// Check for aggregation by walking AST
	if info.Expression != nil {
		agg := ExtractAggregation(info.Expression)
		info.IsAggregation = agg.IsAggregation
		info.AggFunc = agg.FuncName
		info.AggArgs = agg.Args
		info.IsDistinct = agg.IsDistinct
	}

	return info
}

// AggregationInfo holds aggregation function info extracted from AST
type AggregationInfo struct {
	IsAggregation bool
	IsCountAll    bool // COUNT(*)
	FuncName      string
	Args          []IExpressionContext
	IsDistinct    bool
}

// ExtractAggregation extracts aggregation function info from expression AST
func ExtractAggregation(expr IExpressionContext) AggregationInfo {
	info := AggregationInfo{}
	if expr == nil {
		return info
	}

	atom := findAtomInExpression(expr)
	if atom == nil {
		return info
	}

	// Check for COUNT(*) - has its own AST node
	if countAll := atom.CountAll(); countAll != nil {
		info.IsAggregation = true
		info.IsCountAll = true
		info.FuncName = "COUNT"
		return info
	}

	// Check for FunctionInvocation
	funcInvoc := atom.FunctionInvocation()
	if funcInvoc == nil {
		return info
	}

	invocName := funcInvoc.InvocationName()
	if invocName == nil {
		return info
	}

	symbols := invocName.AllSymbol()
	if len(symbols) == 0 {
		return info
	}

	firstSym := symbols[0]

	// Check aggregation function tokens
	if firstSym.COUNT() != nil {
		info.IsAggregation = true
		info.FuncName = "COUNT"
	} else if firstSym.SUM() != nil {
		info.IsAggregation = true
		info.FuncName = "SUM"
	} else if firstSym.AVG() != nil {
		info.IsAggregation = true
		info.FuncName = "AVG"
	} else if firstSym.MIN() != nil {
		info.IsAggregation = true
		info.FuncName = "MIN"
	} else if firstSym.MAX() != nil {
		info.IsAggregation = true
		info.FuncName = "MAX"
	} else if firstSym.COLLECT() != nil {
		info.IsAggregation = true
		info.FuncName = "COLLECT"
	}

	if info.IsAggregation {
		info.IsDistinct = funcInvoc.DISTINCT() != nil
		if exprChain := funcInvoc.ExpressionChain(); exprChain != nil {
			info.Args = exprChain.AllExpression()
		}
	}

	return info
}

func findAtomInExpression(expr IExpressionContext) IAtomContext {
	if expr == nil {
		return nil
	}

	xors := expr.AllXorExpression()
	if len(xors) != 1 {
		return nil
	}

	ands := xors[0].AllAndExpression()
	if len(ands) != 1 {
		return nil
	}

	nots := ands[0].AllNotExpression()
	if len(nots) != 1 {
		return nil
	}

	comp := nots[0].ComparisonExpression()
	if comp == nil {
		return nil
	}

	adds := comp.AllAddSubExpression()
	if len(adds) != 1 {
		return nil
	}

	mults := adds[0].AllMultDivExpression()
	if len(mults) != 1 {
		return nil
	}

	powers := mults[0].AllPowerExpression()
	if len(powers) != 1 {
		return nil
	}

	unarys := powers[0].AllUnaryAddSubExpression()
	if len(unarys) != 1 {
		return nil
	}

	atomic := unarys[0].AtomicExpression()
	if atomic == nil {
		return nil
	}

	propOrLabel := atomic.PropertyOrLabelExpression()
	if propOrLabel == nil {
		return nil
	}

	propExpr := propOrLabel.PropertyExpression()
	if propExpr == nil {
		return nil
	}

	return propExpr.Atom()
}

// ProcedureInfo holds procedure call info from AST
type ProcedureInfo struct {
	Name      string
	Namespace string // e.g. "db" for "db.labels"
	IsDbProc  bool
}

// ExtractProcedureName extracts procedure name from invocation AST
func ExtractProcedureName(invocName IInvocationNameContext) ProcedureInfo {
	info := ProcedureInfo{}
	if invocName == nil {
		return info
	}

	symbols := invocName.AllSymbol()
	if len(symbols) == 0 {
		return info
	}

	// Build full procedure name from symbols
	var parts []string
	for _, sym := range symbols {
		parts = append(parts, sym.GetText())
	}

	info.Name = strings.Join(parts, ".")

	// Check if it's a db.* procedure
	if len(symbols) >= 2 {
		firstSym := symbols[0]
		if firstSym.DATABASE() != nil || firstSym.GetText() == "db" {
			info.IsDbProc = true
			info.Namespace = "db"
		}
	}

	return info
}

// ShowCommandType represents types of SHOW commands
type ShowCommandType int

const (
	ShowUnknown ShowCommandType = iota
	ShowIndexes
	ShowConstraints
	ShowProcedures
	ShowFunctions
	ShowDatabase
	ShowDatabases
)

// ExtractShowCommandType extracts the type of SHOW command from AST
func ExtractShowCommandType(show IShowCommandContext) ShowCommandType {
	if show == nil {
		return ShowUnknown
	}

	// Check AST tokens directly
	if show.INDEXES() != nil || show.INDEX() != nil {
		return ShowIndexes
	}
	if show.CONSTRAINTS() != nil || show.CONSTRAINT() != nil {
		return ShowConstraints
	}
	if show.PROCEDURES() != nil {
		return ShowProcedures
	}
	if show.FUNCTIONS() != nil {
		return ShowFunctions
	}
	if show.DATABASE() != nil {
		return ShowDatabase
	}
	if show.DATABASES() != nil {
		return ShowDatabases
	}

	return ShowUnknown
}

// SortItemInfo holds sort item info from AST
type SortItemInfo struct {
	Expression IExpressionContext
	IsDesc     bool
}

// ExtractSortItems extracts sort items from ORDER BY AST
func ExtractSortItems(order IOrderStContext) []SortItemInfo {
	if order == nil {
		return nil
	}

	var items []SortItemInfo
	for _, orderItem := range order.AllOrderItem() {
		info := SortItemInfo{
			Expression: orderItem.Expression(),
			IsDesc:     orderItem.DESC() != nil || orderItem.DESCENDING() != nil,
		}
		items = append(items, info)
	}

	return items
}

// GroupRows groups rows by expression values
func GroupRows(rows []map[string]interface{}, keyExprs []IExpressionContext, eval *ExpressionEvaluator) [][]map[string]interface{} {
	if len(keyExprs) == 0 {
		return [][]map[string]interface{}{rows}
	}

	groupMap := make(map[string][]map[string]interface{})
	var order []string

	for _, row := range rows {
		eval.SetRow(row)

		var keyParts []string
		for _, expr := range keyExprs {
			val := eval.Evaluate(expr)
			keyParts = append(keyParts, fmt.Sprintf("%v", val))
		}
		groupKey := strings.Join(keyParts, "\x00")

		if _, exists := groupMap[groupKey]; !exists {
			order = append(order, groupKey)
		}
		groupMap[groupKey] = append(groupMap[groupKey], row)
	}

	var result [][]map[string]interface{}
	for _, k := range order {
		result = append(result, groupMap[k])
	}
	return result
}

// ComputeAggregation computes an aggregation over a group of rows
func ComputeAggregation(funcName string, args []IExpressionContext, isCountAll bool, group []map[string]interface{}, eval *ExpressionEvaluator) interface{} {
	switch funcName {
	case "COUNT":
		if isCountAll {
			return int64(len(group))
		}
		count := int64(0)
		for _, row := range group {
			eval.SetRow(row)
			if len(args) > 0 {
				val := eval.Evaluate(args[0])
				if val != nil {
					count++
				}
			}
		}
		return count

	case "SUM":
		sum := float64(0)
		for _, row := range group {
			eval.SetRow(row)
			if len(args) > 0 {
				val := eval.Evaluate(args[0])
				sum += eval.toFloat64(val)
			}
		}
		if sum == float64(int64(sum)) {
			return int64(sum)
		}
		return sum

	case "AVG":
		if len(group) == 0 {
			return nil
		}
		sum := float64(0)
		count := 0
		for _, row := range group {
			eval.SetRow(row)
			if len(args) > 0 {
				val := eval.Evaluate(args[0])
				if val != nil {
					sum += eval.toFloat64(val)
					count++
				}
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case "MIN":
		var minVal interface{}
		for _, row := range group {
			eval.SetRow(row)
			if len(args) > 0 {
				val := eval.Evaluate(args[0])
				if val == nil {
					continue
				}
				if minVal == nil || eval.compareValues(val, minVal) < 0 {
					minVal = val
				}
			}
		}
		return minVal

	case "MAX":
		var maxVal interface{}
		for _, row := range group {
			eval.SetRow(row)
			if len(args) > 0 {
				val := eval.Evaluate(args[0])
				if val == nil {
					continue
				}
				if maxVal == nil || eval.compareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
		}
		return maxVal

	case "COLLECT":
		var result []interface{}
		for _, row := range group {
			eval.SetRow(row)
			if len(args) > 0 {
				val := eval.Evaluate(args[0])
				result = append(result, val)
			}
		}
		return result
	}

	return nil
}

// CompareValues compares two values, returns -1, 0, or 1
func CompareValues(a, b interface{}) int {
	eval := &ExpressionEvaluator{}
	return eval.compareValues(a, b)
}

// ValuesEqual checks if two values are equal
func ValuesEqual(a, b interface{}) bool {
	eval := &ExpressionEvaluator{}
	return eval.valuesEqual(a, b)
}

// ToFloat64 converts a value to float64
func ToFloat64(val interface{}) float64 {
	eval := &ExpressionEvaluator{}
	return eval.toFloat64(val)
}

// ExtractVariablesFromExpressionChain extracts variable names from an expression chain AST
func ExtractVariablesFromExpressionChain(chain IExpressionChainContext) []string {
	if chain == nil {
		return nil
	}

	seen := make(map[string]struct{})
	var vars []string
	for _, expr := range chain.AllExpression() {
		collectVariablesFromTree(expr, seen, &vars)
	}
	return vars
}

// ExtractVariableFromExpression extracts a simple variable name from an expression AST
func ExtractVariableFromExpression(expr IExpressionContext) string {
	if expr == nil {
		return ""
	}

	// Walk down to find Symbol
	xors := expr.AllXorExpression()
	if len(xors) != 1 {
		return ""
	}

	ands := xors[0].AllAndExpression()
	if len(ands) != 1 {
		return ""
	}

	nots := ands[0].AllNotExpression()
	if len(nots) != 1 {
		return ""
	}

	comp := nots[0].ComparisonExpression()
	if comp == nil {
		return ""
	}

	adds := comp.AllAddSubExpression()
	if len(adds) != 1 {
		return ""
	}

	mults := adds[0].AllMultDivExpression()
	if len(mults) != 1 {
		return ""
	}

	powers := mults[0].AllPowerExpression()
	if len(powers) != 1 {
		return ""
	}

	unarys := powers[0].AllUnaryAddSubExpression()
	if len(unarys) != 1 {
		return ""
	}

	atomic := unarys[0].AtomicExpression()
	if atomic == nil {
		return ""
	}

	propOrLabel := atomic.PropertyOrLabelExpression()
	if propOrLabel == nil {
		return ""
	}

	propExpr := propOrLabel.PropertyExpression()
	if propExpr == nil {
		return ""
	}

	// No property access - just a variable
	if len(propExpr.AllDOT()) > 0 {
		return ""
	}

	atom := propExpr.Atom()
	if atom == nil {
		return ""
	}

	// Not a function
	if atom.FunctionInvocation() != nil {
		return ""
	}

	if sym := atom.Symbol(); sym != nil {
		return sym.GetText()
	}

	return ""
}

func collectVariablesFromTree(tree antlr.Tree, seen map[string]struct{}, vars *[]string) {
	if tree == nil {
		return
	}

	switch n := tree.(type) {
	case IExpressionContext:
		for _, xor := range n.AllXorExpression() {
			collectVariablesFromTree(xor, seen, vars)
		}
		return
	case IXorExpressionContext:
		for _, and := range n.AllAndExpression() {
			collectVariablesFromTree(and, seen, vars)
		}
		return
	case IAndExpressionContext:
		for _, not := range n.AllNotExpression() {
			collectVariablesFromTree(not, seen, vars)
		}
		return
	case INotExpressionContext:
		if comp := n.ComparisonExpression(); comp != nil {
			collectVariablesFromTree(comp, seen, vars)
		}
		if exists := n.ExistsSubquery(); exists != nil {
			collectVariablesFromTree(exists, seen, vars)
		}
		return
	case IComparisonExpressionContext:
		for _, add := range n.AllAddSubExpression() {
			collectVariablesFromTree(add, seen, vars)
		}
		return
	case IAddSubExpressionContext:
		for _, mult := range n.AllMultDivExpression() {
			collectVariablesFromTree(mult, seen, vars)
		}
		return
	case IMultDivExpressionContext:
		for _, power := range n.AllPowerExpression() {
			collectVariablesFromTree(power, seen, vars)
		}
		return
	case IPowerExpressionContext:
		for _, unary := range n.AllUnaryAddSubExpression() {
			collectVariablesFromTree(unary, seen, vars)
		}
		return
	case IUnaryAddSubExpressionContext:
		if atomic := n.AtomicExpression(); atomic != nil {
			collectVariablesFromTree(atomic, seen, vars)
		}
		return
	case IAtomicExpressionContext:
		if prop := n.PropertyOrLabelExpression(); prop != nil {
			collectVariablesFromTree(prop, seen, vars)
		}
		for _, str := range n.AllStringExpression() {
			collectVariablesFromTree(str, seen, vars)
		}
		for _, list := range n.AllListExpression() {
			collectVariablesFromTree(list, seen, vars)
		}
		return
	case IStringExpressionContext:
		if prop := n.PropertyOrLabelExpression(); prop != nil {
			collectVariablesFromTree(prop, seen, vars)
		}
		return
	case IListExpressionContext:
		if prop := n.PropertyOrLabelExpression(); prop != nil {
			collectVariablesFromTree(prop, seen, vars)
		}
		for _, child := range n.GetChildren() {
			if expr, ok := child.(IExpressionContext); ok {
				collectVariablesFromTree(expr, seen, vars)
			}
		}
		return
	case IPropertyOrLabelExpressionContext:
		if prop := n.PropertyExpression(); prop != nil {
			collectVariablesFromTree(prop, seen, vars)
		}
		return
	case IPropertyExpressionContext:
		if atom := n.Atom(); atom != nil {
			if sym := atom.Symbol(); sym != nil {
				name := sym.GetText()
				if _, ok := seen[name]; !ok {
					seen[name] = struct{}{}
					*vars = append(*vars, name)
				}
				return
			}
			collectVariablesFromTree(atom, seen, vars)
		}
		return
	case IAtomContext:
		if fn := n.FunctionInvocation(); fn != nil {
			collectVariablesFromTree(fn, seen, vars)
			return
		}
		if paren := n.ParenthesizedExpression(); paren != nil && paren.Expression() != nil {
			collectVariablesFromTree(paren.Expression(), seen, vars)
			return
		}
		if subquery := n.SubqueryExist(); subquery != nil {
			collectVariablesFromTree(subquery, seen, vars)
			return
		}
		return
	case IFunctionInvocationContext:
		if chain := n.ExpressionChain(); chain != nil {
			for _, expr := range chain.AllExpression() {
				collectVariablesFromTree(expr, seen, vars)
			}
		}
		return
	}

	for _, child := range tree.GetChildren() {
		collectVariablesFromTree(child, seen, vars)
	}
}
