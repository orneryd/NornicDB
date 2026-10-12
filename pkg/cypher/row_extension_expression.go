package cypher

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (e *StorageExecutor) evaluateRowExtensionFunction(function, argument string, values map[string]interface{}) (value interface{}, matched bool, resolved bool, err error) {
	name := lowerASCII(function)
	// An argument's error is the function's: eval keeps the first one, and it
	// replaces whatever the function would return.
	var argumentErr error
	defer func() {
		if argumentErr != nil {
			value, resolved, err = nil, false, argumentErr
		}
	}()
	eval := func(expression string) (interface{}, bool) {
		value, ok, err := e.evaluateRowValue(strings.TrimSpace(expression), values)
		if err != nil {
			if argumentErr == nil {
				argumentErr = err
			}
			return nil, false
		}
		return value, ok
	}
	one := func() (interface{}, bool) { return eval(argument) }
	args := func() ([]interface{}, bool) {
		parts := splitTopLevelComma(argument)
		out := make([]interface{}, len(parts))
		for index, part := range parts {
			value, ok := eval(part)
			if !ok {
				return nil, false
			}
			out[index] = value
		}
		return out, true
	}
	if PluginFunctionLookup != nil && strings.Contains(name, ".") {
		if handler, found := PluginFunctionLookup(name); found {
			arguments, ok := args()
			if !ok {
				return nil, true, false, nil
			}
			result, err := callPluginHandler(handler, arguments)
			if err != nil {
				return nil, true, false, err
			}
			return result, true, true, nil
		}
	}
	if value, matched, resolved, err := e.evaluateRowTemporalComponent(name, argument, values); matched {
		return value, true, resolved, err
	}
	switch name {
	case "rand":
		if strings.TrimSpace(argument) != "" {
			return nil, true, false, nil
		}
		return randomCypherFloat(), true, true, nil
	case "randomuuid":
		if strings.TrimSpace(argument) != "" {
			return nil, true, false, nil
		}
		return e.generateUUID(), true, true, nil
	case "toupper", "tolower", "tostring", "tostringornull", "tointeger", "toint", "tointegerornull", "tofloat", "tofloatornull", "toboolean", "tobooleanornull":
		value, resolved, err := e.evaluateRowGraphFunction(function, argument, values)
		return value, true, resolved, err
	case "substring", "left", "right", "replace", "split", "tail":
		value, resolved, err := e.evaluateRowGraphFunction(function, argument, values)
		return value, true, resolved, err
	case "lpad", "rpad":
		arguments, ok := args()
		if !ok || len(arguments) < 2 || len(arguments) > 3 {
			return nil, true, false, nil
		}
		text, textOK := arguments[0].(string)
		length, lengthOK := toInt(arguments[1])
		if !textOK || !lengthOK || length < 0 {
			return nil, true, false, nil
		}
		padding := " "
		if len(arguments) == 3 {
			var paddingOK bool
			padding, paddingOK = arguments[2].(string)
			if !paddingOK || padding == "" {
				return nil, true, false, nil
			}
		}
		return padRowString(text, length, padding, name == "lpad"), true, true, nil
	case "format":
		value, resolved, err := e.evaluateRowGraphFunction(function, argument, values)
		return value, true, resolved, err
	case "apoc.create.uuid":
		return e.generateUUID(), true, true, nil
	case "apoc.text.join":
		values, ok := args()
		if !ok || len(values) != 2 {
			return nil, true, false, nil
		}
		separator, separatorOK := values[1].(string)
		if !separatorOK {
			return nil, true, false, nil
		}
		items := toAnySlice(values[0])
		parts := make([]string, len(items))
		for index, item := range items {
			parts[index] = fmt.Sprint(item)
		}
		return strings.Join(parts, separator), true, true, nil
	case "apoc.coll.flatten", "apoc.coll.toset", "apoc.coll.sum", "apoc.coll.avg", "apoc.coll.min", "apoc.coll.max", "apoc.coll.reverse":
		value, ok := one()
		if !ok {
			return nil, true, false, nil
		}
		if name == "apoc.coll.flatten" {
			return flattenList(value), true, true, nil
		}
		switch name {
		case "apoc.coll.toset":
			return toSet(value), true, true, nil
		case "apoc.coll.sum":
			return apocCollSum(value), true, true, nil
		case "apoc.coll.avg":
			return apocCollAvg(value), true, true, nil
		case "apoc.coll.min":
			return apocCollMin(value), true, true, nil
		case "apoc.coll.reverse":
			return apocCollReverse(value), true, true, nil
		default:
			return apocCollMax(value), true, true, nil
		}
	case "apoc.convert.tojson":
		value, ok := one()
		if !ok {
			return nil, true, false, nil
		}
		encoded, err := json.Marshal(value)
		return string(encoded), true, err == nil, nil
	case "apoc.convert.fromjsonmap", "apoc.convert.fromjsonlist":
		value, ok := one()
		text, textOK := value.(string)
		if !ok || !textOK {
			return nil, true, false, nil
		}
		if name == "apoc.convert.fromjsonmap" {
			var result map[string]interface{}
			err := json.Unmarshal([]byte(text), &result)
			return result, true, err == nil, nil
		}
		var result []interface{}
		err := json.Unmarshal([]byte(text), &result)
		return result, true, err == nil, nil
	case "apoc.meta.type":
		value, ok := one()
		return apocValueTypeName(value), true, ok, nil
	case "apoc.meta.istype":
		values, ok := args()
		if !ok || len(values) != 2 {
			return nil, true, false, nil
		}
		typeName, typeOK := values[1].(string)
		return strings.EqualFold(apocValueTypeName(values[0]), typeName), true, typeOK, nil
	case "apoc.map.merge", "apoc.map.fromlists":
		values, ok := args()
		if !ok || len(values) != 2 {
			return nil, true, false, nil
		}
		if name == "apoc.map.merge" {
			return mergeMaps(values[0], values[1]), true, true, nil
		}
		return fromLists(values[0], values[1]), true, true, nil
	case "apoc.map.frompairs":
		value, ok := one()
		return fromPairs(value), true, ok, nil
	case "kalman.init", "kalman.adaptive.init":
		var config map[string]interface{}
		if strings.TrimSpace(argument) != "" {
			value, ok := one()
			if !ok {
				return nil, true, false, nil
			}
			config, _ = value.(map[string]interface{})
		}
		if name == "kalman.init" {
			return kalmanInit(config), true, true, nil
		}
		return kalmanAdaptiveInit(config), true, true, nil
	case "kalman.process", "kalman.predict", "kalman.state", "kalman.reset", "kalman.velocity.init", "kalman.velocity.process", "kalman.velocity.predict", "kalman.adaptive.process":
		values, ok := args()
		if !ok {
			return nil, true, false, nil
		}
		return evaluateRowKalman(name, values), true, true, nil
	}
	return nil, false, false, nil
}

func padRowString(value string, length int, padding string, left bool) string {
	characters := []rune(value)
	if len(characters) >= length {
		return string(characters[:length])
	}
	padCharacters := []rune(padding)
	needed := length - len(characters)
	result := make([]rune, 0, length)
	if !left {
		result = append(result, characters...)
	}
	for index := 0; index < needed; index++ {
		result = append(result, padCharacters[index%len(padCharacters)])
	}
	if left {
		result = append(result, characters...)
	}
	return string(result)
}

func (e *StorageExecutor) evaluateRowTemporalComponent(name, argument string, values map[string]interface{}) (interface{}, bool, bool, error) {
	if name == "timestamp" {
		if strings.TrimSpace(argument) != "" {
			return nil, true, false, nil
		}
		return time.Now().UnixMilli(), true, true, nil
	}
	if name == "localtime" || name == "localdatetime" || name == "datetime" || name == "date" || name == "time" || name == "duration" || strings.HasSuffix(name, ".truncate") {
		return nil, false, false, nil // constructors are handled by evaluateTemporalConstructor
	}
	dot := strings.IndexByte(name, '.')
	if dot < 0 {
		return nil, false, false, nil
	}
	kind, component := name[:dot], name[dot+1:]
	if kind != "date" && kind != "datetime" {
		return nil, false, false, nil
	}
	value, ok, err := e.evaluateRowValue(strings.TrimSpace(argument), values)
	if err != nil {
		return nil, true, false, err
	}
	if !ok {
		return nil, true, false, nil
	}
	temporal, valid := rowTemporalTime(value)
	if !valid {
		return nil, true, true, nil
	}
	switch component {
	case "year":
		return int64(temporal.Year()), true, true, nil
	case "month":
		return int64(temporal.Month()), true, true, nil
	case "day":
		return int64(temporal.Day()), true, true, nil
	case "hour":
		return int64(temporal.Hour()), true, true, nil
	case "minute":
		return int64(temporal.Minute()), true, true, nil
	case "second":
		return int64(temporal.Second()), true, true, nil
	case "week":
		_, week := temporal.ISOWeek()
		return int64(week), true, true, nil
	case "quarter":
		return int64((int(temporal.Month())-1)/3 + 1), true, true, nil
	case "dayofweek":
		day := int64(temporal.Weekday())
		if day == 0 {
			day = 7
		}
		return day, true, true, nil
	case "dayofyear", "ordinalday":
		return int64(temporal.YearDay()), true, true, nil
	case "weekyear":
		year, _ := temporal.ISOWeek()
		return int64(year), true, true, nil
	default:
		return nil, false, false, nil
	}
}

func rowTemporalTime(value interface{}) (time.Time, bool) {
	switch typed := value.(type) {
	case time.Time:
		return typed, true
	case CypherDate:
		return typed.Time, true
	case CypherDateTime:
		return typed.Time, true
	case CypherLocalDateTime:
		return typed.Time, true
	case CypherTime:
		return typed.Time, true
	case CypherLocalTime:
		return typed.Time, true
	case string:
		parsed := parseDateTime(typed)
		return parsed, !parsed.IsZero()
	default:
		return time.Time{}, false
	}
}

func evaluateRowKalman(name string, values []interface{}) interface{} {
	switch name {
	case "kalman.process":
		if len(values) < 2 {
			return nil
		}
		measurement, _ := toFloat64(values[0])
		state, _ := values[1].(string)
		target := 0.0
		if len(values) > 2 {
			target, _ = toFloat64(values[2])
		}
		return kalmanProcess(measurement, state, target)
	case "kalman.predict":
		if len(values) != 2 {
			return nil
		}
		state, _ := values[0].(string)
		steps, _ := toInt(values[1])
		return kalmanPredict(state, steps)
	case "kalman.state":
		if len(values) != 1 {
			return nil
		}
		state, _ := values[0].(string)
		return kalmanStateValue(state)
	case "kalman.reset":
		if len(values) != 1 {
			return nil
		}
		state, _ := values[0].(string)
		return kalmanReset(state)
	case "kalman.velocity.init":
		if len(values) < 2 {
			return kalmanVelocityInit(0, 0, false)
		}
		pos, _ := toFloat64(values[0])
		vel, _ := toFloat64(values[1])
		return kalmanVelocityInit(pos, vel, true)
	case "kalman.velocity.process":
		if len(values) != 2 {
			return nil
		}
		measurement, _ := toFloat64(values[0])
		state, _ := values[1].(string)
		return kalmanVelocityProcess(measurement, state)
	case "kalman.velocity.predict":
		if len(values) != 2 {
			return nil
		}
		state, _ := values[0].(string)
		steps, _ := toInt(values[1])
		return kalmanVelocityPredict(state, steps)
	case "kalman.adaptive.process":
		if len(values) != 2 {
			return nil
		}
		measurement, _ := toFloat64(values[0])
		state, _ := values[1].(string)
		return kalmanAdaptiveProcess(measurement, state)
	}
	return nil
}
