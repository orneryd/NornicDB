package config

import (
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
	"gopkg.in/yaml.v3"
)

// YAML type errors can quote the offending scalar. Retain credential context
// even when decoding the typed config fails, without changing the parser cause.
func configDisplayError(err error, data []byte, defaults *Config) error {
	var credentials []string
	var collectValue func(reflect.Value, string)
	collectValue = func(value reflect.Value, name string) {
		if value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
			if !value.IsNil() {
				collectValue(value.Elem(), name)
			}
			return
		}
		switch value.Kind() {
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				collectValue(value.Field(i), name+"."+value.Type().Field(i).Name)
			}
		case reflect.String:
			if observability.IsSensitiveFieldName(name) {
				credentials = append(credentials, value.String())
			}
		}
	}
	collectValue(reflect.ValueOf(defaults), "")
	var document yaml.Node
	if yaml.Unmarshal(data, &document) == nil {
		visited := make(map[*yaml.Node]bool)
		var collect func(*yaml.Node, string, bool)
		collect = func(node *yaml.Node, name string, sensitive bool) {
			if node == nil || visited[node] {
				return
			}
			visited[node] = true
			defer delete(visited, node)
			sensitive = sensitive || observability.IsSensitiveFieldName(name)
			if node.Kind == yaml.ScalarNode && sensitive {
				credentials = append(credentials, node.Value)
			}
			if node.Kind == yaml.MappingNode {
				for i := 0; i+1 < len(node.Content); i += 2 {
					collect(node.Content[i+1], strings.TrimPrefix(name+"."+node.Content[i].Value, "."), sensitive)
				}
			} else {
				for _, child := range node.Content {
					collect(child, name, sensitive)
				}
				collect(node.Alias, name, sensitive)
			}
		}
		collect(&document, "", false)
	} else {
		// Syntax/composition errors can quote source data (for example an
		// unknown YAML alias) before credential fields can be recovered. Keep
		// the parser cause, and project its opaque detail through the owner.
		return observability.RedactError(err, err.Error())
	}
	// yaml.v3 abbreviates a scalar longer than ten bytes to seven plus "..."
	// in TypeError text. Supply that parser-owned representation as well.
	for _, credential := range append([]string(nil), credentials...) {
		if len(credential) > 10 {
			credentials = append(credentials, credential[:7]+"...")
		}
	}
	return observability.RedactError(err, credentials...)
}
