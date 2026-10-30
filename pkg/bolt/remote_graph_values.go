package bolt

import "github.com/orneryd/nornicdb/pkg/storage"

// Marked remote entities already carry source identities. Passing them through
// local NodeElementID generation would incorrectly replace the source database.
func encodeRemoteEntityInto(dst []byte, value storage.RemoteEntityValue, utc, v5 bool) []byte {
	if value == nil {
		return append(dst, 0xc0)
	}
	id, start, end := value.LegacyIDs()
	properties, _ := value["properties"].(map[string]interface{})
	if properties == nil {
		properties = map[string]interface{}{}
	}
	if kind, relationship := value["type"].(string); relationship {
		fields := byte(5)
		if v5 {
			fields = 8
		}
		dst = append(dst, 0xb0+fields, 0x52)
		for _, n := range []int64{id, start, end} {
			dst = encodePackStreamIntInto(dst, n)
		}
		dst = encodePackStreamStringInto(dst, kind)
		dst = encodePackStreamValueIntoWithUTC(dst, properties, utc)
		if v5 {
			for _, key := range []string{"elementId", "startNodeElementId", "endNodeElementId"} {
				text, _ := value[key].(string)
				dst = encodePackStreamStringInto(dst, text)
			}
		}
		return dst
	}
	fields := byte(3)
	if v5 {
		fields = 4
	}
	dst = append(dst, 0xb0+fields, 0x4e)
	dst = encodePackStreamIntInto(dst, id)
	dst = encodePackStreamValueIntoWithUTC(dst, value["labels"], utc)
	dst = encodePackStreamValueIntoWithUTC(dst, properties, utc)
	if v5 {
		elementID, _ := value["elementId"].(string)
		dst = encodePackStreamStringInto(dst, elementID)
	}
	return dst
}

func encodeRemotePathInto(dst []byte, path storage.RemotePathValue, utc, v5 bool) []byte {
	if path == nil {
		return append(dst, 0xc0)
	}
	if _, _, valid := path.Parts(); !valid {
		return append(dst, 0xc0)
	}
	nodes := make([]storage.RemoteEntityValue, 0, (len(path)+1)/2)
	edges := make([]storage.RemoteEntityValue, 0, len(path)/2)
	nodeIndex := make(map[string]int)
	edgeIndex := make(map[string]int)
	sequence := make([]int64, 0, len(path)-1)
	for i, value := range path {
		id, _ := value["elementId"].(string)
		if i%2 == 0 {
			if _, ok := nodeIndex[id]; !ok {
				nodeIndex[id] = len(nodes)
				nodes = append(nodes, value)
			}
		} else {
			if _, ok := edgeIndex[id]; !ok {
				edgeIndex[id] = len(edges)
				edges = append(edges, value)
			}
		}
	}
	for i := 1; i < len(path); i += 2 {
		id, _ := path[i]["elementId"].(string)
		from, _ := path[i-1]["elementId"].(string)
		to, _ := path[i+1]["elementId"].(string)
		start, _ := path[i]["startNodeElementId"].(string)
		end, _ := path[i]["endNodeElementId"].(string)
		direction := int64(1)
		if start != from || end != to {
			direction = -1
		}
		sequence = append(sequence, direction*int64(edgeIndex[id]+1), int64(nodeIndex[to]))
	}
	dst = append(dst, 0xb3, 0x50)
	dst = appendPackStreamListHeader(dst, len(nodes))
	for _, node := range nodes {
		dst = encodeRemoteEntityInto(dst, node, utc, v5)
	}
	dst = appendPackStreamListHeader(dst, len(edges))
	for _, edge := range edges {
		fields := byte(3)
		if v5 {
			fields = 4
		}
		dst = append(dst, 0xb0+fields, 0x72)
		id, _, _ := edge.LegacyIDs()
		dst = encodePackStreamIntInto(dst, id)
		kind, _ := edge["type"].(string)
		dst = encodePackStreamStringInto(dst, kind)
		props, _ := edge["properties"].(map[string]interface{})
		if props == nil {
			props = map[string]interface{}{}
		}
		dst = encodePackStreamValueIntoWithUTC(dst, props, utc)
		if v5 {
			elementID, _ := edge["elementId"].(string)
			dst = encodePackStreamStringInto(dst, elementID)
		}
	}
	return encodePackStreamValueIntoWithUTC(dst, sequence, utc)
}
