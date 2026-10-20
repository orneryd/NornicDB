package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

// remoteDecodeWindow bounds each individual protocol value, while allowing an
// arbitrarily long row stream. A decoder can retain at most one bounded row.
type remoteDecodeWindow struct {
	io.Reader
	remaining int64
}

func (r *remoteDecodeWindow) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, fmt.Errorf("remote result value exceeds execution memory limit")
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.Reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func (h *httpTransport) consumeRequest(ctx context.Context, method, target string, body remoteTxRequest, consumer resultstream.Consumer) (*remoteTxResponse, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	var upstream context.Context
	var cancel context.CancelFunc
	if h.client.Timeout > 0 {
		upstream, cancel = context.WithTimeout(ctx, h.client.Timeout)
	} else {
		upstream, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	var input io.Reader = bytes.NewReader(encoded)
	var credits *io.PipeWriter
	if h.pull.Load() {
		reader, writer := io.Pipe()
		input, credits = reader, writer
		defer reader.Close()
		defer writer.Close()
		go func() { _, _ = writer.Write(append(encoded, []byte("\n1\n")...)) }()
		go func() { <-upstream.Done(); _ = writer.CloseWithError(upstream.Err()) }()
	}
	request, err := http.NewRequestWithContext(upstream, method, target, input)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(resultstream.StreamHeader, "1")
	if credits != nil {
		request.Header.Set(resultstream.StreamHeader, "pull-v1")
	}
	if h.user != "" || h.password != "" {
		request.SetBasicAuth(h.user, h.password)
	} else if h.authToken != "" {
		request.Header.Set("Authorization", h.authToken)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	window := &remoteDecodeWindow{Reader: response.Body, remaining: 16 << 20}
	decoder := json.NewDecoder(window)
	result := &remoteTxResponse{}
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("invalid remote transaction object: %v", err)
	}
	stopped := false
	for decoder.More() {
		window.remaining = 16 << 20
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		switch key {
		case "results":
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
			for decoder.More() {
				if _, err := decoder.Token(); err != nil {
					return nil, err
				}
				current := remoteResult{}
				for decoder.More() {
					window.remaining = 16 << 20
					name, err := decoder.Token()
					if err != nil {
						return nil, err
					}
					switch name {
					case "columns":
						if err := decoder.Decode(&current.Columns); err != nil {
							return nil, err
						}
					case "data":
						if _, err := decoder.Token(); err != nil {
							return nil, err
						}
						for decoder.More() {
							window.remaining = 16 << 20
							var data remoteResultRow
							if err := decoder.Decode(&data); err != nil {
								return nil, err
							}
							if err := normalizeRemoteResultRow(&data); err != nil {
								return nil, err
							}
							if !stopped {
								more, err := consumer(current.Columns, data.Row)
								if err != nil {
									return nil, err
								}
								stopped = !more
								if stopped && !resultstream.Drain(ctx) {
									cancel()
									return &remoteTxResponse{Results: []remoteResult{{Columns: current.Columns}}}, nil
								}
							}
							if credits != nil {
								_, _ = credits.Write([]byte("\n1\n"))
							}
						}
						if _, err := decoder.Token(); err != nil {
							return nil, err
						}
					default:
						var discard json.RawMessage
						if err := decoder.Decode(&discard); err != nil {
							return nil, err
						}
					}
				}
				if _, err := decoder.Token(); err != nil {
					return nil, err
				}
				result.Results = append(result.Results, current)
			}
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
		case "errors":
			if err := decoder.Decode(&result.Errors); err != nil {
				return nil, err
			}
		default:
			var discard json.RawMessage
			if err := decoder.Decode(&discard); err != nil {
				return nil, err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if len(result.Errors) > 0 {
		first := result.Errors[0]
		return nil, localizedError(localization.StorageRemoteError(first.Code, first.Message), nil)
	}
	return result, nil
}

func normalizeRemoteResultRow(data *remoteResultRow) error {
	if data.REST != nil && len(data.REST) != len(data.RawRow) {
		return localizedError(localization.StorageRemoteRESTRowLengthMismatch(), nil)
	}
	entities := make(map[string]interface{})
	for _, node := range data.Graph.Nodes {
		entities[fmt.Sprint(node["elementId"])] = map[string]interface{}{
			remoteIdentityKey: remoteNumericIdentity{id: remoteHTTPNumericID(node["id"]), start: -1, end: -1},
			"elementId":       node["elementId"], "labels": node["labels"], "properties": node["properties"],
		}
	}
	for _, edge := range data.Graph.Relationships {
		entities[fmt.Sprint(edge["elementId"])] = map[string]interface{}{
			remoteIdentityKey: remoteNumericIdentity{id: remoteHTTPNumericID(edge["id"]), start: remoteHTTPEndpointID(edge, "startNode", entities), end: remoteHTTPEndpointID(edge, "endNode", entities)},
			"elementId":       edge["elementId"], "type": edge["type"], "properties": edge["properties"],
			"startNodeElementId": edge["startNodeElementId"], "endNodeElementId": edge["endNodeElementId"],
		}
	}
	index := 0
	data.Row = make([]any, len(data.RawRow))
	for i, raw := range data.RawRow {
		var rest json.RawMessage
		if len(data.REST) == len(data.RawRow) {
			rest = data.REST[i]
		}
		value, err := decodeRemoteHTTPValueWithREST(raw, rest, data.Meta, &index, entities)
		if err != nil {
			return err
		}
		data.Row[i] = value
	}
	return nil
}
