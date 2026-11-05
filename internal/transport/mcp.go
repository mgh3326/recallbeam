package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/mgh3326/recallbeam/internal/core"
)

// MCP serves newline-delimited JSON-RPC on stdio. It owns closable input
// and closes it on return to unblock the scanner. Diagnostics belong on stderr.
func MCP(ctx context.Context, in io.Reader, out io.Writer, s core.Searcher) error {
	if closer, ok := in.(io.Closer); ok {
		defer closer.Close()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	type scanned struct {
		data []byte
		err  error
	}
	lines := make(chan scanned)
	go func() {
		scan := bufio.NewScanner(in)
		scan.Buffer(make([]byte, 4096), 1<<20)
		for scan.Scan() {
			select {
			case lines <- scanned{data: append([]byte(nil), scan.Bytes()...)}:
			case <-readCtx.Done():
				return
			}
		}
		err := scan.Err()
		if err == nil {
			err = io.EOF
		}
		select {
		case lines <- scanned{err: err}:
		case <-readCtx.Done():
		}
	}()
	enc := json.NewEncoder(out)
	for {
		var line scanned
		select {
		case <-ctx.Done():
			return ctx.Err()
		case line = <-lines:
			if line.err == io.EOF {
				return nil
			}
			if line.err != nil {
				return line.err
			}
		}

		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		code := 0
		msg := ""
		var result any
		if err := json.Unmarshal(line.data, &req); err != nil {
			code = -32700
			msg = "parse error"
		} else if req.JSONRPC != "2.0" || req.Method == "" {
			code = -32600
			msg = "invalid request"
		} else {
			if len(req.ID) == 0 {
				continue
			}
			switch req.Method {
			case "initialize":
				var p struct {
					ProtocolVersion string `json:"protocolVersion"`
				}
				if err := json.Unmarshal(req.Params, &p); err != nil || p.ProtocolVersion == "" {
					code = -32602
					msg = "invalid initialize parameters"
					break
				}
				v := p.ProtocolVersion
				if v != "2024-11-05" && v != "2025-03-26" && v != "2025-06-18" {
					v = "2025-06-18"
				}
				result = map[string]any{"protocolVersion": v, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "recallbeam", "version": "0.1.0"}}
			case "ping":
				result = map[string]any{}
			case "tools/list":
				result = map[string]any{"tools": []any{map[string]any{"name": "search", "description": "Search indexed source documents, tasks and decisions.", "annotations": map[string]bool{"readOnlyHint": true, "destructiveHint": false, "openWorldHint": false}, "inputSchema": map[string]any{"type": "object", "required": []string{"q"}, "additionalProperties": false, "properties": map[string]any{"q": map[string]any{"type": "string", "minLength": 1, "maxLength": 256}, "model": map[string]any{"type": "string", "enum": core.IndexedModels}, "limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 100}}}}}}
			case "tools/call":
				var p struct {
					Name      string          `json:"name"`
					Arguments json.RawMessage `json:"arguments"`
				}
				if err := json.Unmarshal(req.Params, &p); err != nil || p.Name != "search" {
					code = -32602
					msg = "invalid tool parameters"
					break
				}
				q, err := searchArguments(p.Arguments)
				if err != nil {
					code = -32602
					msg = err.Error()
					break
				}

				hits, err := s.Search(ctx, q)
				if err != nil {
					result = map[string]any{"isError": true, "content": []any{map[string]string{"type": "text", "text": "search unavailable"}}}
				} else {
					b, _ := json.Marshal(map[string]any{"results": hits})
					result = map[string]any{"content": []any{map[string]string{"type": "text", "text": string(b)}}}
				}
			default:
				code = -32601
				msg = "method not found"
			}
		}
		id := req.ID
		if len(id) == 0 {
			id = json.RawMessage("null")
		}
		response := map[string]any{"jsonrpc": "2.0", "id": id}
		if code != 0 {
			response["error"] = map[string]any{"code": code, "message": msg}
		} else {
			response["result"] = result
		}
		if err := enc.Encode(response); err != nil {
			return err
		}
	}
}

func searchArguments(raw json.RawMessage) (core.Query, error) {
	var args map[string]json.RawMessage
	var q core.Query
	if err := json.Unmarshal(raw, &args); err != nil || args == nil {
		return q, errors.New("invalid search arguments")
	}
	for name, value := range args {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return q, errors.New("search arguments cannot be null")
		}
		var err error
		switch name {
		case "q":
			err = json.Unmarshal(value, &q.Q)
		case "model":
			err = json.Unmarshal(value, &q.Model)
			if err == nil && !core.Indexed(q.Model) {
				err = errors.New("unknown model")
			}
		case "limit":
			err = json.Unmarshal(value, &q.Limit)
			if err == nil && q.Limit == 0 {
				err = errors.New("limit must be 1 to 100")
			}
		default:
			return q, errors.New("unknown search argument")
		}
		if err != nil {
			return q, errors.New("invalid search argument")
		}
	}
	return q, q.Validate()
}
