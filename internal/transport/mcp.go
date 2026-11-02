package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"io"

	"github.com/mgh3326/recallbeam/internal/core"
)

// MCP serves newline-delimited JSON-RPC on stdio. Diagnostics belong on stderr.
func MCP(ctx context.Context, in io.Reader, out io.Writer, s core.Searcher) error {
	scan := bufio.NewScanner(in)
	scan.Buffer(make([]byte, 4096), 1<<20)
	enc := json.NewEncoder(out)
	for scan.Scan() {
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		code := 0
		msg := ""
		var result any
		if err := json.Unmarshal(scan.Bytes(), &req); err != nil {
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
				_ = json.Unmarshal(req.Params, &p)
				v := p.ProtocolVersion
				if v != "2024-11-05" && v != "2025-03-26" && v != "2025-06-18" {
					v = "2025-06-18"
				}
				result = map[string]any{"protocolVersion": v, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "recallbeam", "version": "0.1.0"}}
			case "ping":
				result = map[string]any{}
			case "tools/list":
				result = map[string]any{"tools": []any{map[string]any{"name": "search", "description": "Search indexed source documents, tasks, comments and decisions.", "annotations": map[string]bool{"readOnlyHint": true, "destructiveHint": false, "openWorldHint": false}, "inputSchema": map[string]any{"type": "object", "required": []string{"q"}, "additionalProperties": false, "properties": map[string]any{"q": map[string]any{"type": "string", "minLength": 1, "maxLength": 256}, "model": map[string]any{"type": "string", "enum": core.Models}, "limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 100}}}}}}
			case "tools/call":
				var p struct {
					Name      string `json:"name"`
					Arguments struct {
						Q     string `json:"q"`
						Model string `json:"model"`
						Limit *int   `json:"limit"`
					} `json:"arguments"`
				}
				if err := json.Unmarshal(req.Params, &p); err != nil || p.Name != "search" {
					code = -32602
					msg = "invalid tool parameters"
					break
				}
				q := core.Query{Q: p.Arguments.Q, Model: p.Arguments.Model}
				if p.Arguments.Limit != nil {
					q.Limit = *p.Arguments.Limit
					if q.Limit == 0 {
						code = -32602
						msg = "limit must be 1 to 100"
						break
					}
				}
				if err := q.Validate(); err != nil {
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
	return scan.Err()
}
