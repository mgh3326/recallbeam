package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/recallbeam/internal/core"
)

type search struct {
	q     core.Query
	fail  bool
	calls int
}

func (s *search) Search(_ context.Context, q core.Query) ([]core.Hit, error) {
	s.q = q
	s.calls++
	if s.fail {
		return nil, errors.New("private database connection detail")
	}
	return []core.Hit{{Doc: core.Doc{Model: "task", ID: "123"}, Tier: 1}}, nil
}
func TestHTTPAuthValidationAndSearch(t *testing.T) {
	for _, tc := range []struct {
		query, token string
		status       int
	}{{"q=ROB-123&model=task&limit=3", "test", 200}, {"q=test", "bad", 401}, {"q=", "test", 400}, {"q=test&limit=0", "test", 400}, {"q=test&limit=101", "test", 400}, {"q=test&model=bad", "test", 400}} {
		s := &search{}
		r := httptest.NewRequest("GET", "/v1/search?"+tc.query, nil)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		HTTP(s, "test").ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.query, w.Code, w.Body)
		}
		if tc.status == 200 && (s.q.Limit != 3 || s.q.Model != "task" || !strings.Contains(w.Body.String(), `"tier":1`)) {
			t.Fatal(s.q, w.Body)
		}
	}
}
func TestHTTPDoesNotExposeStoreErrors(t *testing.T) {
	s := &search{fail: true}
	r := httptest.NewRequest("GET", "/v1/search?q=test", nil)
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	HTTP(s, "test").ServeHTTP(w, r)
	if w.Code != 503 || strings.Contains(w.Body.String(), "private") {
		t.Fatal(w)
	}
}
func TestMCPInitializeListAndReadOnlySearch(t *testing.T) {
	input := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search","arguments":{"q":"ROB-123","model":"task","limit":3}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"delete","arguments":{"q":"x"}}}
`
	s := &search{}
	var out bytes.Buffer
	if err := MCP(t.Context(), strings.NewReader(input), &out, s); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&out)
	var replies []map[string]any
	for dec.More() {
		var r map[string]any
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		replies = append(replies, r)
	}
	if len(replies) != 4 || s.calls != 1 || s.q.Q != "ROB-123" || s.q.Limit != 3 {
		t.Fatal(replies, s)
	}
	b, _ := json.Marshal(replies[1])
	if !bytes.Contains(b, []byte(`"readOnlyHint":true`)) {
		t.Fatal(string(b))
	}
	if replies[3]["error"] == nil {
		t.Fatal("unknown tool accepted")
	}
}
func TestMCPInvalidQueryAndParseError(t *testing.T) {
	s := &search{}
	var out bytes.Buffer
	err := MCP(t.Context(), strings.NewReader("{\n"+`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search","arguments":{"q":""}}}`+"\n"), &out, s)
	if err != nil || s.calls != 0 || strings.Count(out.String(), `"error"`) != 2 {
		t.Fatal(err, out.String())
	}
}

func TestMCPRejectsExplicitZeroLimit(t *testing.T) {
	s := &search{}
	var out bytes.Buffer
	err := MCP(t.Context(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"q":"test","limit":0}}}`+"\n"), &out, s)
	if err != nil || s.calls != 0 || !strings.Contains(out.String(), `"code":-32602`) {
		t.Fatal(err, out.String())
	}
}

func TestMCPCancellationUnblocksIdleInput(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- MCP(ctx, reader, io.Discard, &search{}) }()
	// Complete a frame so the scanner is running, then leave stdin open and idle.
	if _, err := io.WriteString(writer, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n"); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("MCP did not return while stdin stayed open")
	}
	if _, err := writer.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("scanner input was not closed: %v", err)
	}
}
func TestMCPRejectsMalformedInitializeParameters(t *testing.T) {
	for _, params := range []string{`{"protocolVersion":42}`, `null`, `{}`, `[]`} {
		var out bytes.Buffer
		input := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":` + params + "}\n"
		if err := MCP(t.Context(), strings.NewReader(input), &out, &search{}); err != nil || !strings.Contains(out.String(), `"code":-32602`) {
			t.Fatal(params, err, out.String())
		}
	}
}
func TestMCPRejectsArgumentsOutsideSchema(t *testing.T) {
	for _, args := range []string{`{"q":"test","limit":null}`, `{"q":"test","extra":1}`, `{"q":"test","model":null}`, `{"q":"test","limit":1.5}`, `{"q":"test","model":""}`, `null`} {
		var out bytes.Buffer
		s := &search{}
		input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":` + args + "}}\n"
		if err := MCP(t.Context(), strings.NewReader(input), &out, s); err != nil || s.calls != 0 || !strings.Contains(out.String(), `"code":-32602`) {
			t.Fatal(args, err, out.String())
		}
	}
}
func TestHTTPRejectsMalformedQueryEncoding(t *testing.T) {
	s := &search{}
	r := httptest.NewRequest("GET", "/v1/search?q=test&limit=%GG", nil)
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	HTTP(s, "test").ServeHTTP(w, r)
	if w.Code != 400 || s.calls != 0 {
		t.Fatal(w.Code, s.calls, w.Body.String())
	}
}
