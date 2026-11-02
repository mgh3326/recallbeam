package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

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
