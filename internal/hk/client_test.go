package hk

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mgh3326/recallbeam/internal/core"
)

func TestFakeHKBootstrapDeltaAndFetch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Error("missing auth")
		}
		switch r.URL.Path {
		case "/v1/sync/bootstrap":
			if r.URL.Query().Get("model") != "document" || r.URL.Query().Get("cursor") != "opaque +" {
				t.Error(r.URL)
			}
			fmt.Fprint(w, `{"rows":[{"id":"7","key":"design/a"}],"cursor":"next","max_sync_id":42,"schema_version":1}`)
		case "/v1/sync/delta":
			fmt.Fprintln(w, `{"sync_id":43,"model":"document","model_id":"7","action":"U","at":"2026-01-01T00:00:00Z"}`)
			fmt.Fprintln(w, `{"_metadata_":{"last_sync_id":43,"max_sync_id":43,"schema_version":1}}`)
		case "/v1/documents/design/a":
			fmt.Fprint(w, `{"id":7,"key":"design/a","body":"한국어","updated_at":"2026-01-01T00:00:00Z"}`)
		case "/v1/tasks/9":
			fmt.Fprint(w, `{"id":9,"title":"Task","refs":{"linear":{"brief":"details"}},"events":[{"note":"done"}]}`)
		case "/v1/sync/rows/task_comment/3":
			fmt.Fprint(w, `{"id":3,"body":"comment","created_at":"2026-01-01T00:00:00Z"}`)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			fmt.Fprint(w, `{"error":"not_found"}`)
		}
	}))
	defer server.Close()
	c := Client{Base: server.URL, Token: "test"}
	p, err := c.Bootstrap(t.Context(), "document", "opaque +")
	if err != nil || p.Max != 42 || p.Next != "next" {
		t.Fatalf("%+v %v", p, err)
	}
	d, err := c.Delta(t.Context(), 42)
	if err != nil || len(d.Changes) != 1 || d.Last != 43 {
		t.Fatalf("%+v %v", d, err)
	}
	doc, err := c.Fetch(t.Context(), "document", p.Rows[0])
	if err != nil || doc.Body != "한국어" {
		t.Fatalf("%+v %v", doc, err)
	}
	task, err := c.Fetch(t.Context(), "task", core.Ref{ID: "9"})
	if err != nil || task.Body == "" {
		t.Fatalf("%+v %v", task, err)
	}
}
func TestFakeHK410AndMalformedDelta(t *testing.T) {
	for _, body := range []string{"410", `{"sync_id":1}`, "{", `{"_metadata_":{"last_sync_id":1}}` + "\n" + `{}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if body == "410" {
					w.WriteHeader(410)
					fmt.Fprint(w, `{"reset":true,"min_sync_id":20}`)
					return
				}
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			c := Client{Base: server.URL}
			_, err := c.Delta(t.Context(), 0)
			if err == nil {
				t.Fatal("accepted incomplete feed")
			}
			if body == "410" && !errors.Is(err, core.ErrReset) {
				t.Fatal(err)
			}
		})
	}
}

func TestMissingReadEndpointIsNotATombstone(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	c := Client{Base: server.URL}
	_, err := c.Fetch(t.Context(), "task_comment", core.Ref{ID: "1"})
	if err == nil || errors.Is(err, core.ErrNotFound) {
		t.Fatal("missing endpoint could silently delete indexed rows", err)
	}
}
func TestResolveUsingExistingPaginatedAPIs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sync/bootstrap":
			if r.URL.Query().Get("model") == "document" {
				fmt.Fprint(w, `{"rows":[{"id":"7","key":"design/a"}],"schema_version":1}`)
				return
			}
			if r.URL.Query().Get("cursor") == "" {
				fmt.Fprint(w, `{"rows":[{"id":"1"}],"cursor":"next","schema_version":1}`)
			} else {
				fmt.Fprint(w, `{"rows":[{"id":"2","updated_at":"2026-01-01T00:00:00Z"}],"schema_version":1}`)
			}
		case "/v1/documents/design/a":
			fmt.Fprint(w, `{"key":"design/a","body":"doc"}`)
		case "/v1/tasks/1/comments":
			fmt.Fprint(w, `{"comments":[]}`)
		case "/v1/tasks/2/comments":
			if r.URL.Query().Get("after_id") != "8" {
				t.Error(r.URL)
			}
			fmt.Fprint(w, `{"comments":[{"id":9,"body":"comment","created_at":"2026-01-01T00:00:00Z"}]}`)
		case "/v1/tasks/1":
			fmt.Fprint(w, `{"refs":{}}`)
		case "/v1/tasks/2":
			fmt.Fprint(w, `{"refs":{"decision_request":{"id":"decision-1","question":"Which?","resolution":{"reason":"chosen"}}}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := Client{Base: server.URL}
	for _, tc := range []struct{ model, id, want string }{{"document", "7", "doc"}, {"task_comment", "9", "comment"}, {"decision", "decision-1", "chosen"}} {
		d, err := c.Fetch(t.Context(), tc.model, core.Ref{ID: tc.id})
		if err != nil || !strings.Contains(d.Body, tc.want) {
			t.Fatalf("%s: %+v %v", tc.model, d, err)
		}
	}
	_, err := c.Fetch(t.Context(), "decision", core.Ref{ID: "missing"})
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatal(err)
	}
}

// No network is used: the transport records whether credentials reach it.
func TestSourceURLPolicyBeforeCredentials(t *testing.T) {
	for _, tc := range []struct {
		base    string
		allowed bool
	}{
		{"https://source.invalid", true},
		{"http://127.0.0.1", true},
		{"http://[::1]", true},
		{"http://source.invalid", false},
		{"http://192.0.2.1", false},
		{"http://localhost", false},
		{"ftp://source.invalid", false},
		{"https://user:password@source.invalid", false},
		{"https://source.invalid/#fragment", false},
		{"/relative", false},
	} {
		t.Run(tc.base, func(t *testing.T) {
			calls := 0
			c := Client{Base: tc.base, Token: "test-secret", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Header.Get("Authorization") != "Bearer test-secret" {
					t.Error("authorized request lost token")
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"_metadata_":{"last_sync_id":0,"max_sync_id":0,"schema_version":1}}`))}, nil
			})}}
			_, err := c.Delta(t.Context(), 0)
			if tc.allowed {
				if err != nil || calls != 1 {
					t.Fatalf("allowed URL: calls=%d err=%v", calls, err)
				}
			} else if err == nil || calls != 0 {
				t.Fatalf("unsafe URL reached transport: calls=%d err=%v", calls, err)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSourceHTTPSDoesNotRedirectCredentials(t *testing.T) {
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
		fmt.Fprintln(w, `{"_metadata_":{"last_sync_id":0,"max_sync_id":0,"schema_version":1}}`)
	}))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-secret" {
			t.Error("missing source token")
		}
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()
	injected := source.Client()
	c := Client{Base: source.URL, Token: "test-secret", HTTP: injected}
	_, err := c.Delta(t.Context(), 0)
	if err == nil || leaked.Load() {
		t.Fatalf("redirect followed: err=%v leaked=%v", err, leaked.Load())
	}
	if injected.CheckRedirect != nil {
		t.Fatal("mutated caller's HTTP client")
	}
}
