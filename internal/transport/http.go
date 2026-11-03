package transport

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/mgh3326/recallbeam/internal/core"
)

func HTTP(s core.Searcher, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/search", func(w http.ResponseWriter, r *http.Request) {
		a, b := sha256.Sum256([]byte(r.Header.Get("Authorization"))), sha256.Sum256([]byte("Bearer "+token))
		if token == "" || subtle.ConstantTimeCompare(a[:], b[:]) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			http.Error(w, "invalid query encoding", 400)
			return
		}
		q := core.Query{Q: values.Get("q"), Model: values.Get("model")}
		if values.Has("limit") {
			n, err := strconv.Atoi(values.Get("limit"))
			if err != nil || n < 1 {
				http.Error(w, "invalid limit", 400)
				return
			}
			q.Limit = n
		}
		if err := q.Validate(); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		hits, err := s.Search(r.Context(), q)
		if err != nil {
			http.Error(w, "search unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": hits})
	})
	return mux
}
