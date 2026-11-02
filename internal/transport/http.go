package transport

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
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
		q := core.Query{Q: r.URL.Query().Get("q"), Model: r.URL.Query().Get("model")}
		if r.URL.Query().Has("limit") {
			n, err := strconv.Atoi(r.URL.Query().Get("limit"))
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
