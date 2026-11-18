package payments

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

func Handler(s *Store, keys map[string]string) http.Handler {
	mux := http.NewServeMux()
	send := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { send(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		merchant, ok := keys[strings.TrimPrefix(auth, "Bearer ")]
		if !strings.HasPrefix(auth, "Bearer ") || !ok || merchant == "" {
			send(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		if r.URL.Path == "/payments" && r.Method == http.MethodPost {
			var req Request
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&req); err != nil {
				send(w, 400, map[string]string{"error": "invalid JSON body"})
				return
			}
			if dec.Decode(&struct{}{}) != io.EOF {
				send(w, 400, map[string]string{"error": "expected one JSON object"})
				return
			}
			p, replay, err := s.Create(r.Context(), merchant, r.Header.Get("Idempotency-Key"), req)
			if err != nil {
				code := 500
				message := "internal error"
				if errors.Is(err, ErrConflict) {
					code = 409
					message = err.Error()
				}
				if errors.Is(err, ErrInvalid) {
					code = 400
					message = err.Error()
				}
				send(w, code, map[string]string{"error": message})
				return
			}
			if replay {
				w.Header().Set("Idempotent-Replay", "true")
			}
			send(w, 202, p)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/payments/") && r.Method == http.MethodGet {
			p, err := s.Get(r.Context(), merchant, strings.TrimPrefix(r.URL.Path, "/payments/"))
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					send(w, 404, map[string]string{"error": "not found"})
				} else {
					send(w, 500, map[string]string{"error": "internal error"})
				}
				return
			}
			send(w, 200, p)
			return
		}
		send(w, 404, map[string]string{"error": "not found"})
	})
	return mux
}
