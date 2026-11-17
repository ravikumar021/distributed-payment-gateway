package payments

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var ctx = context.Background()

func setup(t *testing.T) *Store {
	t.Helper()
	s, e := Open(filepath.Join(t.TempDir(), "test.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func create(t *testing.T, s *Store, scenario string) Payment {
	t.Helper()
	p, _, e := s.Create(ctx, "a", "key", Request{100, "EUR", scenario})
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func TestIdempotency(t *testing.T) {
	s := setup(t)
	a := create(t, s, "success")
	b, replay, e := s.Create(ctx, "a", "key", Request{100, "eur", ""})
	if e != nil || !replay || a.ID != b.ID {
		t.Fatal(b, replay, e)
	}
	_, _, e = s.Create(ctx, "a", "key", Request{101, "EUR", ""})
	if !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
}
func TestMerchantIsolation(t *testing.T) {
	s := setup(t)
	a := create(t, s, "success")
	if _, e := s.Get(ctx, "b", a.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	b, _, e := s.Create(ctx, "b", "key", Request{100, "EUR", ""})
	if e != nil || b.ID == a.ID {
		t.Fatal(e)
	}
}
func TestValidation(t *testing.T) {
	s := setup(t)
	for _, r := range []Request{{0, "EUR", ""}, {-1, "EUR", ""}, {100, "BAD", ""}, {100, "EUR", "bogus"}, {1000000001, "EUR", ""}} {
		if _, _, e := s.Create(ctx, "a", "k", r); !errors.Is(e, ErrInvalid) {
			t.Fatal(r, e)
		}
	}
	if _, _, e := s.Create(ctx, "a", "", Request{100, "EUR", ""}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}
func TestConcurrentDuplicateAcrossConnections(t *testing.T) {
	s := setup(t)
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _, e := s.Create(ctx, "a", "same", Request{100, "EUR", ""})
			ids <- p.ID
			errs <- e
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("duplicate payment")
		}
	}
	var count int
	s.DB.QueryRow("SELECT COUNT(*) FROM outbox").Scan(&count)
	if count != 1 {
		t.Fatal(count)
	}
}
func TestTwoStores(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	a, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, s := range []*Store{a, b} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			_, _, e := s.Create(ctx, "a", "key", Request{100, "EUR", ""})
			errs <- e
		}(s)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var n int
	a.DB.QueryRow("SELECT COUNT(*) FROM payments").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
}
func TestAtomicOutboxRollback(t *testing.T) {
	s := setup(t)
	_, e := s.DB.Exec("CREATE TRIGGER fail_event BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.Create(ctx, "a", "key", Request{100, "EUR", ""}); e == nil {
		t.Fatal("expected rollback")
	}
	var n int
	s.DB.QueryRow("SELECT COUNT(*) FROM payments").Scan(&n)
	if n != 0 {
		t.Fatal(n)
	}
}
func TestRetryAndTerminalStates(t *testing.T) {
	for _, tc := range []struct {
		scenario, status string
		attempts         int
	}{{"success", "succeeded", 1}, {"decline", "failed", 1}, {"transient", "succeeded", 3}, {"unavailable", "failed", 3}} {
		t.Run(tc.scenario, func(t *testing.T) {
			s := setup(t)
			p := create(t, s, tc.scenario)
			now := time.Now()
			for i := 0; i < 5; i++ {
				if _, e := s.ProcessOne(ctx, now.Add(time.Duration(i)*10*time.Second)); e != nil {
					t.Fatal(e)
				}
			}
			got, e := s.Get(ctx, "a", p.ID)
			if e != nil || got.Status != tc.status || got.Attempts != tc.attempts {
				t.Fatal(got, e)
			}
		})
	}
}
func TestRetryNotEarly(t *testing.T) {
	s := setup(t)
	create(t, s, "transient")
	now := time.Now()
	s.ProcessOne(ctx, now)
	if ok, e := s.ProcessOne(ctx, now); e != nil || ok {
		t.Fatal(ok, e)
	}
}
func TestRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	p := create(t, s, "success")
	s.Close()
	s, e = Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.ProcessOne(ctx, time.Now()); e != nil {
		t.Fatal(e)
	}
	got, e := s.Get(ctx, "a", p.ID)
	if e != nil || got.Status != "succeeded" {
		t.Fatal(got, e)
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, fmt.Errorf("sink unavailable") }
func TestOutboxRetry(t *testing.T) {
	s := setup(t)
	create(t, s, "success")
	if _, e := s.DrainOne(ctx, brokenWriter{}); e == nil {
		t.Fatal("expected failure")
	}
	var b bytes.Buffer
	if ok, e := s.DrainOne(ctx, &b); e != nil || !ok || !strings.Contains(b.String(), `"event_id":1`) {
		t.Fatal(ok, e, b.String())
	}
	if ok, e := s.DrainOne(ctx, &b); ok || e != nil {
		t.Fatal(ok, e)
	}
}
func TestHTTP(t *testing.T) {
	s := setup(t)
	h := Handler(s, map[string]string{"secret": "a"})
	tests := []struct {
		body, key, auth string
		status          int
	}{{`{"amount_minor":100,"currency":"EUR"}`, "k", "Bearer secret", 202}, {`{"amount_minor":101,"currency":"EUR"}`, "k", "Bearer secret", 409}, {`{"amount_minor":100,"currency":"EUR","extra":1}`, "j", "Bearer secret", 400}, {`{} {}`, "j", "Bearer secret", 400}, {`{}`, "j", "", 401}, {`{"amount_minor":1.5,"currency":"EUR"}`, "j", "Bearer secret", 400}}
	for _, tc := range tests {
		r := httptest.NewRequest("POST", "/payments", strings.NewReader(tc.body))
		r.Header.Set("Authorization", tc.auth)
		r.Header.Set("Idempotency-Key", tc.key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatal(tc, w.Code, w.Body.String())
		}
	}
}
