package payments

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	_ "modernc.org/sqlite"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

var ErrConflict = errors.New("idempotency key reused with different payload")
var ErrInvalid = errors.New("invalid payment request")
var ErrNotFound = errors.New("payment not found")

type Request struct {
	Amount   int64  `json:"amount_minor"`
	Currency string `json:"currency"`
	Scenario string `json:"scenario"`
}
type Payment struct {
	ID       string `json:"id"`
	Amount   int64  `json:"amount_minor"`
	Currency string `json:"currency"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
}
type Store struct{ DB *sql.DB }

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	db, err := sql.Open("sqlite", u.String()+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS payments (
 id TEXT PRIMARY KEY,merchant TEXT NOT NULL,idem TEXT NOT NULL,fingerprint TEXT NOT NULL,
 amount INTEGER NOT NULL CHECK(amount>0),currency TEXT NOT NULL,scenario TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('pending','succeeded','failed')),
 attempts INTEGER NOT NULL DEFAULT 0,next_at INTEGER NOT NULL DEFAULT 0,
 UNIQUE(merchant,idem));
 CREATE TABLE IF NOT EXISTS outbox (
 id INTEGER PRIMARY KEY AUTOINCREMENT,payment_id TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,delivered INTEGER NOT NULL DEFAULT 0);
 CREATE INDEX IF NOT EXISTS pending_payments ON payments(status,next_at);
 CREATE INDEX IF NOT EXISTS pending_events ON outbox(delivered,id);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func normalize(r Request) (Request, error) {
	r.Currency = strings.ToUpper(strings.TrimSpace(r.Currency))
	if r.Scenario == "" {
		r.Scenario = "success"
	}
	if r.Amount <= 0 || r.Amount > 1000000000 {
		return r, ErrInvalid
	}
	switch r.Currency {
	case "INR", "EUR", "USD", "GBP", "CAD", "AUD", "NZD":
	default:
		return r, ErrInvalid
	}
	switch r.Scenario {
	case "success", "decline", "transient", "unavailable":
	default:
		return r, ErrInvalid
	}
	return r, nil
}
func scan(row interface{ Scan(...any) error }) (Payment, error) {
	var p Payment
	err := row.Scan(&p.ID, &p.Amount, &p.Currency, &p.Status, &p.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return p, err
}
func event(tx *sql.Tx, p Payment, kind string) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT INTO outbox(payment_id,kind,payload) VALUES(?,?,?)", p.ID, kind, string(b))
	return err
}
func (s *Store) Create(ctx context.Context, merchant, key string, r Request) (Payment, bool, error) {
	if strings.TrimSpace(merchant) == "" || strings.TrimSpace(key) == "" || len(key) > 128 {
		return Payment{}, false, ErrInvalid
	}
	r, err := normalize(r)
	if err != nil {
		return Payment{}, false, err
	}
	b, _ := json.Marshal(r)
	hash := sha256.Sum256(b)
	fingerprint := hex.EncodeToString(hash[:])
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Payment{}, false, err
	}
	defer tx.Rollback()
	var existing string
	err = tx.QueryRowContext(ctx, "SELECT fingerprint FROM payments WHERE merchant=? AND idem=?", merchant, key).Scan(&existing)
	if err == nil {
		if existing != fingerprint {
			return Payment{}, false, ErrConflict
		}
		pay, err := scan(tx.QueryRowContext(ctx, "SELECT id,amount,currency,status,attempts FROM payments WHERE merchant=? AND idem=?", merchant, key))
		return pay, true, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Payment{}, false, err
	}
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		return Payment{}, false, err
	}
	pay := Payment{ID: hex.EncodeToString(id), Amount: r.Amount, Currency: r.Currency, Status: "pending"}
	_, err = tx.ExecContext(ctx, "INSERT INTO payments(id,merchant,idem,fingerprint,amount,currency,scenario,status) VALUES(?,?,?,?,?,?,?,?)", pay.ID, merchant, key, fingerprint, r.Amount, r.Currency, r.Scenario, pay.Status)
	if err != nil {
		return Payment{}, false, err
	}
	if err = event(tx, pay, "payment.created"); err != nil {
		return Payment{}, false, err
	}
	return pay, false, tx.Commit()
}
func (s *Store) Get(ctx context.Context, merchant, id string) (Payment, error) {
	return scan(s.DB.QueryRowContext(ctx, "SELECT id,amount,currency,status,attempts FROM payments WHERE merchant=? AND id=?", merchant, id))
}

// ProcessOne simulates a provider inside an atomic transaction. It performs no real charge.
// Real network calls must not be added here: they require leases and provider idempotency.
func (s *Store) ProcessOne(ctx context.Context, now time.Time) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var p Payment
	var scenario string
	err = tx.QueryRowContext(ctx, "SELECT id,amount,currency,status,attempts,scenario FROM payments WHERE status='pending' AND next_at<=? ORDER BY next_at,id LIMIT 1", now.UnixMilli()).Scan(&p.ID, &p.Amount, &p.Currency, &p.Status, &p.Attempts, &scenario)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	p.Attempts++
	kind := "payment.succeeded"
	next := int64(0)
	switch {
	case scenario == "decline":
		p.Status = "failed"
		kind = "payment.failed"
	case scenario == "unavailable" || (scenario == "transient" && p.Attempts < 3):
		if p.Attempts >= 3 {
			p.Status = "failed"
			kind = "payment.failed"
		} else {
			kind = "payment.retry_scheduled"
			next = now.Add(time.Second * time.Duration(1<<uint(p.Attempts-1))).UnixMilli()
		}
	default:
		p.Status = "succeeded"
	}
	_, err = tx.ExecContext(ctx, "UPDATE payments SET status=?,attempts=?,next_at=? WHERE id=?", p.Status, p.Attempts, next, p.ID)
	if err != nil {
		return false, err
	}
	if err = event(tx, p, kind); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// DrainOne writes an outbox event before marking delivery. A crash between these
// actions permits redelivery; consumers must deduplicate the stable event_id.
func (s *Store) DrainOne(ctx context.Context, w io.Writer) (bool, error) {
	var id int64
	var kind, payload string
	err := s.DB.QueryRowContext(ctx, "SELECT id,kind,payload FROM outbox WHERE delivered=0 ORDER BY id LIMIT 1").Scan(&id, &kind, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	b, err := json.Marshal(struct {
		ID      int64           `json:"event_id"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payment"`
	}{id, kind, json.RawMessage(payload)})
	if err != nil {
		return false, err
	}
	if _, err = fmt.Fprintln(w, string(b)); err != nil {
		return false, err
	}
	_, err = s.DB.ExecContext(ctx, "UPDATE outbox SET delivered=1 WHERE id=?", id)
	return err == nil, err
}
