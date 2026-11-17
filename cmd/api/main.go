package main

import (
	"encoding/json"
	"github.com/ravikumar021/distributed-payment-gateway/internal/payments"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	keys := map[string]string{}
	if err := json.Unmarshal([]byte(os.Getenv("PAYMENT_API_KEYS")), &keys); err != nil || len(keys) == 0 {
		log.Fatal("set PAYMENT_API_KEYS to a JSON object mapping secret tokens to merchant IDs")
	}
	for k, v := range keys {
		if len(k) < 16 || v == "" {
			log.Fatal("tokens must contain at least 16 characters and merchants must be nonempty")
		}
	}
	path := os.Getenv("PAYMENT_DB")
	if path == "" {
		path = "payments.db"
	}
	s, err := payments.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	addr := os.Getenv("PAYMENT_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	server := http.Server{Addr: addr, Handler: payments.Handler(s, keys), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	log.Printf("payment simulator listening on %s", addr)
	log.Fatal(server.ListenAndServe())
}
