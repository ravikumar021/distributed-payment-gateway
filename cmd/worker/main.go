package main

import (
	"context"
	"github.com/ravikumar021/distributed-payment-gateway/internal/payments"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	path := os.Getenv("PAYMENT_DB")
	if path == "" {
		path = "payments.db"
	}
	s, err := payments.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := s.ProcessOne(ctx, now); err != nil && ctx.Err() == nil {
				log.Printf("processing error: %v", err)
			}
			for i := 0; i < 20; i++ {
				ok, err := s.DrainOne(ctx, os.Stdout)
				if err != nil {
					if ctx.Err() == nil {
						log.Printf("outbox error: %v", err)
					}
					break
				}
				if !ok {
					break
				}
			}
		}
	}
}
