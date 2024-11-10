package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"golang.org/x/sync/semaphore"
)

type Config struct {
	DSN        string
	Query      string
	Duration   time.Duration
	Concurrent int
}

type Benchmark struct {
	db     *sql.DB
	config Config
}

func NewBenchmark(cfg Config) (*Benchmark, error) {
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &Benchmark{config: cfg, db: db}, nil
}

func (b *Benchmark) Run() {
	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(b.config.Concurrent))

	start := time.Now()
	var reqCount int64
	ctx, cancel := context.WithTimeout(context.Background(), b.config.Duration)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer sem.Release(1)

				if _, err := b.executeQuery(); err != nil {
					log.Printf("Execution error: %v\n", err)
					return
				}

				atomic.AddInt64(&reqCount, 1)
			}()
		}
		if ctx.Err() != nil {
			break
		}
	}

	wg.Wait()
	elapsed := time.Since(start)
	rps := float64(reqCount) / elapsed.Seconds()
	fmt.Printf("Total Requests: %d\n", reqCount)
	fmt.Printf("Duration: %s\n", elapsed)
	fmt.Printf("RPS: %.2f\n", rps)
}

func (b *Benchmark) executeQuery() (sql.Result, error) {
	return b.db.Exec(b.config.Query)
}

func main() {
	config := Config{
		DSN:        "postgres://user:password@localhost:5432/dbname?sslmode=disable",
		Query:      "SELECT pg_sleep(0.01)",
		Duration:   10 * time.Second,
		Concurrent: 100,
	}

	benchmark, err := NewBenchmark(config)
	if err != nil {
		log.Fatalf("Failed to create benchmark: %v", err)
	}

	benchmark.Run()
}
