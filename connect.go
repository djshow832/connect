package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func main() {
	host := flag.String("host", "127.0.0.1", "mysql host")
	port := flag.Int("port", 4000, "mysql port")
	user := flag.String("user", "root", "mysql user")
	password := flag.String("password", "12345678", "mysql password")
	conns := flag.Int("conns", 100, "number of long connections")
	intervalMs := flag.Int("interval", 1000, "interval of short connections (ms)")
	slowThreshold := flag.Int("slow", 100, "slow threshold (ms)")
	concurrency := flag.Int("concurrency", 1, "goroutines to create short connections")

	help := flag.Bool("help", false, "show the usage")
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	path := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", *user, *password, *host, *port)
	db, err := sql.Open("mysql", path)
	if err != nil {
		panic(errors.Wrap(err, "open db fails"))
	}
	defer db.Close()

	var wg sync.WaitGroup
	// long connnection
	runLongConn(&wg, db, *conns)
	// short connection
	interval := time.Duration(*intervalMs) * time.Millisecond
	slow := time.Duration(*slowThreshold) * time.Millisecond
	runShortConn(&wg, path, interval, slow, *concurrency)
	wg.Wait()
}

func runLongConn(wg *sync.WaitGroup, db *sql.DB, conns int) {
	wg.Add(conns)
	for i := 0; i < conns; i++ {
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				conn, err := db.Conn(context.Background())
				if err != nil {
					fmt.Println("create connection fails", time.Now(), err)
					<-ticker.C
					continue
				}

				for {
					startTime := time.Now()
					err = conn.PingContext(context.Background())
					if err != nil {
						fmt.Println("long connection fail", time.Now(), err)
						<-ticker.C
						break
					}
					duration := time.Since(startTime)
					if duration > 100*time.Millisecond {
						fmt.Println("long connection too slow", time.Now(), duration)
					}
					<-ticker.C
				}
				_ = conn.Close()
			}
		}()
	}
}

func runShortConn(wg *sync.WaitGroup, path string, interval, slow time.Duration, concurrency int) {
	wg.Add(concurrency + 1)
	var refused, timeout, deadline atomic.Int32
	// print error num by second
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var sb strings.Builder
		for {
			shouldLog := false
			sb.WriteString(time.Now().Format("15:04:05"))
			refusedNum := refused.Swap(0)
			timeoutNum := timeout.Swap(0)
			deadlineNum := deadline.Swap(0)
			if refusedNum > 0 {
				sb.WriteString(fmt.Sprintf(" refused %d", refusedNum))
				shouldLog = true
			}
			if timeoutNum > 0 {
				sb.WriteString(fmt.Sprintf(" timeout %d", timeoutNum))
				shouldLog = true
			}
			if deadlineNum > 0 {
				sb.WriteString(fmt.Sprintf(" deadline %d", deadlineNum))
				shouldLog = true
			}
			if shouldLog {
				fmt.Println(sb.String())
				sb.Reset()
			}
			<-ticker.C
		}
	}()
	// connect and increase error num
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				db, err := sql.Open("mysql", path)
				if err != nil {
					panic(errors.Wrap(err, "open db fails"))
				}
				startTime := time.Now()
				ctx, cancel := context.WithDeadline(context.Background(), startTime.Add(slow))
				err = db.PingContext(ctx)
				cancel()
				if err != nil {
					errMsg := strings.ToLower(err.Error())
					switch {
					case strings.Contains(errMsg, "connection refused"):
						refused.Add(1)
					case strings.Contains(errMsg, "i/o timeout"):
						timeout.Add(1)
					case strings.Contains(errMsg, "deadline exceeded"):
						deadline.Add(1)
					default:
						fmt.Println("short connection fail", time.Now(), err)
					}
				}
				_ = db.Close()
				<-ticker.C
			}
		}()
	}
}
