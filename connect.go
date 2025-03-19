package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func main() {
	host := flag.String("host", "127.0.0.1", "mysql host")
	port := flag.Int("port", 4000, "mysql port")
	user := flag.String("user", "root", "mysql user")
	password := flag.String("password", "12345678", "mysql password")
	path := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", *user, *password, *host, *port)
	conns := flag.Int("conns", 100, "number of long connections")
	intervalMs := flag.Int("interval", 1000, "interval of short connections (ms)")
	interval := time.Duration(*intervalMs) * time.Millisecond
	slowThreshold := flag.Int("slow", 100, "slow threshold (ms)")
	slow := time.Duration(*slowThreshold) * time.Millisecond

	help := flag.Bool("help", false, "show the usage")
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	db, err := sql.Open("mysql", path)
	if err != nil {
		panic(errors.Wrap(err, "open db fails"))
	}
	defer db.Close()

	var wg sync.WaitGroup
	// long connnection
	runLongConn(&wg, db, *conns)
	// short connection
	runShortConn(&wg, path, interval, slow)
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

func runShortConn(wg *sync.WaitGroup, path string, interval, slow time.Duration) {
	wg.Add(1)
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
			err = db.Ping()
			if err != nil {
				fmt.Println("short connection fails", time.Now(), err)
			} else {
				duration := time.Since(startTime)
				if duration > slow {
					fmt.Println("short connection too slow", time.Now(), duration)
				}
			}
			_ = db.Close()
			<-ticker.C
		}
	}()
}
