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
	conns := flag.Int("conns", 100, "number of long connections")
	intervalMs := flag.Int("interval", 1000, "interval of short connections (ms)")
	slowThreshold := flag.Int("slow", 100, "slow threshold (ms)")

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
		var errNum, slowNum int
		var lastSummaryTime time.Time
		for {
			db, err := sql.Open("mysql", path)
			if err != nil {
				panic(errors.Wrap(err, "open db fails"))
			}
			startTime := time.Now()
			err = db.Ping()
			if err != nil {
				errNum++
			} else {
				duration := time.Since(startTime)
				if duration > slow {
					slowNum++
				}
			}
			curTime := time.Now()
			if curTime.Sub(lastSummaryTime) > time.Second {
				if errNum > 0 || slowNum > 0 {
					fmt.Println(curTime.Format("15:04:05"), "err", errNum, "slow", slowNum)
					errNum, slowNum = 0, 0
				}
				lastSummaryTime = curTime
			}
			_ = db.Close()
			<-ticker.C
		}
	}()
}
