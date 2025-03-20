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

type errType struct {
	substring string
	shortName string
}

var errTypes = []errType{
	{
		substring: "connection refused",
		shortName: "refused",
	},
	{
		substring: "i/o timeout",
		shortName: "timeout",
	},
	{
		substring: "deadline exceeded",
		shortName: "deadline",
	},
	{
		substring: "unexpected eof",
		shortName: "eof",
	},
	{
		substring: "reset by peer",
		shortName: "reset",
	},
	{
		substring: "invalid connection",
		shortName: "invalid",
	},
	{
		substring: "slow ping",
		shortName: "slow",
	},
}

type errCounter struct {
	counters []atomic.Int32
	sb       strings.Builder
}

func newErrCounter() *errCounter {
	return &errCounter{
		counters: make([]atomic.Int32, len(errTypes)),
	}
}

func (c *errCounter) addError(err error) {
	if err == nil {
		return
	}
	errMsg := strings.ToLower(err.Error())
	for i, et := range errTypes {
		if strings.Contains(errMsg, et.substring) {
			c.counters[i].Add(1)
			return
		}
	}
	fmt.Println(time.Now().Format("15:04:05"), errMsg)
}

func (c *errCounter) summary() {
	c.sb.Reset()
	c.sb.WriteString(time.Now().Format("15:04:05"))
	shouldLog := false
	for i := range c.counters {
		num := c.counters[i].Swap(0)
		if num > 0 {
			c.sb.WriteString(fmt.Sprintf(" %s %d", errTypes[i].shortName, num))
			shouldLog = true
		}
	}
	if shouldLog {
		fmt.Println(c.sb.String())
	}
}

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
	counter := newErrCounter()
	// print error num by second
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			counter.summary()
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
				if err = db.Ping(); err == nil {
					if time.Since(startTime) > slow {
						err = errors.New("slow ping")
					}
				}
				counter.addError(err)
				_ = db.Close()
				<-ticker.C
			}
		}()
	}
}
