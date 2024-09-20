package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func main() {
	path := os.Args[1]
	conns, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("mysql", path)
	if err != nil {
		panic(errors.Wrap(err, "open db fails"))
	}
	defer db.Close()
	var wg sync.WaitGroup
	wg.Add(int(conns) + 1)

	// long connnection
	for i := 0; i < int(conns); i++ {
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

	// short connection
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
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
				if duration > 200*time.Millisecond {
					fmt.Println("short connection too slow", time.Now(), duration)
				}
			}
			_ = db.Close()
			<-ticker.C
		}
	}()
	wg.Wait()
}
