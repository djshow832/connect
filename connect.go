package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func main() {
	path := os.Args[1]
	db, err := sql.Open("mysql", path)
	if err != nil {
		panic(errors.Wrap(err, "open db fails"))
	}
	defer db.Close()
	var wg sync.WaitGroup
	wg.Add(2)

	// long connnection
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
				err = conn.PingContext(context.Background())
				if err != nil {
					fmt.Println("long connection fail", time.Now(), err)
					<-ticker.C
					break
				}
				fmt.Println("long connection succeeds", time.Now())
				<-ticker.C
			}
			_ = conn.Close()
		}
	}()

	// short connection
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			err := db.Ping()
			if err != nil {
				fmt.Println("new connection fails", time.Now(), err)
				<-ticker.C
				continue
			}
			fmt.Println("new connection succeeds", time.Now())
			<-ticker.C
		}
	}()
	wg.Wait()
}
