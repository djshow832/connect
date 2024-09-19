package main

import (
        "database/sql"
        "fmt"
        "os/exec"
        "time"

        _ "github.com/go-sql-driver/mysql"
)

func main() {
        path := "root:@tcp(10.2.12.200:6661)/test"
        db, err := sql.Open("mysql", path)
        if err != nil {
                panic(err)
        }
        for {
                start := time.Now()
                output, err1 := exec.Command("/usr/sbin/arp", "10.2.12.200").CombinedOutput()
                if err1 != nil {
                        fmt.Println(err1)
                }
                err = db.Ping()
                end := time.Now()
                var dig string
                if err != nil {
                        dig = string(output)
                }
                fmt.Println(end, end.Sub(start), err, dig)
                <-time.After(500 * time.Millisecond)
        }
}
