package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	config "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/config"
)

// Writes everything from src to dest.
func mustCopy(dst io.Writer, src io.Reader) {
	if _, err := io.Copy(dst, src); err != nil {
		log.Fatal(err)
	}
}

// Connect to the database server and send messages to it.
func main() {
	var port = flag.Int("p", 0, "port number")
	flag.Parse()
	dbName := config.DBName
	if *port == 0 {
		fmt.Println("usage: ./" + dbName + "_client -p <port>")
		return
	}
	conn, err := net.Dial("tcp", fmt.Sprintf(":%v", *port))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	go mustCopy(os.Stdout, conn)
	mustCopy(conn, os.Stdin)
}
