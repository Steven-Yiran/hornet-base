package main

import (
	"flag"
	"fmt"

	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	config "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/config"
	list "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"
	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"

	concurrency "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/concurrency"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	query "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/query"
	recovery "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/recovery"

	uuid "github.com/google/uuid"
)

// Default port 8335 (BEES).
const DEFAULT_PORT int = 8335

const LOG_FILE_NAME = "data/bumble.log"

// [BTREE]
// Listens for SIGINT or SIGTERM and calls table.CloseDB().
func setupCloseHandler(database *db.Database) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("closehandler invoked")
		database.Close()
		os.Exit(0)
	}()
}

// [CONCURRENCY]
// Start listening for connections at port `port`.
func startServer(repl *repl.REPL, tm *concurrency.TransactionManager, prompt string, port int) {
	// Handle a connection by running the repl on it.
	handleConn := func(c net.Conn) {
		clientId := uuid.New()
		defer c.Close()
		if tm != nil {
			defer tm.Commit(clientId)
		}
		repl.Run(c, clientId, prompt)
	}
	// Start listening for new connections.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Fatal(err)
	}
	dbName := config.DBName
	fmt.Printf("%v server started listening on localhost:%v\n", dbName,
		listener.Addr().(*net.TCPAddr).Port)
	// Handle each connection.
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go handleConn(conn)
	}
}

// Start the database.
func main() {
	// Set up flags.
	var promptFlag = flag.Bool("c", true, "use prompt?")
	var projectFlag = flag.String("project", "", "choose project: [go,pager,db,query,concurrency,recovery] (required)")

	// [BTREE]
	var dbFlag = flag.String("db", "data/", "DB folder")

	// [CONCURRENCY]
	var portFlag = flag.Int("p", DEFAULT_PORT, "port number")

	flag.Parse()

	// [BTREE]
	// Open the db.
	database, err := db.Open(*dbFlag)
	if err != nil {
		panic(err)
	}

	// [RECOVERY]
	// Set up the log file.
	err = database.CreateLogFile(LOG_FILE_NAME)
	if err != nil {
		panic(err)
	}

	// [BTREE]
	// Setup close conditions.
	defer database.Close()
	setupCloseHandler(database)

	// Set up REPL resources.
	prompt := config.GetPrompt(*promptFlag)
	repls := make([]*repl.REPL, 0)

	// [CONCURRENCY]
	var tm *concurrency.TransactionManager
	server := false

	// [RECOVERY]
	var rm *recovery.RecoveryManager

	// Get the right REPLs.
	switch *projectFlag {
	case "go":
		l := list.NewList()
		repls = append(repls, list.ListRepl(l))

	// [PAGER]
	case "pager":
		pRepl, err := pager.PagerRepl()
		if err != nil {
			fmt.Println(err)
			return
		}
		repls = append(repls, pRepl)

	// [BTREE]
	case "db":
		server = false
		repls = append(repls, db.DatabaseRepl(database))

	// [QUERY]
	case "query":
		server = false
		repls = append(repls, db.DatabaseRepl(database))
		repls = append(repls, query.QueryRepl(database))

	// [CONCURRENCY]
	case "concurrency":
		server = true
		lm := concurrency.NewLockManager()
		tm = concurrency.NewTransactionManager(lm)
		repls = append(repls, concurrency.TransactionREPL(database, tm))

	// [RECOVERY]
	case "recovery":
		server = true
		lm := concurrency.NewLockManager()
		tm = concurrency.NewTransactionManager(lm)
		rm, err = recovery.NewRecoveryManager(database, tm, LOG_FILE_NAME)
		if err != nil {
			fmt.Println(err)
			return
		}
		repls = append(repls, recovery.RecoveryREPL(database, tm, rm))
		// Recover in this case!
		rm.Recover()

	default:
		fmt.Println("must specify -project [go,pager,db,query,concurrency,recovery]")
		return
	}

	// Combine the REPLs.
	r, err := repl.CombineRepls(repls)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Start server if server (concurrency or recovery), else run REPL here.
	if server {
		// 	[CONCURRENCY]
		startServer(r, tm, prompt, *portFlag)
	} else {
		r.Run(nil, uuid.New(), prompt)
	}
}
