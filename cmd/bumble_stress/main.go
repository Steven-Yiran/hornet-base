package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	btree "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/btree"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"

	uuid "github.com/google/uuid"
)

var STARTUP = 100 * time.Millisecond
var MAX_DELAY int64 = 10

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

// Get delay jitter.
func jitter() time.Duration {
	return time.Duration(rand.Int63n(MAX_DELAY)+1) * time.Millisecond
}

// Parse workload
func parseWorkload(path string) ([]string, error) {
	// Open the file.
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// Scan through all lines.
	var workload []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		workload = append(workload, scanner.Text())
	}
	return workload, scanner.Err()
}

// Handle workload
func handleWorkload(c chan string, wg *sync.WaitGroup, workload []string, idx int, n int) {
	// Iterate!
	defer wg.Done()
	for i := idx; i < len(workload); i += n {
		time.Sleep(jitter())
		c <- workload[i]
	}
}

// Start the database.
func main() {
	// Set up flags.
	var indexFlag = flag.String("index", "", "choose index: [btree,hash] (required)")
	var workloadFlag = flag.String("workload", "", "workload file (required)")
	var nFlag = flag.Int("n", 1, "number of threads to run (default: 1)")
	var verifyFlag = flag.Bool("verify", false, "enable to verify database state at the end of the workload")
	flag.Parse()
	// Open the db.
	database, err := db.Open("data")
	if err != nil {
		panic(err)
	}
	// Set up the log file.
	os.Remove("./data/db.log")
	err = database.CreateLogFile("./data/db.log")
	if err != nil {
		panic(err)
	}
	// Setup close conditions.
	defer database.Close()
	setupCloseHandler(database)
	// Clean up old db resources.
	os.Remove("./data/t")
	os.Remove("./data/t.meta")
	// Run REPL.
	r := db.DatabaseRepl(database)
	c := make(chan string)
	go r.RunChan(c, uuid.New(), "")
	// Some time to wake up...
	time.Sleep(STARTUP)
	// Initialize the db.
	switch *indexFlag {
	case "btree":
		c <- "create btree table t"
	case "hash":
		c <- "create hash table t"
	default:
		fmt.Println("must specify -index [btree,hash]")
		return
	}
	// Parse and run workload.
	if *workloadFlag == "" {
		fmt.Println("no workload file given")
		return
	}
	workload, err := parseWorkload(*workloadFlag)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Some time to wake up...
	time.Sleep(STARTUP)
	var wg sync.WaitGroup
	for i := 0; i < *nFlag; i++ {
		wg.Add(1)
		go handleWorkload(c, &wg, workload, i, *nFlag)
	}
	wg.Wait()
	// Verify the structure of the index.
	if *verifyFlag {
		index, err := database.GetTable("t")
		if err != nil {
			fmt.Println("error getting table t")
			return
		}
		switch *indexFlag {
		case "btree":
			index := index.(*btree.BTreeIndex)
			btree.IsBTree(index)
		case "hash":
			index := index.(*hash.HashIndex)
			hash.IsHash(index)
		}
	}
}
