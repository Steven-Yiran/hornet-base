package concurrency

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	query "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/query"
	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"

	uuid "github.com/google/uuid"
)

// Transaction REPL.
func TransactionREPL(d *db.Database, tm *TransactionManager) *repl.REPL {
	r := repl.NewRepl()
	r.AddCommand("create", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleCreateTable(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Create a table. usage: create table <table>")
	r.AddCommand("find", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleFind(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Find an element. usage: find <key> from <table>")
	r.AddCommand("insert", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleInsert(d, tm, payload, replConfig.GetAddr())
	}, "Insert an element. usage: insert <key> <value> into <table>")
	r.AddCommand("update", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleUpdate(d, tm, payload, replConfig.GetAddr())
	}, "Update en element. usage: update <table> <key> <value>")
	r.AddCommand("delete", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleDelete(d, tm, payload, replConfig.GetAddr())
	}, "Delete an element. usage: delete <key> from <table>")
	r.AddCommand("select", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleSelect(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Select elements from a table. usage: select from <table>")
	r.AddCommand("join", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleJoin(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Joins two tables. usage: join <table1> <key/val for table1> on <table2> <key/val for table2>")
	r.AddCommand("transaction", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleTransaction(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Handle transactions. usage: transaction <begin|commit>")
	r.AddCommand("lock", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleLock(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Grabs a write lock on a resource. usage: lock <table> <key>")
	r.AddCommand("pretty", func(payload string, replConfig *repl.REPLConfig) error {
		return HandlePretty(d, payload, replConfig.GetWriter())
	}, "Print out the internal data representation. usage: pretty")
	return r
}

// Handle transaction.
func HandleTransaction(d *db.Database, tm *TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: create <type> table <table>
	if numFields != 2 || (fields[1] != "begin" && fields[1] != "commit") {
		return errors.New("usage: transaction <begin|commit>")
	}
	switch fields[1] {
	case "begin":
		return tm.Begin(clientId)
	case "commit":
		return tm.Commit(clientId)
	default:
		return errors.New("internal error in create table handler")
	}
}

// Handle create table.
func HandleCreateTable(d *db.Database, tm *TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	return db.HandleCreateTable(d, payload, w)
}

// Handle find.
func HandleFind(d *db.Database, tm *TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: find <key> from <table>
	var key int
	var table db.Index
	if numFields != 4 || fields[2] != "from" {
		return fmt.Errorf("usage: find <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	if table, err = d.GetTable(fields[3]); err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), R_LOCK); err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	if err = db.HandleFind(d, payload, w); err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	return nil
}

// Handle inserts.
func HandleInsert(d *db.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: insert <key> <value> into <table>
	var key int
	var table db.Index
	if numFields != 5 || fields[3] != "into" {
		return fmt.Errorf("usage: insert <key> <value> into <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if table, err = d.GetTable(fields[4]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if err = db.HandleInsert(d, payload); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	return nil
}

// Handle update.
func HandleUpdate(d *db.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: update <table> <key> <value>
	var key int
	var table db.Index
	if numFields != 4 {
		return fmt.Errorf("usage: update <table> <key> <value>")
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if table, err = d.GetTable(fields[1]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if err = db.HandleUpdate(d, payload); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	return nil
}

// Handle delete.
func HandleDelete(d *db.Database, tm *TransactionManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: delete <key> from <table>
	var key int
	var table db.Index
	if numFields != 4 || fields[2] != "from" {
		return fmt.Errorf("usage: delete <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	if table, err = d.GetTable(fields[3]); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	// Get the transaction, run the find, release lock and rollback if error.
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	if err = db.HandleDelete(d, payload); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	return nil
}

// Handle select.
func HandleSelect(d *db.Database, tm *TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: select from <table>
	if numFields != 3 || fields[1] != "from" {
		return fmt.Errorf("usage: select from <table>")
	}
	// NOTE: Select is unsafe; not locking anything. May provide an inconsistent view of the database.
	if err = db.HandleSelect(d, payload, w); err != nil {
		return fmt.Errorf("select error: %v", err)
	}
	return nil
}

// Handle join.
func HandleJoin(d *db.Database, tm *TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: join <table1> <key/val for table1> on <table2> <key/val for table2>
	if numFields != 6 || fields[3] != "on" || (fields[2] != "key" && fields[2] != "val") || (fields[5] != "key" && fields[5] != "val") {
		return fmt.Errorf("usage: join <table1> <key/val for table1> on <table2> <key/val for table2>")
	}
	// NOTE: Join is unsafe; not locking anything. May provide an inconsistent view of the database.
	err = query.HandleJoin(d, payload, w)
	return err
}

// Handle write lock requests.
func HandleLock(d *db.Database, tm *TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: lock <table> <key>
	var key int
	var table db.Index
	if numFields != 3 {
		return fmt.Errorf("usage: lock <table> <key>")
	}
	if table, err = d.GetTable(fields[1]); err != nil {
		return fmt.Errorf("lock error: %v", err)
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("lock error: %v", err)
	}
	if err = tm.Lock(clientId, table, int64(key), W_LOCK); err != nil {
		return fmt.Errorf("lock error: %v", err)
	}
	return nil
}

// Handle pretty printing.
func HandlePretty(d *db.Database, payload string, w io.Writer) (err error) {
	return db.HandlePretty(d, payload, w)
}
