package recovery

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	concurrency "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/concurrency"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	query "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/query"
	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"

	uuid "github.com/google/uuid"
)

// Recovery REPL.
func RecoveryREPL(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager) *repl.REPL {
	r := repl.NewRepl()
	r.AddCommand("create", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleCreateTable(d, tm, rm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Create a table. usage: create <btree|hash> table <table>")
	r.AddCommand("find", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleFind(d, tm, rm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Find an element. usage: find <key> from <table>")
	r.AddCommand("insert", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleInsert(d, tm, rm, payload, replConfig.GetAddr())
	}, "Insert an element. usage: insert <key> <value> into <table>")
	r.AddCommand("update", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleUpdate(d, tm, rm, payload, replConfig.GetAddr())
	}, "Update en element. usage: update <table> <key> <value>")
	r.AddCommand("delete", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleDelete(d, tm, rm, payload, replConfig.GetAddr())
	}, "Delete an element. usage: delete <key> from <table>")
	r.AddCommand("select", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleSelect(d, tm, rm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Select elements from a table. usage: select from <table>")
	r.AddCommand("join", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleJoin(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Joins two tables together on either their keys or values. usage: join <table1> <key/val for table1> on <table2> <key/val for table2>")
	r.AddCommand("transaction", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleTransaction(d, tm, rm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Handle transactions. usage: transaction <begin|commit>")
	r.AddCommand("lock", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleLock(d, tm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Grabs a write lock on a resource. usage: lock <table> <key>")
	r.AddCommand("checkpoint", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleCheckpoint(d, tm, rm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Saves a checkpoint of the current database state and running transactions. usage: checkpoint")
	r.AddCommand("abort", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleAbort(d, tm, rm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Simulate an abort of the current transaction. usage: abort")
	r.AddCommand("crash", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleCrash(d, tm, rm, payload, replConfig.GetWriter(), replConfig.GetAddr())
	}, "Crash the database. usage: crash")
	r.AddCommand("pretty", func(payload string, replConfig *repl.REPLConfig) error {
		return HandlePretty(d, payload, replConfig.GetWriter())
	}, "Print out the internal data representation. usage: pretty")
	return r
}

// Handle transaction.
func HandleTransaction(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: transaction <begin|commit>
	if numFields != 2 || (fields[1] != "begin" && fields[1] != "commit") {
		return errors.New("usage: transaction <begin|commit>")
	}
	switch fields[1] {
	case "begin":
		rm.Start(clientId)
		err = tm.Begin(clientId)
	case "commit":
		rm.Commit(clientId)
		err = tm.Commit(clientId)
	default:
		return errors.New("internal error in create table handler")
	}
	if err != nil {
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle create table.
func HandleCreateTable(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: create <type> table <table>
	if numFields != 4 || fields[2] != "table" || (fields[1] != "btree" && fields[1] != "hash") {
		return fmt.Errorf("usage: create <btree|hash> table <table>")
	}
	rm.Table(fields[1], fields[3])
	return db.HandleCreateTable(d, payload, w)
}

// Handle find.
func HandleFind(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	return concurrency.HandleFind(d, tm, payload, w, clientId)
}

// Handle insert.
func HandleInsert(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: insert <key> <value> into <table>
	var key, newval int
	var table db.Index
	if numFields != 5 || fields[3] != "into" {
		return fmt.Errorf("usage: insert <key> <value> into <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if newval, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if table, err = d.GetTable(fields[4]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	// First, check that the desired value doesn't exist.
	_, err = table.Find(int64(key))
	if err == nil {
		return errors.New("insert error: key already exists")
	}
	// Log.
	rm.Edit(clientId, table, INSERT_ACTION, int64(key), 0, int64(newval))
	// Run transaction insert.
	err = concurrency.HandleInsert(d, tm, payload, clientId)
	if err != nil {
		// Add a log to mark this insert as a no-op.
		rm.Edit(clientId, table, DELETE_ACTION, int64(key), int64(newval), int64(0))
		// Then pop the last two actions from the transaction stack because
		// these last two actions were no-ops.
		stack := rm.txStack[clientId]
		rm.txStack[clientId] = stack[:len(stack)-2]
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle update.
func HandleUpdate(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: update <table> <key> <value>
	var key, newval int
	var table db.Index
	if numFields != 4 {
		return fmt.Errorf("usage: update <table> <key> <value>")
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if newval, err = strconv.Atoi(fields[3]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if table, err = d.GetTable(fields[1]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	// First, check that the desired value exists.
	oldval, err := table.Find(int64(key))
	if err != nil {
		return errors.New("update error: key doesn't exists")
	}
	// Log.
	rm.Edit(clientId, table, UPDATE_ACTION, int64(key), oldval.GetValue(), int64(newval))
	// Run transaction insert.
	err = concurrency.HandleUpdate(d, tm, payload, clientId)
	if err != nil {
		// Add a log to mark this update as a no-op.
		rm.Edit(clientId, table, UPDATE_ACTION, int64(key), int64(newval), oldval.GetValue())
		// Then pop the last two actions from the transaction stack because
		// these last two actions were no-ops.
		stack := rm.txStack[clientId]
		rm.txStack[clientId] = stack[:len(stack)-2]
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle delete.
func HandleDelete(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, clientId uuid.UUID) (err error) {
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
	// First, check that the desired value exists.
	oldval, err := table.Find(int64(key))
	if err != nil {
		return errors.New("delete error: key doesn't exists")
	}
	// Log.
	rm.Edit(clientId, table, DELETE_ACTION, int64(key), oldval.GetValue(), 0)
	// Run transaction insert.
	err = concurrency.HandleDelete(d, tm, payload, clientId)
	if err != nil {
		// Add a log to mark this delete as a no-op.
		rm.Edit(clientId, table, INSERT_ACTION, int64(key), 0, oldval.GetValue())
		// Then pop the last two actions from the transaction stack because
		// these last two actions were no-ops.
		stack := rm.txStack[clientId]
		rm.txStack[clientId] = stack[:len(stack)-2]
		rberr := rm.Rollback(clientId)
		if rberr != nil {
			return rberr
		}
	}
	return err
}

// Handle select.
func HandleSelect(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: select from <table>
	if numFields != 3 || fields[1] != "from" {
		return fmt.Errorf("usage: select from <table>")
	}
	// NOTE: Select is unsafe; not locking anything. May provide an inconsistent view of the database.
	err = db.HandleSelect(d, payload, w)
	return err
}

// Handle join.
func HandleJoin(d *db.Database, tm *concurrency.TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
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
func HandleLock(d *db.Database, tm *concurrency.TransactionManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	return concurrency.HandleLock(d, tm, payload, w, clientId)
}

// Handle checkpoint.
func HandleCheckpoint(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: checkpoint
	if numFields != 1 {
		return fmt.Errorf("usage: checkpoint")
	}
	// Get the transaction, run the find, release lock and rollback if error.
	rm.Checkpoint()
	return err
}

// Handle abort.
func HandleAbort(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: abort
	if numFields != 1 {
		return fmt.Errorf("usage: abort")
	}
	// Get the transaction, run the find, release lock and rollback if error.
	_, found := tm.GetTransaction(clientId)
	if !found {
		return errors.New("no running transaction to abort")
	}
	err = rm.Rollback(clientId)
	return err
}

// Handle crash.
func HandleCrash(d *db.Database, tm *concurrency.TransactionManager, rm *RecoveryManager, payload string, w io.Writer, clientId uuid.UUID) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: crash
	if numFields != 1 {
		return fmt.Errorf("usage: crash")
	}
	panic("it's the end of the world!")
}

// Handle pretty printing.
func HandlePretty(d *db.Database, payload string, w io.Writer) (err error) {
	return db.HandlePretty(d, payload, w)
}
