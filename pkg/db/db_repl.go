package db

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// Creates a DB Repl for the given index.
func DatabaseRepl(db *Database) *repl.REPL {
	r := repl.NewRepl()
	r.AddCommand("create", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleCreateTable(db, payload, replConfig.GetWriter())
	}, "Create a table. usage: create table <table>")
	r.AddCommand("find", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleFind(db, payload, replConfig.GetWriter())
	}, "Find an element. usage: find <key> from <table>")
	r.AddCommand("insert", func(payload string, replConfig *repl.REPLConfig) error { return HandleInsert(db, payload) }, "Insert an element. usage: insert <key> <value> into <table>")
	r.AddCommand("update", func(payload string, replConfig *repl.REPLConfig) error { return HandleUpdate(db, payload) }, "Update en element. usage: update <table> <key> <value>")
	r.AddCommand("delete", func(payload string, replConfig *repl.REPLConfig) error { return HandleDelete(db, payload) }, "Delete an element. usage: delete <key> from <table>")
	r.AddCommand("select", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleSelect(db, payload, replConfig.GetWriter())
	}, "Select elements from a table. usage: select from <table>")
	r.AddCommand("pretty", func(payload string, replConfig *repl.REPLConfig) error {
		return HandlePretty(db, payload, replConfig.GetWriter())
	}, "Print out the internal data representation. usage: pretty")
	return r
}

// Handle create table.
func HandleCreateTable(d *Database, payload string, w io.Writer) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: create <type> table <table>
	if numFields != 4 || fields[2] != "table" || (fields[1] != "btree" && fields[1] != "hash") {
		return fmt.Errorf("usage: create <btree|hash> table <table>")
	}
	var tableType IndexType
	switch fields[1] {
	case "btree":
		tableType = BTreeIndexType
	case "hash":
		tableType = HashIndexType
	default:
		return errors.New("create error: internal error")
	}
	tableName := fields[3]
	_, err = d.createTable(tableName, tableType)
	if err != nil {
		return err
	}
	io.WriteString(w, fmt.Sprintf("%s table %s created.\n", fields[1], tableName))
	return nil
}

// Handle find.
func HandleFind(d *Database, payload string, w io.Writer) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: find <key> from <table>
	var key int
	if numFields != 4 || fields[2] != "from" {
		return fmt.Errorf("usage: find <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	tableName := fields[3]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	entry, err := table.Find(int64(key))
	if err != nil || entry == nil {
		return fmt.Errorf("find error: %v", err)
	}
	io.WriteString(w, fmt.Sprintf("found entry: (%d, %d)\n",
		entry.GetKey(), entry.GetValue()))
	return nil
}

// Handle insert.
func HandleInsert(d *Database, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: insert <key> <value> into <table>
	var key, value int
	if numFields != 5 || fields[3] != "into" {
		return fmt.Errorf("usage: insert <key> <value> into <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	if value, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	tableName := fields[4]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	val, _ := table.Find(int64(key))
	if val != nil {
		return fmt.Errorf("insert error: key already in table")
	}
	err = table.Insert(int64(key), int64(value))
	if err != nil {
		return fmt.Errorf("insert error: %v", err)
	}
	return nil
}

// Handle update.
func HandleUpdate(d *Database, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: update <table> <key> <value>
	var key, value int
	if numFields != 4 {
		return fmt.Errorf("usage: update <table> <key> <value>")
	}
	if key, err = strconv.Atoi(fields[2]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	if value, err = strconv.Atoi(fields[3]); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	tableName := fields[1]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	err = table.Update(int64(key), int64(value))
	if err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	return nil
}

// Handle delete.
func HandleDelete(d *Database, payload string) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: delete <key> from <table>
	var key int
	if numFields != 4 || fields[2] != "from" {
		return fmt.Errorf("usage: delete <key> from <table>")
	}
	if key, err = strconv.Atoi(fields[1]); err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	tableName := fields[3]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	err = table.Delete(int64(key))
	if err != nil {
		return fmt.Errorf("delete error: %v", err)
	}
	return nil
}

// Handle select.
func HandleSelect(d *Database, payload string, w io.Writer) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: select from <table>
	if numFields != 3 || fields[1] != "from" {
		return fmt.Errorf("usage: select from <table>")
	}
	tableName := fields[2]
	table, err := d.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("select error: %v", err)
	}
	var results []utils.Entry
	if results, err = table.Select(); err != nil {
		return err
	}
	printResults(results, w)
	return nil
}

// Handle pretty printing.
func HandlePretty(d *Database, payload string, w io.Writer) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: pretty <optional pagenumber> from <table>
	if numFields == 3 && fields[1] == "from" {
		tableName := fields[2]
		table, err := d.GetTable(tableName)
		if err != nil {
			return fmt.Errorf("pretty error: %v", err)
		}
		table.Print(w)
	} else if numFields == 4 && fields[2] == "from" {
		var pn int
		if pn, err = strconv.Atoi(fields[1]); err != nil {
			return fmt.Errorf("pretty error: %v", err)
		}
		tableName := fields[3]
		table, err := d.GetTable(tableName)
		if err != nil {
			return fmt.Errorf("pretty error: %v", err)
		}
		table.PrintPN(pn, w)
	} else {
		return fmt.Errorf("usage: pretty <optional pagenumber> from <table>")
	}
	return nil
}

// printResults prints all given entries in a standard format.
func printResults(entries []utils.Entry, w io.Writer) {
	for _, entry := range entries {
		io.WriteString(w, fmt.Sprintf("(%v, %v)\n",
			entry.GetKey(), entry.GetValue()))
	}
}
