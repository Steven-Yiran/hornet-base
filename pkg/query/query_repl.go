package query

import (
	"context"
	"fmt"
	"io"
	"strings"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
)

// Query REPL.
func QueryRepl(d *db.Database) *repl.REPL {
	r := repl.NewRepl()
	r.AddCommand("join", func(payload string, replConfig *repl.REPLConfig) error {
		return HandleJoin(d, payload, replConfig.GetWriter())
	}, "Create a table. usage: create table <table>")
	return r
}

// Handle join.
func HandleJoin(d *db.Database, payload string, w io.Writer) (err error) {
	fields := strings.Fields(payload)
	numFields := len(fields)
	// Usage: join <table1> <key/val for table1> on <table2> <key/val for table2>
	if numFields != 6 || fields[3] != "on" || (fields[2] != "key" && fields[2] != "val") || (fields[5] != "key" && fields[5] != "val") {
		return fmt.Errorf("usage: join <table1> <key/val for table1> on <table2> <key/val for table2>")
	}
	table1Name := fields[1]
	table1, err := d.GetTable(table1Name)
	if err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	table2Name := fields[4]
	table2, err := d.GetTable(table2Name)
	if err != nil {
		return fmt.Errorf("find error: %v", err)
	}
	joinOnLeftKey := fields[2] == "key"
	joinOnRightKey := fields[5] == "key"
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	resultsChan, _, group, cleanupCallback, err := Join(ctx, table1, table2, joinOnLeftKey, joinOnRightKey)
	if cleanupCallback != nil {
		defer cleanupCallback()
	}
	if err != nil {
		return err
	}
	done := make(chan bool)
	go func() {
		for {
			pair, valid := <-resultsChan
			if !valid {
				break
			}
			io.WriteString(w, fmt.Sprintf("{(%v, %v), (%v, %v)}\n",
				pair.l.GetKey(), pair.l.GetValue(), pair.r.GetKey(), pair.r.GetValue()))
		}
		done <- true
	}()
	err = group.Wait()
	close(resultsChan)
	<-done
	if err != nil {
		return fmt.Errorf("join error: %v", err)
	}
	return nil
}
