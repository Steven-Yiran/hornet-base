package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/concurrency"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tl := tableLog{
		tblType: tblType,
		tblName: tblName,
	}
	rm.writeToBuffer(tl.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	el := editLog{
		id:        clientId,
		tablename: table.GetName(),
		action:    action,
		key:       key,
		oldval:    oldval,
		newval:    newval,
	}
	rm.writeToBuffer(el.toString())
	rm.txStack[clientId] = append(rm.txStack[clientId], &el)
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	sl := startLog{
		id: clientId,
	}
	rm.writeToBuffer(sl.toString())
	rm.txStack[clientId] = make([]Log, 1)
	rm.txStack[clientId] = append(rm.txStack[clientId], &sl)
}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	cl := commitLog{
		id: clientId,
	}
	rm.writeToBuffer(cl.toString())
	delete(rm.txStack, clientId)
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// flush all pages to disk
	tables := rm.d.GetTables()
	for _, table := range tables {
		table.GetPager().LockAllUpdates()
		table.GetPager().FlushAllPages()
		table.GetPager().UnlockAllUpdates()
	}
	// get keys of txStack
	keys := make([]uuid.UUID, 0)
	for k := range rm.txStack {
		keys = append(keys, k)
	}
	cl := checkpointLog{
		ids: keys,
	}
	rm.writeToBuffer(cl.toString())
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Do a full recovery to the most recent checkpoint on startup.
// The recovery algorithm is as follows:
// 1. Seek backwards through the log to the most recent checkpoint, keep track of active transactions.
// 2. Redo all actions from the most recent checkpoint to the end of the log, keep track of active transactions.
// 3. Undo all actions that belongs to active transactions.
// 4. Commit the active transactions.
func (rm *RecoveryManager) Recover() error {
	// read in logs
	logs, checkpointPos, err := rm.readLogs()
	if err != nil {
		return err
	}
	// find the most recent checkpoint
	activeTxs := make(map[uuid.UUID]bool)
	// check if the log at checkpointPos is a checkpoint
	if _, ok := logs[checkpointPos].(*checkpointLog); ok {
		// store all active transactions to activeTxs
		for _, id := range logs[checkpointPos].(*checkpointLog).ids {
			rm.tm.Begin(id)
			activeTxs[id] = true
		}
	}
	// commitTxs := make(map[uuid.UUID]bool)
	// for i := len(logs) - 1; i >= checkpointPos; i-- {
	// 	// check if log is a commit log
	// 	comLog, ok := logs[i].(*commitLog)
	// 	if ok {
	// 		commitTxs[comLog.id] = true
	// 	}
	// 	// check if log is a start log
	// 	startLog, ok := logs[i].(*startLog)
	// 	if ok {
	// 		if _, ok := commitTxs[startLog.id]; !ok {
	// 			rm.tm.Begin(startLog.id)
	// 			activeTxs[startLog.id] = true
	// 		}
	// 	}
	// }
	// redo part
	for i := checkpointPos; i < len(logs); i++ {
		//rm.Redo(logs[i])
		switch log := logs[i].(type) {
		case *tableLog:
			rm.Redo(log)
		case *editLog:
			rm.Redo(log)
		case *startLog:
			rm.tm.Begin(log.id)
			activeTxs[log.id] = true
		case *commitLog:
			delete(activeTxs, log.id)
			rm.Commit(log.id)
			rm.tm.Commit(log.id)
		}
	}
	// undo part
	for i := len(logs) - 1; i >= 0; i-- {
		switch log := logs[i].(type) {
		case *editLog:
			// check if log belongs to an active transaction
			if _, ok := activeTxs[log.id]; ok {
				rm.Undo(logs[i])
			}
		case *startLog:
			// check if log belongs to an active transaction
			if _, ok := activeTxs[log.id]; ok {
				rm.Commit(log.id)
				rm.tm.Commit(log.id)
			}
		}
	}
	return nil
}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	logs, found := rm.txStack[clientId]
	if !found {
		return errors.New("transaction not found")
	}
	// Check if the first entry of the log is a start log
	for i := len(logs) - 1; i >= 1; i-- {
		// check is log is an edit log
		_, ok := logs[i].(*editLog)
		if !ok {
			continue
		}
		err := rm.Undo(logs[i])
		if err != nil {
			return err
		}
	}
	// Commit to both the RecoveryManager and TransactionManager when Rollback ends so that both the logs and system know that this transaction has ended
	rm.Commit(clientId)
	rm.tm.Commit(clientId)
	return nil
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
