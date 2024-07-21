package concurrency

import (
	"errors"
	"sync"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	uuid "github.com/google/uuid"
)

// Each client can have a transaction running. Each transaction has a list of locked resources.
type Transaction struct {
	clientId  uuid.UUID
	resources map[Resource]LockType
	lock      sync.RWMutex
}

// Grab a write lock on the tx
func (t *Transaction) WLock() {
	t.lock.Lock()
}

// Release the write lock on the tx
func (t *Transaction) WUnlock() {
	t.lock.Unlock()
}

// Grab a read lock on the tx
func (t *Transaction) RLock() {
	t.lock.RLock()
}

// Release the write lock on the tx
func (t *Transaction) RUnlock() {
	t.lock.RUnlock()
}

// Get the transaction id.
func (t *Transaction) GetClientID() uuid.UUID {
	return t.clientId
}

// Get the transaction's resources.
func (t *Transaction) GetResources() map[Resource]LockType {
	return t.resources
}

// Transaction Manager manages all of the transactions on a server.
type TransactionManager struct {
	lm           *LockManager
	tmMtx        sync.RWMutex
	pGraph       *Graph
	transactions map[uuid.UUID]*Transaction
}

// Get a pointer to a new transaction manager.
func NewTransactionManager(lm *LockManager) *TransactionManager {
	return &TransactionManager{lm: lm, pGraph: NewGraph(), transactions: make(map[uuid.UUID]*Transaction)}
}

// Get the transactions.
func (tm *TransactionManager) GetLockManager() *LockManager {
	return tm.lm
}

// Get the transactions.
func (tm *TransactionManager) GetTransactions() map[uuid.UUID]*Transaction {
	return tm.transactions
}

// Get a particular transaction.
func (tm *TransactionManager) GetTransaction(clientId uuid.UUID) (*Transaction, bool) {
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	t, found := tm.transactions[clientId]
	return t, found
}

// Begin a transaction for the given client; error if already began.
func (tm *TransactionManager) Begin(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	_, found := tm.transactions[clientId]
	if found {
		return errors.New("transaction already began")
	}
	tm.transactions[clientId] = &Transaction{clientId: clientId, resources: make(map[Resource]LockType)}
	return nil
}

// Locks the given resource. Will return an error if deadlock is created.
func (tm *TransactionManager) Lock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	// fetching the Transaction by uuid
	tm.tmMtx.RLock()
	t, found := tm.GetTransaction(clientId)
	tm.tmMtx.RUnlock()
	if !found {
		return errors.New("transaction not found")
	}
	// Check if the transaction has rights to the resource
	resource := Resource{tableName: table.GetName(), resourceKey: resourceKey}
	t.RLock()
	lockType, found := t.resources[resource]
	t.RUnlock()
	if found {
		if lockType == R_LOCK && lType == W_LOCK {
			return errors.New("transaction does not have rights to the resource")
		}
		return nil
	}
	// Look for other transactions that might conflict with the current transaction
	depTransactions := tm.discoverTransactions(resource, lType)
	// If a conflicting transaction is found, add an edge to the precedence graph
	for _, trans := range depTransactions {
		tm.pGraph.AddEdge(t, trans)
	}
	// Check for deadlocks in the precedence graph
	if tm.pGraph.DetectCycle() {
		// remove edge from the precedence graph
		for _, trans := range depTransactions {
			tm.pGraph.RemoveEdge(t, trans)
		}
		return errors.New("deadlock detected")
	}
	// Add the resource to the trasaction's resource list and lock it
	t.WLock()
	t.resources[resource] = lType
	t.WUnlock()
	// lock the resource
	tm.lm.Lock(resource, lType)
	// remove the edge from the precedence graph
	//depTransactions = tm.discoverTransactions(resource, lType)
	for _, trans := range depTransactions {
		tm.pGraph.RemoveEdge(t, trans)
	}
	return nil
}

// Unlocks the given resource.
func (tm *TransactionManager) Unlock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	// Fetching the Transaction by uuid
	tm.tmMtx.RLock()
	t, found := tm.GetTransaction(clientId)
	tm.tmMtx.RUnlock()
	if !found {
		return errors.New("transaction to unlock not found")
	}
	// Find the resource in the transaction's resource list
	resource := Resource{tableName: table.GetName(), resourceKey: resourceKey}
	t.RLock()
	lockType, found := t.resources[resource]
	t.RUnlock()
	if !found {
		return errors.New("resource to unlock not found")
	}
	if lockType != lType {
		return errors.New("lock type does not match")
	}
	// Remove the resource from the transaction's resource list and unlock the resource
	t.WLock()
	delete(t.resources, resource)
	t.WUnlock()
	return tm.lm.Unlock(resource, lockType)
}

// Commits the given transaction and removes it from the running transactions list.
func (tm *TransactionManager) Commit(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	// Get the transaction we want.
	t, found := tm.transactions[clientId]
	if !found {
		return errors.New("no transactions running")
	}
	// Unlock all resources.
	t.RLock()
	defer t.RUnlock()
	for r, lType := range t.resources {
		err := tm.lm.Unlock(r, lType)
		if err != nil {
			return err
		}
	}
	// Remove the transaction from our transactions list.
	delete(tm.transactions, clientId)
	return nil
}

// Returns a slice of all transactions that conflict w/ the given resource and locktype.
func (tm *TransactionManager) discoverTransactions(r Resource, lType LockType) []*Transaction {
	ret := make([]*Transaction, 0)
	for _, t := range tm.transactions {
		t.RLock()
		for storedResource, storedType := range t.resources {
			if storedResource == r && (storedType == W_LOCK || lType == W_LOCK) {
				ret = append(ret, t)
				break
			}
		}
		t.RUnlock()
	}
	return ret
}
