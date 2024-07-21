package btree

import (
	"errors"
	"sync"

	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// Cursors are an abstration to represent locations in a table.
type BTreeCursor struct {
	table   *BTreeIndex  // The table that this cursor point to.
	cellnum int64        // The cell number within a leaf node.
	isEnd   bool         // Indicates that this cursor is at the end of a node.
	curNode *LeafNode    // Current node.
	mu      sync.RWMutex // Mutex for cursor
}

// TableStart returns a cursor pointing to the first entry of the table and lock it.
func (table *BTreeIndex) TableStart() (utils.Cursor, error) {
	cursor := BTreeCursor{table: table, cellnum: 0}
	// Get the root page.
	curPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return nil, err
	}
	defer curPage.Put()
	curHeader := pageToNodeHeader(curPage)
	// Traverse the leftmost children until we reach a leaf node.
	for curHeader.nodeType != LEAF_NODE {
		curNode := pageToInternalNode(curPage)
		leftmostPN := curNode.getPNAt(0)
		curPage, err = table.pager.GetPage(leftmostPN)
		if err != nil {
			return nil, err
		}
		defer curPage.Put()
		curHeader = pageToNodeHeader(curPage)
	}
	// Set the cursor to point to the first entry in the leftmost leaf node.
	leftmostNode := pageToLeafNode(curPage)
	leftmostNode.page.RLock()
	cursor.isEnd = (leftmostNode.numKeys == 0)
	cursor.curNode = leftmostNode
	return &cursor, nil
}

// TableEnd returns a cursor pointing to the last entry in the db.
// If the db is empty, returns a cursor to the new insertion position.
func (table *BTreeIndex) TableEnd() (utils.Cursor, error) {
	cursor := BTreeCursor{table: table, cellnum: 0}
	// Get the root page.
	curPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return &BTreeCursor{}, err
	}
	defer curPage.Put()
	curHeader := pageToNodeHeader(curPage)
	// Traverse the rightmost children until we reach a leaf node.
	for curHeader.nodeType != LEAF_NODE {
		curNode := pageToInternalNode(curPage)
		rightmostPN := curNode.getPNAt(curHeader.numKeys)
		curPage, err = table.pager.GetPage(rightmostPN)
		if err != nil {
			return &BTreeCursor{}, err
		}
		defer curPage.Put()
		curHeader = pageToNodeHeader(curPage)
	}
	// Set the cursor to point to the last entry in the rightmost leaf node.
	rightmostNode := pageToLeafNode(curPage)
	cursor.isEnd = false
	cursor.cellnum = rightmostNode.numKeys - 1
	cursor.curNode = rightmostNode
	return &cursor, nil
}

// TableFind returns a cursor pointing to the given key.
// If the key is not found, returns a cursor to the new insertion position.
// Hint: use keyToNodeEntry
func (table *BTreeIndex) TableFind(key int64) (utils.Cursor, error) {
	cursor := BTreeCursor{table: table}
	// Get the root page.
	rootPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return &BTreeCursor{}, err
	}
	defer rootPage.Put()
	rootNode := pageToNode(rootPage)
	// Find the leaf node and cellnum that this key belongs to.
	leaf, cellnum, err := rootNode.keyToNodeEntry(key)
	if err != nil {
		return &BTreeCursor{}, err
	}
	// Initialize cursor.
	cursor.cellnum = cellnum
	cursor.isEnd = (cellnum == leaf.numKeys)
	cursor.curNode = leaf
	return &cursor, nil
}

// TableFindRange returns a slice of Entries with keys between the startKey and endKey.
func (table *BTreeIndex) TableFindRange(startKey int64, endKey int64) ([]utils.Entry, error) {
	/* SOLUTION {{{ */
	// Initialize entries array, get starting cursor.
	entries := make([]utils.Entry, 0)
	cursor, err := table.TableFind(startKey)
	if err != nil {
		return entries, err
	}
	// Keep advancing the cursor and adding the current entry to the list of
	// entries until reaching the end key.
	curEntry, err := cursor.GetEntry()
	if err != nil {
		return entries, err
	}
	for endKey > curEntry.GetKey() && !cursor.IsEnd() {
		entries = append(entries, curEntry)
		cursor.StepForward()
		curEntry, err = cursor.GetEntry()
		if err != nil {
			return entries, err
		}
	}
	return entries, nil
	/* SOLUTION }}} */
}

// stepForward moves the cursor ahead by one entry. Returns true at the end of the BTree.
func (cursor *BTreeCursor) StepForward() (atEnd bool) {
	// If the cursor is at the end of the node, go to the next node.
	if cursor.cellnum+1 >= cursor.curNode.numKeys {
		// Get the next node's page number.
		nextPN := cursor.curNode.rightSiblingPN
		if nextPN < 0 {
			cursor.curNode.page.RUnlock()
			return true
		}
		// Convert the page into a node.
		nextPage, err := cursor.table.pager.GetPage(nextPN)
		if err != nil {
			cursor.curNode.page.RUnlock()
			return true
		}
		defer nextPage.Put()
		nextNode := pageToLeafNode(nextPage)
		nextNode.page.RLock()
		cursor.curNode.page.RUnlock()
		// Reinitialize the cursor.
		cursor.cellnum = 0
		cursor.curNode = nextNode
		// If the next node is empty, step to the next node.
		if cursor.cellnum == nextNode.numKeys {
			return cursor.StepForward()
		}
		return false
	}
	// Else, just move the cursor forward.
	cursor.cellnum++
	return false
}

// IsEnd returns true if at end.
func (cursor *BTreeCursor) IsEnd() bool {
	return cursor.isEnd
}

// getEntry returns the entry currently pointed to by the cursor.
func (cursor *BTreeCursor) GetEntry() (utils.Entry, error) {
	// Check if we're retrieving a non-existent entry.
	if cursor.isEnd {
		return BTreeEntry{}, errors.New("getEntry: entry is non-existent")
	}
	cursor.curNode.page.WLock()
	defer cursor.curNode.page.WUnlock()
	entry := cursor.curNode.getEntry(cursor.cellnum)
	return entry, nil
}
