package btree

import (
	"errors"
	"io"

	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// Tables are an abstraction over the entries stored in our database.
type BTreeIndex struct {
	pager  *pager.Pager // The page handler to read from files.
	rootPN int64        // The root page number.
}

// OpenTable returns a table associated with the given database filename.
func OpenTable(filename string) (table *BTreeIndex, err error) {
	// Create a pager for the table
	pager := pager.NewPager()
	err = pager.Open(filename)
	if err != nil {
		return nil, err
	}
	// Initialize the pager if it's new.
	if pager.GetNumPages() == 0 {
		rootPage, err := pager.GetPage(ROOT_PN)
		if err != nil {
			return nil, err
		}
		defer rootPage.Put()
		initPage(rootPage, LEAF_NODE)
		rootNode := pageToLeafNode(rootPage)
		rootNode.setRightSibling(-1)
	}
	return &BTreeIndex{pager: pager, rootPN: ROOT_PN}, nil
}

// Get this index's filename.
func (table *BTreeIndex) GetName() string {
	return table.pager.GetFileName()
}

// Get this index's pager.
func (table *BTreeIndex) GetPager() *pager.Pager {
	return table.pager
}

// Close flushes all changes to disk.
func (table *BTreeIndex) Close() (err error) {
	err = table.pager.Close()
	return err
}

// Finds the given key.
func (table *BTreeIndex) Find(key int64) (utils.Entry, error) {
	// Get the root node.
	rootPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return nil, err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer rootPage.Put()
	// Insert the entry into the root node.
	value, found := rootNode.get(key)
	if found {
		return BTreeEntry{key: key, value: value}, nil
	}
	return nil, errors.New("entry could not be found")
}

// Inserts an entry to the table.
func (table *BTreeIndex) Insert(key int64, value int64) error {
	// Get the root node.
	rootPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer rootPage.Put()
	// Insert the entry into the root node.
	result := rootNode.insert(key, value, false)
	// Check if we need to split the root node.
	// Remember to preserve the invariant that the root node occupies page 0.
	if result.isSplit {
		// [CONCURRENCY] Unlock the root node.
		defer SUPER_NODE.unlock()
		// Ensure that our left PN hasn't changed.
		if result.leftPN != 0 {
			return errors.New("splitting was corrupted")
		}
		// Create a new node to transfer our data.
		var newNodePN int64
		// Depending on whether the root is a leaf or an internal node...
		if rootNode.getNodeType() == LEAF_NODE {
			// Create a new leaf node.
			newNode, err := createLeafNode(table.pager)
			if err != nil {
				return errors.New("failed to split root node")
			}
			defer newNode.page.Put()
			// Copy the attributes from the root node.
			leafyRoot := pageToLeafNode(rootNode.getPage())
			newNode.copy(leafyRoot)
			newNodePN = newNode.page.GetPageNum()
		} else {
			// Create a new internal node.
			newNode, err := createInternalNode(table.pager)
			if err != nil {
				return errors.New("failed to split root node")
			}
			defer newNode.page.Put()
			// Copy the attributes from the root node.
			internedRoot := pageToInternalNode(rootNode.getPage())
			newNode.copy(internedRoot)
			newNodePN = newNode.page.GetPageNum()
		}
		// Reinitialize the root node.
		initPage(rootNode.getPage(), INTERNAL_NODE)
		newRoot := pageToInternalNode(rootNode.getPage())
		// Populate the pointers to children.
		newRoot.updateKeyAt(0, result.key)
		newRoot.updatePNAt(0, newNodePN)
		newRoot.updatePNAt(1, result.rightPN)
		newRoot.updateNumKeys(1)
	}
	return result.err
}

// Update modifies an existing entry.
func (table *BTreeIndex) Update(key int64, value int64) error {
	// Get the root node.
	rootPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer rootPage.Put()
	// Update the entry.
	result := rootNode.insert(key, value, true)
	return result.err
}

// Delete removes a key from the table.
func (table *BTreeIndex) Delete(key int64) error {
	// Get the root node.
	rootPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return err
	}
	// [CONCURRENCY] Lock and eventually unlock the root node.
	lockRoot(rootPage)
	rootNode := pageToNode(rootPage)
	initRootNode(rootNode)
	defer unsafeUnlockRoot(rootNode)
	defer rootPage.Put()
	// Delete the key.
	rootNode.delete(key)
	return nil
}

// Select returns a slice of all entries in the table.
func (table *BTreeIndex) Select() ([]utils.Entry, error) {
	// Use a cursor to traverse the table from start to end.
	entries := make([]utils.Entry, 0)
	cursor, err := table.TableStart()
	if err != nil {
		return nil, err
	}

	// Traverse over all entries.
	for {
		atEnd := cursor.IsEnd()
		if !atEnd {
			entry, err := cursor.GetEntry()
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		if err := cursor.StepForward(); err {
			break
		}
	}

	return entries, nil
}

// Print will pretty-print all nodes in the table.
func (table *BTreeIndex) Print(w io.Writer) {
	rootPage, err := table.pager.GetPage(table.rootPN)
	if err != nil {
		return
	}
	defer rootPage.Put()
	rootNode := pageToNode(rootPage)
	rootNode.printNode(w, "", "")
}

// PrintPN will pretty-print the node with page number PN.
func (table *BTreeIndex) PrintPN(pagenum int, w io.Writer) {
	page, err := table.pager.GetPage(int64(pagenum))
	if err != nil {
		return
	}
	defer page.Put()
	node := pageToNode(page)
	node.printNode(w, "", "")
}
