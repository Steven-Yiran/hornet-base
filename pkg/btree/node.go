package btree

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
)

// Split is a supporting data structure to propagate keys up our B+ tree.
type Split struct {
	isSplit bool  // A flag that's set if a split occurs.
	key     int64 // The key to promote.
	leftPN  int64 // The pagenumber for the left node.
	rightPN int64 // The pagenumber for the right node.
	err     error // Used to propagate errors upwards.
}

// Node defines a common interface for leaf and internal nodes.
type Node interface {
	// Interface for main node functions.
	search(int64) int64
	insert(int64, int64, bool) Split
	delete(int64)
	get(int64) (int64, bool)

	// Interface for helper functions.
	keyToNodeEntry(int64) (*LeafNode, int64, error)
	printNode(io.Writer, string, string)
	getPage() *pager.Page
	getNodeType() NodeType
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////// Leaf Node Methods /////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key >= given key.
// If no key satisfies this condition, returns numKeys.
func (node *LeafNode) search(key int64) int64 {
	/* SOLUTION {{{ */
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) >= key
		},
	)
	return int64(minIndex)
	/* SOLUTION }}} */
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	/* SOLUTION {{{ */
	node.unlockParent(false)
	defer node.unlock()
	// Get insert position.
	insertPos := node.search(key)
	// Check if this is a duplicate entry.
	if insertPos < node.numKeys && node.getKeyAt(insertPos) == key {
		if update {
			node.updateValueAt(insertPos, value)
			node.unlockParent(true)
			return Split{}
		} else {
			node.unlockParent(true)
			return Split{err: errors.New("cannot insert duplicate key")}
		}
	}
	// Return an error if we're updating a non-existent entry.
	if update {
		node.unlockParent(true)
		return Split{err: errors.New("cannot update non-existent entry")}
	}
	// Shift entries to the right if needed.
	for i := node.numKeys - 1; i >= insertPos; i-- {
		node.updateKeyAt(i+1, node.getKeyAt(i))
		node.updateValueAt(i+1, node.getValueAt(i))
	}
	node.updateNumKeys(node.numKeys + 1)
	// Modify the Entry at this position.
	node.modifyEntry(insertPos, BTreeEntry{key: key, value: value})
	// Check if we need to split the node.
	if node.numKeys > ENTRIES_PER_LEAF_NODE {
		split := node.split()
		return split
	}
	node.unlockParent(true)
	return Split{}
	/* SOLUTION }}} */
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	// Find entry.
	node.unlockParent(true)
	defer node.unlock()
	deletePos := node.search(key)
	if deletePos >= node.numKeys || node.getKeyAt(deletePos) != key {
		// Thank you Mario! But our key is in another castle!
		return
	}
	// Shift the keys and values to the left
	for i := deletePos; i < node.numKeys-1; i++ {
		node.updateKeyAt(i, node.getKeyAt(i+1))
		node.updateValueAt(i, node.getValueAt(i+1))
	}
	// Update the number of keys
	node.updateNumKeys(node.numKeys - 1)
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	/* SOLUTION {{{ */
	// Create a new leaf node to split our keys.
	newNode, err := createLeafNode(node.page.GetPager())
	if err != nil {
		return Split{err: err}
	}
	defer newNode.getPage().Put()
	// Set the right sibling for our two nodes.
	prevSiblingPN := node.setRightSibling(newNode.page.GetPageNum())
	newNode.setRightSibling(prevSiblingPN)
	// Transfer entries to the new node (plus the new entry) accordingly.
	midpoint := node.numKeys / 2
	for i := midpoint; i < node.numKeys; i++ {
		newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
		newNode.updateValueAt(newNode.numKeys, node.getValueAt(i))
		newNode.updateNumKeys(newNode.numKeys + 1)
	}
	node.updateNumKeys(midpoint)
	return Split{
		isSplit: true,
		key:     newNode.getKeyAt(0), // Get the right node's first key
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
	}
	/* SOLUTION }}} */
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	// Unlock parents, eventually unlock this node.
	node.unlockParent(true)
	defer node.unlock()
	// Find index.
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// Thank you Mario! But our key is in another castle!
		return 0, false
	}
	entry := node.getEntry(index)
	return entry.GetValue(), true
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *LeafNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	return node, node.search(key), nil
}

// printNode pretty prints our leaf node.
func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Leaf"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	for cellnum := int64(0); cellnum < node.numKeys; cellnum++ {
		entry := node.getEntry(cellnum)
		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
			prefix, entry.GetKey(), entry.GetValue()))
	}
	if node.rightSiblingPN > 0 {
		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
		io.WriteString(w, fmt.Sprintf("%v    | right sibling @ [%v]\n",
			prefix, node.rightSiblingPN))
		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
	}
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////// Internal Node Methods ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key > given key.
// If no such index exists, it returns numKeys.
func (node *InternalNode) search(key int64) int64 {
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) > key
		},
	)
	return int64(minIndex)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	node.unlockParent(false)
	// Insert the entry into the appropriate child node. Use getChildAt for the indexing
	childIdx := node.search(key)
	child, err := node.getAndLockChildAt(childIdx)
	if err != nil {
		return Split{err: err}
	}
	node.initChild(child)
	defer child.getPage().Put()

	// Insert value into the child.
	result := child.insert(key, value, update)
	// Insert a new key into our node if necessary.
	if !result.isSplit {
		node.unlockParent(true)
		return Split{err: result.err}
	} else {
		defer node.unlock()
		s := node.insertSplit(result)
		if s.isSplit {
			return s
		} else {
			node.unlockParent(true)
			return s
		}
	}
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {
	/* SOLUTION {{{ */
	insertPos := node.search(split.key)
	// Shift keys to the right.
	for i := node.numKeys - 1; i >= insertPos; i-- {
		node.updateKeyAt(i+1, node.getKeyAt(i))
	}
	// Shift children to the right.
	for i := node.numKeys; i > insertPos; i-- {
		node.updatePNAt(i+1, node.getPNAt(i))
	}
	// Insert the new key and pagenumber at this position.
	node.updateKeyAt(insertPos, split.key)
	node.updatePNAt(insertPos+1, split.rightPN)
	node.updateNumKeys(node.numKeys + 1)
	// Check if we need to split.
	if node.numKeys > KEYS_PER_INTERNAL_NODE {
		return node.split()
	}
	return Split{}
	/* SOLUTION }}} */
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	// Get child.
	node.unlockParent(true)
	childIdx := node.search(key)
	child, err := node.getAndLockChildAt(childIdx)
	if err != nil {
		return
	}
	node.initChild(child)
	defer child.getPage().Put()
	// Delete from child.
	child.delete(key)
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	/* SOLUTION {{{ */
	// Create a new internal node to split our keys.
	newNode, err := createInternalNode(node.page.GetPager())
	if err != nil {
		return Split{err: err}
	}
	defer newNode.getPage().Put()
	// Compute the midpoint based on the number of children to move.
	midpoint := (node.numKeys - 1) / 2
	// Transfer the keys to the new node.
	for i := midpoint; i <= node.numKeys; i++ {
		newNode.updatePNAt(newNode.numKeys, node.getPNAt(i))
		if i < node.numKeys {
			newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
			newNode.updateNumKeys(newNode.numKeys + 1)
		}
	}
	middleKey := node.getKeyAt(midpoint - 1)
	node.updateNumKeys(midpoint - 1)
	// Propagate the split.
	return Split{
		isSplit: true,
		key:     middleKey,
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
	}
	/* SOLUTION }}} */
}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	// [CONCURRENCY] Unlock parents
	node.unlockParent(true)
	// Find the child.
	childIdx := node.search(key)
	child, err := node.getAndLockChildAt(childIdx)
	if err != nil {
		return 0, false
	}
	node.initChild(child)
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index)
	if err != nil {
		return &LeafNode{}, 0, err
	}
	defer child.getPage().Put()
	return child.keyToNodeEntry(key)
}

// printNode pretty prints our internal node.
func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Internal"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys + 1))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	nextFirstPrefix := prefix + " |--> "
	nextPrefix := prefix + " |    "
	for idx := int64(0); idx <= node.numKeys; idx++ {
		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
		child, err := node.getChildAt(idx)
		if err != nil {
			return
		}
		defer child.getPage().Put()
		child.printNode(w, nextFirstPrefix, nextPrefix)
		if idx != node.numKeys {
			io.WriteString(w, fmt.Sprintf("\n%v[KEY] %v\n", nextPrefix, node.getKeyAt(idx)))
		}
	}
}
