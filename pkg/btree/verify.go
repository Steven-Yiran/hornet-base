package btree

import (
	"errors"
)

func IsBTree(index *BTreeIndex) (l int64, r int64, isbtree bool, err error) {
	// Get the node from the page
	rootPage, err := index.pager.GetPage(index.rootPN)
	if err != nil {
		return 0, 0, false, err
	}
	n := pageToNode(rootPage)
	return isBTree(n)
}

func isBTree(n Node) (l int64, r int64, isbtree bool, err error) {
	// Depending on the node type...
	switch n := n.(type) {
	case *InternalNode:
		// Check that each key is less than the bounds of the node it goes around.
		var lowest, highest int64
		for i := int64(0); i < n.numKeys+1; i++ {
			// Get child
			c, err := n.getChildAt(i)
			if err != nil {
				return -1, -1, false, err
			}
			// Check if child is BTree
			cl, cr, cisbtree, err := isBTree(c)
			if err != nil {
				return -1, -1, false, err
			} else if !cisbtree {
				return -1, -1, false, nil
			}
			// Set conditions.
			if i == 0 {
				lowest = cl
			}
			if i == n.numKeys {
				highest = cr
			}
			// If it is, check that the key bounds work out.
			if i-1 >= 0 {
				k := n.getKeyAt(i - 1)
				if k > cl {
					return -1, -1, false, nil
				}
			}
			if i < n.numKeys {
				k := n.getKeyAt(i)
				if k < cr {
					return -1, -1, false, nil
				}
			}
		}
		// Return bounds.
		return lowest, highest, true, nil
	case *LeafNode:
		// Check that each key is less than the one after it.
		for i := int64(0); i < n.numKeys-1; i++ {
			if n.getKeyAt(i) > n.getKeyAt(i+1) {
				return -1, -1, false, nil
			}
		}
		// If good, return bounds.
		return n.getKeyAt(0), n.getKeyAt(n.numKeys - 1), true, nil
	default:
		return -1, -1, false, errors.New("should not have gotten here")
	}
}
