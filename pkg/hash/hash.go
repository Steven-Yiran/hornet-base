package hash

import (
	"io"

	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// HashIndex is an index that uses a HashTable as its datastructure. Implements db.Index.
type HashIndex struct {
	table *HashTable
	pager *pager.Pager
}

// Opens the pager with the given table name.
func OpenTable(filename string) (*HashIndex, error) {
	// Create a pager for the table.
	pager := pager.NewPager()
	err := pager.Open(filename)
	if err != nil {
		return nil, err
	}
	// Return index.
	var table *HashTable
	if pager.GetNumPages() == 0 {
		table, err = NewHashTable(pager)
	} else {
		table, err = ReadHashTable(pager)
	}
	if err != nil {
		return nil, err
	}
	return &HashIndex{table: table, pager: pager}, nil
}

// Get name.
func (table *HashIndex) GetName() string {
	return table.pager.GetFileName()
}

// Get pager.
func (table *HashIndex) GetPager() *pager.Pager {
	return table.pager
}

// Get table.
func (index *HashIndex) GetTable() *HashTable {
	return index.table
}

// Closes the table by closing the pager.
func (index *HashIndex) Close() error {
	return WriteHashTable(index.pager, index.table)
}

// Find element by key.
func (index *HashIndex) Find(key int64) (utils.Entry, error) {
	return index.table.Find(key)
}

// Insert given element.
func (index *HashIndex) Insert(key int64, value int64) error {
	return index.table.Insert(key, value)
}

// Update given element.
func (index *HashIndex) Update(key int64, value int64) error {
	return index.table.Update(key, value)
}

// Delete given element.
func (index *HashIndex) Delete(key int64) error {
	return index.table.Delete(key)
}

// Select all elements.
func (index *HashIndex) Select() ([]utils.Entry, error) {
	return index.table.Select()
}

// Print all elements.
func (index *HashIndex) Print(w io.Writer) {
	index.table.Print(w)
}

// Print a page of elements.
func (index *HashIndex) PrintPN(pn int, w io.Writer) {
	index.table.PrintPN(pn, w)
}
