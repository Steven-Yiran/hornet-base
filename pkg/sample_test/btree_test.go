package test

import (
	"io/ioutil"
	"os"
	"testing"

	btree "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/btree"
)

// Set to some other value
var btree_salt = int64(999999)

func getTempBTreeDB(t *testing.T) string {
	tmpfile, err := ioutil.TempFile(".", "db-*")
	if err != nil {
		t.Error(err)
	}
	defer tmpfile.Close()
	return tmpfile.Name()
}

func TestBTreeTA(t *testing.T) {
	t.Run("TestBTreeInsertTenNoWrite", testBTreeInsertTenNoWrite)
	t.Run("TestBTreeInsertTen", testBTreeInsertTen)
	t.Run("TestBTreeDeleteTenNoWrite", testBTreeDeleteTenNoWrite)
	t.Run("TestBTreeDeleteTen", testBTreeDeleteTen)
	t.Run("TestBTreeUpdateTenNoWrite", testBTreeUpdateTenNoWrite)
	t.Run("TestBTreeUpdateTen", testBTreeUpdateTen)
}

func testBTreeInsertTenNoWrite(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)

	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%btree_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%btree_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testBTreeInsertTen(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)

	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%btree_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Close and reopen the database
	index.Close()
	index, err = btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%btree_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testBTreeDeleteTenNoWrite(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)

	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%btree_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%btree_salt {
			t.Error("Entry found has the wrong value")
		}
		// Delete this entry
		index.Delete(i)
	}
	// Retrieve deleted entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if entry != nil || err == nil {
			t.Error("Could find deleted entry")
		}
	}
	index.Close()
}

func testBTreeDeleteTen(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)

	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%btree_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Close and reopen the database
	index.Close()
	index, err = btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%btree_salt {
			t.Error("Entry found has the wrong value")
		}
		// Delete this entry
		index.Delete(i)
	}
	// Retrieve deleted entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if entry != nil || err == nil {
			t.Error("Could find deleted entry")
		}
	}
	index.Close()
}

func testBTreeUpdateTenNoWrite(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)

	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%btree_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%btree_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	// Update entries
	for i := int64(0); i <= 10; i++ {
		err = index.Update(i, -(i % btree_salt))
		if err != nil {
			t.Error(err)
		}
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != -(i % btree_salt) {
			t.Error("Entry found has the wrong value")
		}
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != -(i % btree_salt) {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testBTreeUpdateTen(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)

	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%btree_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%btree_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	// Update entries
	for i := int64(0); i <= 10; i++ {
		err = index.Update(i, -(i % btree_salt))
		if err != nil {
			t.Error(err)
		}
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != -(i % btree_salt) {
			t.Error("Entry found has the wrong value")
		}
	}
	// Close and reopen the database
	index.Close()
	index, err = btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Retrieve entries
	for i := int64(0); i <= 10; i++ {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != -(i % btree_salt) {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}
