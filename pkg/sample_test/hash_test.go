package test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
)

type hash_kv struct {
	key int64
	val int64
}

// Mod vals by this value to prevent hardcoding tests
var hash_salt int64 = rand.Int63n(1000)

func getTempHashDB(t *testing.T) string {
	tmpfile, err := ioutil.TempFile(".", "db-*")
	if err != nil {
		t.Error(err)
	}
	defer tmpfile.Close()
	return tmpfile.Name()
}

func genRandomHashEntries(n int) (entries []hash_kv, answerKey map[int64]int64) {
	entries = make([]hash_kv, 0)
	answerKey = make(map[int64]int64)
	for i := 0; i <= n; i++ {
	genKey:
		key := rand.Int63()
		if _, ok := answerKey[key]; ok {
			goto genKey
		}
		val := rand.Int63()
		answerKey[key] = val
		entries = append(entries, hash_kv{key: key, val: val})
	}
	return entries, answerKey
}

func TestHashTA(t *testing.T) {
	t.Run("TestHashInsertTenNoWrite", testHashInsertTenNoWrite)
	t.Run("TestHashInsertTen", testHashInsertTen)
	t.Run("TestHashDeleteTenNoWrite", testHashDeleteTenNoWrite)
	t.Run("TestHashDeleteTen", testHashDeleteTen)
	t.Run("TestHashUpdateTenNoWrite", testHashUpdateTenNoWrite)
	t.Run("TestHashUpdateTen", testHashUpdateTen)
}

func testHashInsertTenNoWrite(t *testing.T) {
	dbName := getTempHashDB(t)
	defer os.Remove(dbName)
	defer os.Remove(dbName + ".meta")

	// Init the database
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%hash_salt)
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
		if entry.GetValue() != i%hash_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testHashInsertTen(t *testing.T) {
	dbName := getTempHashDB(t)
	defer os.Remove(dbName)
	defer os.Remove(dbName + ".meta")

	// Init the database
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%hash_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Close and reopen the database
	index.Close()
	index, err = hash.OpenTable(dbName)
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
		if entry.GetValue() != i%hash_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testHashDeleteTenNoWrite(t *testing.T) {
	dbName := getTempHashDB(t)
	defer os.Remove(dbName)
	defer os.Remove(dbName + ".meta")

	// Init the database
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%hash_salt)
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
		if entry.GetValue() != i%hash_salt {
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

func testHashDeleteTen(t *testing.T) {
	dbName := getTempHashDB(t)
	defer os.Remove(dbName)
	defer os.Remove(dbName + ".meta")

	// Init the database
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%hash_salt)
		if err != nil {
			t.Error(err)
		}
	}
	// Close and reopen the database
	index.Close()
	index, err = hash.OpenTable(dbName)
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
		if entry.GetValue() != i%hash_salt {
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

func testHashUpdateTenNoWrite(t *testing.T) {
	dbName := getTempHashDB(t)
	defer os.Remove(dbName)
	defer os.Remove(dbName + ".meta")

	// Init the database
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%hash_salt)
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
		if entry.GetValue() != i%hash_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	// Update entries
	for i := int64(0); i <= 10; i++ {
		err = index.Update(i, -(i % hash_salt))
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
		if entry.GetValue() != -(i % hash_salt) {
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
		if entry.GetValue() != -(i % hash_salt) {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testHashUpdateTen(t *testing.T) {
	dbName := getTempHashDB(t)
	defer os.Remove(dbName)
	defer os.Remove(dbName + ".meta")

	// Init the database
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Insert entries
	for i := int64(0); i <= 10; i++ {
		err = index.Insert(i, i%hash_salt)
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
		if entry.GetValue() != i%hash_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	// Update entries
	for i := int64(0); i <= 10; i++ {
		err = index.Update(i, -(i % hash_salt))
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
		if entry.GetValue() != -(i % hash_salt) {
			t.Error("Entry found has the wrong value")
		}
	}
	// Close and reopen the database
	index.Close()
	index, err = hash.OpenTable(dbName)
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
		if entry.GetValue() != -(i % hash_salt) {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}
