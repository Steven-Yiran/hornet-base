package test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
	"github.com/csci1270-fall-2023/dbms-projects-handout/pkg/query"
)

func TestQueryTA(t *testing.T) {
	t.Run("TestQuerySimple", testQuerySimple)
	t.Run("TestFilterInsertAndCheckSmall", testFilterInsertAndCheckSmall)
}

// Mod vals by this value to prevent hardcoding tests
var query_salt int64 = rand.Int63n(1000)

func getTempQueryDB(t *testing.T) string {
	tmpfile, err := ioutil.TempFile(".", "db-*")
	if err != nil {
		t.Error(err)
	}
	defer tmpfile.Close()
	return tmpfile.Name()
}

func setupQuery(t *testing.T) (string, string, *hash.HashIndex, *hash.HashIndex) {
	// Init the first database
	dbName1 := getTempQueryDB(t)
	defer os.Remove(dbName1)
	index1, err := hash.OpenTable(dbName1)
	if err != nil {
		t.Error(err)
	}

	// Init the second database
	dbName2 := getTempQueryDB(t)
	defer os.Remove(dbName2)
	index2, err := hash.OpenTable(dbName2)
	if err != nil {
		t.Error(err)
	}
	return dbName1, dbName2, index1, index2
}

func getresults(t *testing.T, index1 *hash.HashIndex, index2 *hash.HashIndex, joinOnLeftKey bool, joinOnRightKey bool) ([]query.EntryPair, error) {
	// Create context.
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	// Join the indixes; set up cleanup.
	resultsChan, _, group, cleanupCallback, err := query.Join(ctx, index1, index2, joinOnLeftKey, joinOnRightKey)
	if cleanupCallback != nil {
		defer cleanupCallback()
	}
	if err != nil {
		return nil, err
	}

	// Iterate through results.
	done := make(chan bool)
	results := make([]query.EntryPair, 0)
	go func() {
		for {
			pair, valid := <-resultsChan
			if !valid {
				break
			}
			results = append(results, pair)
		}
		done <- true
	}()

	// Wait, close, and return.
	err = group.Wait()
	close(resultsChan)
	<-done
	if err != nil {
		return nil, fmt.Errorf("join error: %v", err)
	}
	return results, nil
}

func teardownQuery(dbName1 string, dbName2 string, index1 *hash.HashIndex, index2 *hash.HashIndex) {
	index1.Close()
	index2.Close()
	os.Remove(dbName1 + ".meta")
	os.Remove(dbName2 + ".meta")
}

func testQuerySimple(t *testing.T) {
	// Setup.
	var err error
	dbName1, dbName2, index1, index2 := setupQuery(t)

	// Insert entries.
	for i := int64(0); i < 10; i++ {
		err = index1.Insert(i, i%query_salt)
		if err != nil {
			t.Error(err)
		}
	}
	err = index2.Insert(5, 5%query_salt)
	if err != nil {
		t.Error(err)
	}
	err = index2.Insert(6, 10%query_salt)
	if err != nil {
		t.Error(err)
	}

	// Get and check results.
	results, err := getresults(t, index1, index2, true, true)
	if err != nil {
		t.Error(err)
	}
	if len(results) != 2 {
		t.Errorf("basic join not working; expected %v results, got %d\n", 2, len(results))
	}

	// Cleanup.
	teardownQuery(dbName1, dbName2, index1, index2)
}

func testFilterInsertAndCheckSmall(t *testing.T) {
	filter := query.CreateFilter(16)
	for i := 0; i < 10; i++ {
		filter.Insert(int64(i))
		if !filter.Contains(int64(i)) {
			t.Errorf("inserted value %d but not found", i)
		}
	}
}
