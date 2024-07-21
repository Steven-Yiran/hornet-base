package db

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	btree "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/btree"
	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// Database interface.
type Database struct {
	basepath string
	tables   map[string]Index
}

// Index interface.
type Index interface {
	Close() error
	GetName() string
	GetPager() *pager.Pager
	Find(int64) (utils.Entry, error)
	Insert(int64, int64) error
	Update(int64, int64) error
	Delete(int64) error
	Select() ([]utils.Entry, error)
	Print(io.Writer)
	PrintPN(int, io.Writer)
	TableStart() (utils.Cursor, error)
}

// An index can either be a B+Tree or a Hash Table.
type IndexType int64

const (
	BTreeIndexType IndexType = 0
	HashIndexType  IndexType = 1
)

// Opens a database given a data folder.
func Open(folder string) (*Database, error) {
	// Ensure folder is of the form */
	if !strings.HasSuffix(folder, "/") {
		folder += "/"
	}
	// Make the data directory.
	err := os.MkdirAll(folder, 0775)
	if err != nil {
		return nil, err
	}
	// Return an empty database.
	return &Database{
		basepath: folder,
		tables:   make(map[string]Index),
	}, nil
}

// Close each table in the database, then close the database.
func (db *Database) Close() (err error) {
	for _, table := range db.tables {
		curErr := table.Close()
		if err == nil {
			err = curErr
		}
	}
	return err
}

// Create a log file for the database.
func (db *Database) CreateLogFile(filename string) error {
	if _, err := os.Stat(filename); err == nil {
		return nil
	}
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	return file.Close()
}

// Create a table with the given type.
func (db *Database) createTable(name string, indexType IndexType) (index Index, err error) {
	// Ensure the db name is alphanumeric.
	alphanumeric, _ := regexp.Compile(`\W`)
	if alphanumeric.MatchString(name) {
		return nil, errors.New("table name must be alphanumeric")
	}
	// Create the file, if not exists.
	path := filepath.Join(db.basepath, name)
	if _, err := os.Stat(path); err == nil {
		return nil, errors.New("table already exists")
	}
	// Open the right type of index.
	switch indexType {
	case BTreeIndexType:
		index, err = btree.OpenTable(path)
		if err != nil {
			return nil, err
		}
	case HashIndexType:
		index, err = hash.OpenTable(path)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("invalid index type")
	}
	db.tables[name] = index
	return index, nil
}

// Get a table by its name, either from existing tables, or by creating a new one.
func (db *Database) GetTable(name string) (index Index, err error) {
	// Check existing set of tables.
	if idx, ok := db.tables[name]; ok {
		return idx, nil
	}
	// Check if file exists; if not, error.
	path := filepath.Join(db.basepath, name)
	if _, err := os.Stat(path); err != nil {
		return nil, errors.New("table not found")
	}
	// Else, open from disk.
	// NOTE: This is janky; assumes that if a .meta file exists, then it is a hash index,
	// else, it is a btree index.
	// if _, err := os.Stat(path + ".meta"); err == nil {
	// 	index, err = hash.OpenTable(path)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// } else {
	index, err = btree.OpenTable(path)
	if err != nil {
		return nil, err
	}
	// }
	db.tables[name] = index
	return index, nil
}

// Get a database's tables.
func (db *Database) GetTables() map[string]Index {
	return db.tables
}

// Returns the basepath of the database.
func (db *Database) GetBasePath() string {
	return db.basepath
}
