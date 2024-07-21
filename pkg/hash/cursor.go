package hash

import (
	"errors"

	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// HashCursor points to a spot in the hash table.
type HashCursor struct {
	table     *HashIndex
	cellnum   int64
	isEnd     bool
	curBucket *HashBucket
}

// TableStart returns a cursor to the first entry in the hash table.
func (table *HashIndex) TableStart() (utils.Cursor, error) {
	cursor := HashCursor{table: table, cellnum: 0}
	curPage, err := table.pager.GetPage(ROOT_PN)
	if err != nil {
		return nil, err
	}
	defer curPage.Put()
	cursor.curBucket = pageToBucket(curPage)
	cursor.isEnd = (cursor.curBucket.numKeys == 0)
	return &cursor, nil
}

// StepForward moves the cursor ahead by one entry.
func (cursor *HashCursor) StepForward() bool {
	// If the cursor is at the end of the bucket, try visiting the next bucket.
	if cursor.isEnd {
		// Get the next page number.
		nextPN := cursor.curBucket.page.GetPageNum() + 1
		if nextPN >= cursor.curBucket.page.GetPager().GetNumPages() {
			return true
		}
		// Convert the page to a bucket.
		nextPage, err := cursor.table.pager.GetPage(nextPN)
		if err != nil {
			return true
		}
		defer nextPage.Put()
		nextBucket := pageToBucket(nextPage)
		// Reinitialize the cursor.
		cursor.cellnum = 0
		cursor.isEnd = (cursor.cellnum == nextBucket.numKeys)
		cursor.curBucket = nextBucket
		if cursor.isEnd {
			return cursor.StepForward()
		}
		return false
	}
	// Else, just move the cursor forward.
	cursor.cellnum++
	if cursor.cellnum >= cursor.curBucket.numKeys {
		cursor.isEnd = true
	}
	return false
}

// IsEnd returns true if at end.
func (cursor *HashCursor) IsEnd() bool {
	return cursor.isEnd
}

// GetEntry returns the entry currently pointed to by the cursor.
func (cursor *HashCursor) GetEntry() (utils.Entry, error) {
	if cursor.isEnd {
		return HashEntry{}, errors.New("getEntry: entry is non-existent")
	}
	entry := cursor.curBucket.getEntry(cursor.cellnum)
	return entry, nil
}
