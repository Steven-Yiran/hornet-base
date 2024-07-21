package pager

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// pagenum for when there is no page being held
const NOPAGE = -1

// A page is a unit that is read from and written to disk.
type Page struct {
	pager      *Pager       // Pointer to the pager that this page belongs to.
	pagenum    int64        // Position of the page in the file.
	pinCount   int64        // The number of active references to this page.
	dirty      bool         // Flag on whether data has to be written back.
	rwlock     sync.RWMutex // Readers-writers lock on the page itself
	updateLock sync.Mutex   // Mutex for updating data in a page
	data       *[]byte      // Serialized data.
}

// Get the pager.
func (page *Page) GetPager() *Pager {
	return page.pager
}

// Get the pagenum.
func (page *Page) GetPageNum() int64 {
	return page.pagenum
}

// Is dirty?
func (page *Page) IsDirty() bool {
	return page.dirty
}

// Set dirty.
func (page *Page) SetDirty(dirty bool) {
	page.dirty = dirty
}

// Get data.
func (page *Page) GetData() *[]byte {
	return page.data
}

// Increment the pincount.
func (page *Page) Get() {
	atomic.AddInt64(&page.pinCount, 1)
}

// Release a reference to the page.
func (page *Page) Put() {
	pager := page.pager
	pager.ptMtx.Lock()
	ret := atomic.AddInt64(&page.pinCount, -1)
	// Check if we can unpin this page; if so, move from pinned to unpinned list.
	if ret == 0 {
		link := pager.pageTable[page.pagenum]
		link.PopSelf()
		newLink := pager.unpinnedList.PushTail(page)
		pager.pageTable[page.pagenum] = newLink
	}
	page.pager.ptMtx.Unlock()
	if ret < 0 {
		fmt.Println("ERROR: pinCount for page is < 0")
	}
}

// Update the target page with `size` bytes of the the given data.
func (page *Page) Update(data []byte, offset int64, size int64) {
	page.updateLock.Lock()
	defer page.updateLock.Unlock()
	page.dirty = true
	copy((*page.data)[offset:offset+size], data)
}

// [CONCURRENCY] Grab a writers lock on the page.
func (page *Page) WLock() {
	page.rwlock.Lock()
}

// [CONCURRENCY] Release a writers lock.
func (page *Page) WUnlock() {
	page.rwlock.Unlock()
}

// [CONCURRENCY] Grab a readers lock on the page.
func (page *Page) RLock() {
	page.rwlock.RLock()
}

// [CONCURRENCY] Release a readers lock.
func (page *Page) RUnlock() {
	page.rwlock.RUnlock()
}

// [RECOVERY] Grab the update lock.
func (page *Page) LockUpdates() {
	page.updateLock.Lock()
}

// [RECOVERY] Release the update lock.
func (page *Page) UnlockUpdates() {
	page.updateLock.Unlock()
}
