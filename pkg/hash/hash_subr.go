package hash

import (
	"encoding/binary"

	xxhash "github.com/cespare/xxhash"
	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
	murmur3 "github.com/spaolacci/murmur3"
)

// Hash table variables
var ROOT_PN int64 = 0
var PAGESIZE int64 = pager.PAGESIZE
var DIRECTORY_HEADER_SIZE int64 = binary.MaxVarintLen64 * 2 // Must store global depth and next pointer
var DEPTH_OFFSET int64 = 0
var DEPTH_SIZE int64 = binary.MaxVarintLen64
var NUM_KEYS_OFFSET int64 = DEPTH_OFFSET + DEPTH_SIZE
var NUM_KEYS_SIZE int64 = binary.MaxVarintLen64
var BUCKET_HEADER_SIZE int64 = DEPTH_SIZE + NUM_KEYS_SIZE
var ENTRYSIZE int64 = binary.MaxVarintLen64 * 2                    // int64 key, int64 value
var BUCKETSIZE int64 = (PAGESIZE-BUCKET_HEADER_SIZE)/ENTRYSIZE - 1 // num entries

// Lock Types
type BucketLockType int

const (
	NO_LOCK    BucketLockType = 0
	WRITE_LOCK BucketLockType = 1
	READ_LOCK  BucketLockType = 2
)

// getHash returns the hash of a key, given a hashing function.
func getHash(hasher func(b []byte) uint64, key int64, size int64) uint {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, key)
	hash := int64(hasher(buf))
	if hash < 0 {
		hash *= -1
	}
	return uint(hash % size)
}

// XxHasher returns the xxHash hash of the given key, bounded by size.
func XxHasher(key int64, size int64) uint {
	return getHash(xxhash.Sum64, key, size)
}

// MurmurHasher returns the MurmurHash3 hash of the given key, bounded by size.
func MurmurHasher(key int64, size int64) uint {
	return getHash(murmur3.Sum64, key, size)
}

// Hasher returns the hash of a key, modded by 2^depth.
func Hasher(key int64, depth int64) int64 {
	return int64(XxHasher(key, powInt(2, depth)))
}

// Get the byte-position of the cell with the given index.
func entryPos(index int64) int64 {
	return BUCKET_HEADER_SIZE + index*ENTRYSIZE
}

// Write the given entry into the given index.
func (bucket *HashBucket) modifyEntry(index int64, entry HashEntry) {
	newdata := entry.Marshal()
	startPos := entryPos(index)
	bucket.page.Update(newdata, startPos, ENTRYSIZE)
}

// Get the entry at the given index.
func (bucket *HashBucket) getEntry(index int64) HashEntry {
	startPos := entryPos(index)
	entry := unmarshalEntry((*bucket.page.GetData())[startPos : startPos+ENTRYSIZE])
	return entry
}

// Get the key at the given index.
func (bucket *HashBucket) getKeyAt(index int64) int64 {
	return bucket.getEntry(index).GetKey()
}

// Update the key at the given index.
func (bucket *HashBucket) updateKeyAt(index int64, key int64) {
	entry := bucket.getEntry(index)
	entry.SetKey(key)
	bucket.modifyEntry(index, entry)
}

// Get the value at the given index.
func (bucket *HashBucket) getValueAt(index int64) int64 {
	return bucket.getEntry(index).GetValue()
}

// Update the value at the given index.
func (bucket *HashBucket) updateValueAt(index int64, value int64) {
	entry := bucket.getEntry(index)
	entry.SetValue(value)
	bucket.modifyEntry(index, entry)
}

// Update this bucket's depth.
func (bucket *HashBucket) updateDepth(depth int64) {
	bucket.depth = depth
	depthData := make([]byte, DEPTH_SIZE)
	binary.PutVarint(depthData, depth)
	bucket.page.Update(depthData, DEPTH_OFFSET, DEPTH_SIZE)
}

// Update number of keys in this bucket.
func (bucket *HashBucket) updateNumKeys(nKeys int64) {
	bucket.numKeys = nKeys
	nKeysData := make([]byte, NUM_KEYS_SIZE)
	binary.PutVarint(nKeysData, nKeys)
	bucket.page.Update(nKeysData, NUM_KEYS_OFFSET, NUM_KEYS_SIZE)
}

// Convert a page into a bucket.
func pageToBucket(page *pager.Page) *HashBucket {
	depth, _ := binary.Varint(
		(*page.GetData())[DEPTH_OFFSET : DEPTH_OFFSET+DEPTH_SIZE],
	)
	numKeys, _ := binary.Varint(
		(*page.GetData())[NUM_KEYS_OFFSET : NUM_KEYS_OFFSET+NUM_KEYS_SIZE],
	)
	return &HashBucket{
		depth:   depth,
		numKeys: numKeys,
		page:    page,
	}
}

// Returns the bucket in the hash table using its page number, and increments the bucket ref count.
func (table *HashTable) GetBucketByPN(pn int64) (*HashBucket, error) {
	page, err := table.pager.GetPage(pn)
	if err != nil {
		return nil, err
	}
	return pageToBucket(page), nil
}

// Returns the bucket in the hash table using its page number, and increments the bucket ref count.
func (table *HashTable) GetAndLockBucketByPN(pn int64, lock BucketLockType) (*HashBucket, error) {
	page, err := table.pager.GetPage(pn)
	if err != nil {
		return nil, err
	}
	if lock == READ_LOCK {
		page.RLock()
	}
	if lock == WRITE_LOCK {
		page.WLock()
	}
	return pageToBucket(page), nil
}

// Returns the bucket in the hash table, and increments the bucket ref count.
func (table *HashTable) GetBucket(hash int64) (*HashBucket, error) {
	pagenum := table.buckets[hash]
	bucket, err := table.GetBucketByPN(pagenum)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

// Returns the bucket in the hash table, and increments the bucket ref count.
func (table *HashTable) GetAndLockBucket(hash int64, lock BucketLockType) (*HashBucket, error) {
	pagenum := table.buckets[hash]
	bucket, err := table.GetAndLockBucketByPN(pagenum, lock)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

// Read hash table in from memory.
func ReadHashTable(bucketPager *pager.Pager) (*HashTable, error) {
	indexPager := pager.NewPager()
	err := indexPager.Open(bucketPager.GetFileName() + ".meta")
	if err != nil {
		return nil, err
	}
	metaPN := int64(0)
	page, err := indexPager.GetPage(metaPN)
	if err != nil {
		return nil, err
	}
	// Read the gobal depth
	depth, _ := binary.Varint((*page.GetData())[:DEPTH_SIZE])
	bytesRead := DEPTH_SIZE
	// Read the bucket index
	pnSize := int64(binary.MaxVarintLen64)
	numHashes := powInt(2, depth)
	buckets := make([]int64, numHashes)
	for i := int64(0); i < numHashes; i++ {
		if bytesRead+pnSize > PAGESIZE {
			page.Put()
			metaPN++
			page, err = indexPager.GetPage(metaPN)
			if err != nil {
				return nil, err
			}
			bytesRead = 0
		}
		pn, _ := binary.Varint((*page.GetData())[bytesRead : bytesRead+pnSize])
		bytesRead += pnSize
		buckets[i] = pn
	}
	page.Put()
	indexPager.Close()
	return &HashTable{depth: depth, buckets: buckets, pager: bucketPager}, nil
}

// Write hash table out to memory.
func WriteHashTable(bucketPager *pager.Pager, table *HashTable) error {
	if bucketPager.HasFile() {
		indexPager := pager.NewPager()
		err := indexPager.Open(bucketPager.GetFileName() + ".meta")
		if err != nil {
			return err
		}
		metaPN := indexPager.GetFreePN()
		page, err := indexPager.GetPage(metaPN)
		if err != nil {
			return err
		}
		page.SetDirty(true)
		// Write global depth to meta file
		depthData := make([]byte, DEPTH_SIZE)
		binary.PutVarint(depthData, table.depth)
		page.Update(depthData, DEPTH_OFFSET, DEPTH_SIZE)
		bytesWritten := DEPTH_SIZE
		// Write bucket index to meta file
		pnSize := int64(binary.MaxVarintLen64)
		pnData := make([]byte, pnSize)
		for _, pn := range table.buckets {
			if bytesWritten+pnSize > PAGESIZE {
				page.Put()
				metaPN = indexPager.GetFreePN()
				page, err = indexPager.GetPage(metaPN)
				if err != nil {
					return err
				}
				page.SetDirty(true)
				bytesWritten = 0
			}
			binary.PutVarint(pnData, pn)
			page.Update(pnData, bytesWritten, pnSize)
			bytesWritten += pnSize
		}
		page.Put()
		indexPager.Close()
	}
	return bucketPager.Close()
}
