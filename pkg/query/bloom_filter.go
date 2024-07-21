package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
	// hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	// first use the bitset pakcage to create a bitset
	newBitset := bitset.New(uint(size))
	// then create a BloomFilter struct
	newBloomFilter := BloomFilter{size: size, bits: newBitset}
	return &newBloomFilter
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	// hash the key twice with hash.XxHasher and hash.MurmurHasher
	h1 := hash.XxHasher(key, filter.size)
	h2 := hash.MurmurHasher(key, filter.size)
	// use the bitset package to set the bits at the two hash values
	filter.bits.Set(uint(h1))
	filter.bits.Set(uint(h2))
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	h1 := hash.XxHasher(key, filter.size)
	h2 := hash.MurmurHasher(key, filter.size)

	// use the bitset package to check if the bits at the two hash values are set
	return filter.bits.Test(uint(h1)) && filter.bits.Test(uint(h2))
}
