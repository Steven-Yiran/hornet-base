package btree

import (
	"encoding/binary"
)

// Global size for Entries.
var ENTRYSIZE int64 = binary.MaxVarintLen64 * 2

// Entry is a struct of one unit of information in our table.
type BTreeEntry struct {
	key   int64
	value int64
}

// Get key.
func (entry BTreeEntry) GetKey() int64 {
	return entry.key
}

// Get value.
func (entry BTreeEntry) GetValue() int64 {
	return entry.value
}

// Set key.
func (entry *BTreeEntry) SetKey(key int64) {
	entry.key = key
}

// Set value.
func (entry *BTreeEntry) SetValue(value int64) {
	entry.value = value
}

// Marshal serializes a given entry into a byte array.
func (entry BTreeEntry) Marshal() []byte {
	// Marshall the key field.
	var newdata []byte
	bin := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(bin, entry.GetKey())
	newdata = bin
	// Marshall the value field.
	bin = make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(bin, entry.GetValue())
	newdata = append(newdata, bin...)
	// Return the combined byte array.
	return newdata
}

// unmarshalEntry deserializes a byte array into an entry.
func unmarshalEntry(data []byte) (entry BTreeEntry) {
	k, _ := binary.Varint(data[:len(data)/2])
	v, _ := binary.Varint(data[len(data)/2:])
	return BTreeEntry{key: k, value: v}
}
