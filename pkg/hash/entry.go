package hash

import (
	"encoding/binary"
	"fmt"
	"io"
)

// HashEntry is a single entry in a hashtable. Implements utils.Entry.
type HashEntry struct {
	key   int64
	value int64
}

// Get key.
func (entry HashEntry) GetKey() int64 {
	return entry.key
}

// Get value.
func (entry HashEntry) GetValue() int64 {
	return entry.value
}

// Set key.
func (entry *HashEntry) SetKey(key int64) {
	entry.key = key
}

// Set value.
func (entry *HashEntry) SetValue(value int64) {
	entry.value = value
}

// marshal serializes a given entry into a byte array.
func (entry HashEntry) Marshal() []byte {
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
func unmarshalEntry(data []byte) (entry HashEntry) {
	k, _ := binary.Varint(data[:len(data)/2])
	v, _ := binary.Varint(data[len(data)/2:])
	return HashEntry{key: k, value: v}
}

// Print this entry.
func (entry HashEntry) Print(w io.Writer) {
	io.WriteString(w, fmt.Sprintf("(%d, %d), ",
		entry.GetKey(), entry.GetValue()))
}
