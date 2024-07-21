package utils

// Interface for an entry in a table.
type Entry interface {
	GetKey() int64
	GetValue() int64
	Marshal() []byte
}

// Interface for a cursor that traverses a table.
type Cursor interface {
	StepForward() bool
	IsEnd() bool
	GetEntry() (Entry, error)
}
