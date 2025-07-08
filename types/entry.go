package types

import "unsafe"

type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

func (e *Entry) GetSize() int {
	return len(e.Key) + len(e.Value) + int(unsafe.Sizeof(0)*2)
}

func NewEntry(key, value []byte, tombStone bool) Entry {
	return Entry{
		Key: key, Value: value, Tombstone: tombStone,
	}
}
