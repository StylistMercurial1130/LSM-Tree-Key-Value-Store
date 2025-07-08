package disk

import (
	"LsmStorageEngine/types"
	"encoding/binary"
)

type Data struct {
	entries       []types.Entry
	dataBlockSize int
}

func NewDataBlock(entries []types.Entry) *Data {

	totalBlockSize := 0
	for _, entry := range entries {
		totalBlockSize += entry.GetSize()
	}

	return &Data{
		entries:       entries,
		dataBlockSize: totalBlockSize,
	}
}

func (d *Data) Encode() []byte {
	var buffer []byte

	var scratchPad []byte
	for _, entry := range d.entries {
		// key size
		binary.LittleEndian.PutUint64(scratchPad, uint64(len(entry.Key)))
		buffer = append(buffer, scratchPad...)
		scratchPad = scratchPad[:0]

		// key
		buffer = append(buffer, entry.Key...)

		// value size
		binary.LittleEndian.PutUint64(scratchPad, uint64(len(entry.Value)))
		buffer = append(buffer, scratchPad...)
		scratchPad = scratchPad[:0]

		// value
		buffer = append(buffer, entry.Value...)

		// tombstone
		var b byte
		if entry.Tombstone {
			b = 1
		} else {
			b = 0
		}
		buffer = append(buffer, b)
	}

	return buffer
}
