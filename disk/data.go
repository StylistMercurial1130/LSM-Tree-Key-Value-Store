package disk

import (
	"LsmStorageEngine/types"
	"encoding/binary"
)

type Data struct {
	entries       []types.Record
	dataBlockSize int
}

func NewDataBlock(entries []types.Record) *Data {

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

	for _, entry := range d.entries {
		// key size
		var keyLenScratchPad []byte = make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLenScratchPad, uint64(len(entry.Key)))
		buffer = append(buffer, keyLenScratchPad...)

		// key
		buffer = append(buffer, entry.Key...)

		// value size
		var valueSizeScratchPad []byte = make([]byte, 8)
		binary.LittleEndian.PutUint64(valueSizeScratchPad, uint64(len(entry.Value)))
		buffer = append(buffer, valueSizeScratchPad...)

		// value
		buffer = append(buffer, entry.Value...)

		// tombstone
		var b byte
		if entry.TombStone {
			b = 1
		} else {
			b = 0
		}
		buffer = append(buffer, b)
	}

	return buffer
}
