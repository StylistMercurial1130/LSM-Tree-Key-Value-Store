package disk

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

type indexRecord struct {
	key    []byte
	offset int
}

type TableIndex struct {
	lookUpTable    []indexRecord
	tableIndexsize int
}

func NewIndexBlock(d *Data) *TableIndex {
	tableIndex := &TableIndex{
		lookUpTable: make([]indexRecord, len(d.entries)),
	}

	offset := 0
	for _, entry := range d.entries {
		tableIndex.lookUpTable = append(tableIndex.lookUpTable, indexRecord{
			key:    entry.Key,
			offset: offset,
		})

		offset += int(unsafe.Sizeof(0))*2 + len(entry.Key) + len(entry.Value) + 1
	}
	tableIndex.tableIndexsize = offset

	return tableIndex
}

func (ti *TableIndex) Encode() []byte {
	var buffer []byte

	for _, record := range ti.lookUpTable {
		var indexField []byte

		var keySize []byte
		binary.LittleEndian.AppendUint64(keySize, uint64(len(record.key)))
		indexField = append(indexField, keySize...)

		indexField = append(indexField, record.key...)

		var dataOffsetPoint []byte
		binary.LittleEndian.AppendUint64(dataOffsetPoint, uint64(record.offset))
		indexField = append(indexField, dataOffsetPoint...)

		buffer = append(buffer, indexField...)
	}

	return buffer
}

func (ti *TableIndex) lookUpKeyOffset(key []byte) (int, bool) {
	start := 0
	end := len(*&ti.lookUpTable) - 1

	for start < end {
		mid := start + ((end - start) / 2)
		compare := bytes.Compare(key, (*&ti.lookUpTable)[mid].key)

		if compare == 0 {
			return (*&ti.lookUpTable)[mid].offset, true
		} else if compare == -1 {
			end = mid - 1
		} else if compare == 1 {
			start = mid + 1
		}
	}

	return -1, false
}
