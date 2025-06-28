package storageengine

import (
	"LsmStorageEngine/types"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"github.com/google/uuid"
)

/*
immutable table is the sstable structure intermeditiary structure
*/
type ImmutableTable struct {
	tableFilePath string         // file path
	data          []types.Record // sorted records
	index         [][]byte       // lookup for records
	indexSize     int
	recordsSize   int
}

func (t *ImmutableTable) Flush(m *Memtable) {
	records := m.GetAll()

	var index [][]byte
	if records != nil && len(records) != 0 {
		offset := 0
		recordSize := 0
		for _, record := range records {
			var indexField []byte

			buffer := make([]byte, 8)
			binary.LittleEndian.PutUint64(buffer, uint64(len(record.Key)))
			indexField = append(indexField, buffer...)

			indexField = append(indexField, record.Key...)

			buffer = buffer[:0]
			binary.LittleEndian.PutUint64(buffer, uint64(offset))
			indexField = append(indexField, buffer...)
			recordSize += int(unsafe.Sizeof(0))*2 + len(record.Key) + len(record.Value) + 1
			offset += int(unsafe.Sizeof(0)) + len(record.Key) + int(unsafe.Sizeof(recordSize))

			index = append(index, indexField)
		}
		t.indexSize = offset
		t.recordsSize = recordSize
	}

	t.tableFilePath = "sstable" + "_" + uuid.NewString() + ".data"
	t.data = records
	t.index = index
}

/*
flush the immtable table into the disk where the file loadded is sstable_<random_uuid>.data file

the first few bytes (as of writting this right now, don't really know how much) is going to be the metadata
the metadata includes
 1. The size of the index
 2. The size of the records set

this should allow offsetting right to the records or to the index convinently
THIS MAY CHANGE!
*/
func (t *ImmutableTable) FlushToDisk() {
	sstableFile, err := os.Create(t.tableFilePath)

	if err != nil {
		panic(fmt.Sprint("error creating the file %s : %s", t.tableFilePath, err.Error()))
	}

	defer sstableFile.Close()

	writter := bufio.NewWriter(sstableFile)

	var buffer []byte
	binary.LittleEndian.PutUint64(buffer, uint64(t.indexSize))
	binary.LittleEndian.AppendUint64(buffer, uint64(t.recordsSize))

	writter.Write(buffer)
	buffer = buffer[:0]

	for _, indexField := range t.index {
		writter.Write(indexField)
	}

	/*
		format of key value pair in file
		<key size><key><value size><value><tombstone>
	*/
	var scratchPad []byte
	for _, record := range t.data {
		// key size
		binary.LittleEndian.PutUint64(scratchPad, uint64(len(record.Key)))
		buffer = append(buffer, scratchPad...)
		scratchPad = scratchPad[:0]

		// key
		buffer = append(buffer, record.Key...)

		// value size
		binary.LittleEndian.PutUint64(scratchPad, uint64(len(record.Value)))
		buffer = append(buffer, scratchPad...)
		scratchPad = scratchPad[:0]

		// value
		buffer = append(buffer, record.Value...)

		// tombstone
		var b byte
		if record.TombStone {
			b = 1
		} else {
			b = 0
		}
		buffer = append(buffer, b)

		writter.Write(buffer)
		buffer = buffer[:0]
	}

	writter.Flush()
}
