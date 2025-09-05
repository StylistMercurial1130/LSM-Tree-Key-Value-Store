package disk

import (
	"LsmStorageEngine/types"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
	indexSize := 0
	for index, entry := range d.entries {
		tableIndex.lookUpTable[index] = indexRecord{
			key:    entry.Key,
			offset: offset,
		}

		offset += int(unsafe.Sizeof(0))*2 + len(entry.Key) + len(entry.Value) + 1
		indexSize += int(unsafe.Sizeof(0))*2 + len(entry.Key)
	}

	tableIndex.tableIndexsize = indexSize

	return tableIndex
}

func NewIndexBlockFromFile(file *os.File, length int) (TableIndex, error) {
	indexBuffer := make([]byte, length)
	_, err := io.ReadFull(file, indexBuffer)

	if err != nil && err != io.EOF {
		return TableIndex{}, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("error reading file : %s", err.Error()),
		)
	}

	reader := bytes.NewReader(indexBuffer)

	return NewIndexBlockFromBuffer(reader)
}

func NewIndexBlockFromBuffer(buffer *bytes.Reader) (TableIndex, error) {
	var indexRecords []indexRecord
	offset := 0

	for {
		sizeBuffer := make([]byte, unsafe.Sizeof(0))

		_, err := buffer.Read(sizeBuffer)

		if err != nil {
			if err == io.EOF {
				break
			}

			return TableIndex{}, types.NewEngineError(
				types.INDEX_BLOCK_DECODE_ERROR,
				fmt.Sprintf("error decoding index block key size : %s", err.Error()),
			)
		}

		keySize := binary.LittleEndian.Uint64(sizeBuffer)
		key := make([]byte, keySize)

		_, err = buffer.Read(key)

		if err != nil {
			return TableIndex{}, types.NewEngineError(
				types.INDEX_BLOCK_DECODE_ERROR,
				fmt.Sprintf("error decoding index block key : %s", err.Error()),
			)
		}

		_, err = buffer.Read(sizeBuffer)

		if err != nil {
			return TableIndex{}, types.NewEngineError(
				types.INDEX_BLOCK_DECODE_ERROR,
				fmt.Sprintf("error decoding index block offset point : %s", err.Error()),
			)
		}

		offsetPoint := binary.LittleEndian.Uint64(sizeBuffer)

		indexRecords = append(indexRecords, indexRecord{
			key:    key,
			offset: int(offsetPoint),
		})

		offset += int(unsafe.Sizeof(0))*2 + len(key)
	}

	return TableIndex{lookUpTable: indexRecords, tableIndexsize: offset}, nil
}

func (ti *TableIndex) Encode() []byte {
	var buffer []byte

	for _, record := range ti.lookUpTable {
		var indexField []byte

		keySize := make([]byte, unsafe.Sizeof(0))
		binary.LittleEndian.PutUint64(keySize, uint64(len(record.key)))
		indexField = append(indexField, keySize...)

		indexField = append(indexField, record.key...)

		dataOffsetPoint := make([]byte, unsafe.Sizeof(0))
		binary.LittleEndian.PutUint64(dataOffsetPoint, uint64(record.offset))
		indexField = append(indexField, dataOffsetPoint...)

		buffer = append(buffer, indexField...)
	}

	return buffer
}

func (ti *TableIndex) lookUpKeyOffset(key []byte) (int, bool) {
	start := 0
	end := len(ti.lookUpTable) - 1

	for start < end {
		mid := start + ((end - start) / 2)
		compare := bytes.Compare(key, ti.lookUpTable[mid].key)

		switch compare {
		case 0:
			return ti.lookUpTable[mid].offset, true
		case -1:
			end = mid - 1
		case 1:
			start = mid + 1
		}
	}

	return -1, false
}
