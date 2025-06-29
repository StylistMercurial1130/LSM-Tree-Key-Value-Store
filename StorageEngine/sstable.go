package storageengine

import (
	"LsmStorageEngine/types"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"
)

type indexRecord struct {
	key    []byte
	offset int
}

type indexTable []indexRecord

func loadIndexTable(f *bufio.Reader, indexSize int) *indexTable {
	indexBinaryBuffer := make([]byte, indexSize)
	if _, err := f.Read(indexBinaryBuffer); err != nil {
		panic(fmt.Sprintf("Could not read index bytes !"))
	}

	/*
		format of key value pair in file
		<key size><key><offset>
	*/
	start := 0
	var lookUp indexTable
	for start < len(indexBinaryBuffer) {
		keySize := binary.LittleEndian.Uint64(indexBinaryBuffer[start:8])
		start += 8

		key := indexBinaryBuffer[start : start+int(keySize)]
		start += int(keySize)

		offset := int(binary.LittleEndian.Uint64(indexBinaryBuffer[start:8]))
		start += 8

		lookUp = append(lookUp, indexRecord{
			key: key, offset: offset,
		})
	}

	return &lookUp
}

func (idxTable *indexTable) get(key []byte) (*indexRecord, bool) {
	start := 0
	end := len(*idxTable) - 1

	for start < end {
		mid := start + ((end - start) / 2)
		compare := bytes.Compare(key, (*idxTable)[mid].key)

		if compare == 0 {
			return &(*idxTable)[mid], true
		} else if compare == -1 {
			end = mid - 1
		} else if compare == 1 {
			start = mid + 1
		}
	}

	return nil, false
}

type SSTable struct {
	file        *os.File // file handle
	lookUpTable *indexTable
}

const (
	KEY_DOES_NOT_EXIST = 1
	SSTABLE_SEEK_ERROR = 2
	SSTBALE_READ_ERROR = 3
)

type SSTableError struct {
	errCode int
	msg     string
}

func (err *SSTableError) Error() string {
	return fmt.Sprintf("sstable error : %s", err.msg)
}

func LoadSSTable(sstableFilePath string) *SSTable {
	f, err := os.Open(sstableFilePath)

	if err != nil {
		panic(fmt.Sprintf("could not open file %s : %s", sstableFilePath, err.Error()))
	}

	reader := bufio.NewReader(f)
	var header [unsafe.Sizeof(0) * 2]byte
	if _, err := reader.Read(header[:]); err != nil {
		panic(fmt.Sprintf("could not read header from file %s : %s", sstableFilePath, err.Error()))
	}

	indexSize := int(binary.LittleEndian.Uint64(header[:8]))
	if _, err := reader.Discard(indexSize); err != nil {
		panic(fmt.Sprintf("could not discard index size from file %s : %s", sstableFilePath, err.Error()))
	}

	return &SSTable{
		file:        f,
		lookUpTable: loadIndexTable(reader, indexSize),
	}
}

func (s *SSTable) Close() {
	if err := s.file.Close(); err != nil {
		panic(fmt.Sprintf("could not close file : %s", err.Error()))
	}
	s.file = nil
}

func extractKey(reader *bufio.Reader) ([]byte, error) {
	arr := make([]byte, 8)
	if _, err := reader.Read(arr); err != nil {
		return []byte{}, &SSTableError{
			errCode: SSTBALE_READ_ERROR,
			msg:     fmt.Sprintf("sstable key size reading error read error : %s", err.Error()),
		}
	}

	keySize := binary.LittleEndian.Uint64(arr)
	sstableKey := make([]byte, keySize)
	if _, err := reader.Read(sstableKey); err != nil {
		return []byte{}, &SSTableError{
			errCode: SSTBALE_READ_ERROR,
			msg:     fmt.Sprintf("sstable key read error : %s", err.Error()),
		}
	}

	return sstableKey, nil
}

func extractValue(reader *bufio.Reader) ([]byte, error) {
	arr := make([]byte, 8)
	if _, err := reader.Read(arr); err != nil {
		return []byte{}, &SSTableError{
			errCode: SSTBALE_READ_ERROR,
			msg:     fmt.Sprintf("sstable value size reading error read error : %s", err.Error()),
		}
	}

	valueSize := binary.LittleEndian.Uint64(arr)
	sstableValue := make([]byte, valueSize)
	if _, err := reader.Read(sstableValue); err != nil {
		return []byte{}, &SSTableError{
			errCode: SSTBALE_READ_ERROR,
			msg:     fmt.Sprintf("sstable value reading error read error : %s", err.Error()),
		}
	}

	return sstableValue, nil
}

func extractTombstome(reader *bufio.Reader) (bool, error) {
	var tombStone bool = false
	if tombStoneByte, err := reader.ReadByte(); err != nil {
		return tombStone, &SSTableError{
			errCode: SSTBALE_READ_ERROR,
			msg:     fmt.Sprintf("sstable tombstone reading error read error : %s", err.Error()),
		}

	} else {
		if tombStoneByte == 1 {
			tombStone = true
		} else {
			tombStone = false
		}
	}

	return tombStone, nil
}

func extractRecord(file *os.File, offset int) (types.Record, error) {
	_, err := file.Seek(int64(offset), 0)

	if err != nil {
		return types.Record{}, &SSTableError{
			errCode: SSTABLE_SEEK_ERROR,
			msg:     fmt.Sprintf("sstable seek error : %s", err.Error()),
		}
	}

	reader := bufio.NewReader(file)

	var key, value []byte
	var tombStone bool

	if key, err = extractKey(reader); err != nil {
		return types.Record{}, err
	}

	if value, err = extractValue(reader); err != nil {
		return types.Record{}, err
	}

	if tombStone, err = extractTombstome(reader); err != nil {
		return types.Record{}, nil
	}

	return types.NewRecord(key, value, tombStone), nil
}

func (s *SSTable) get(key []byte) (types.Record, error) {
	if record, exists := s.lookUpTable.get(key); !exists {
		return types.Record{}, &SSTableError{
			errCode: KEY_DOES_NOT_EXIST,
			msg:     fmt.Sprintf("key %s does not exist !", key),
		}
	} else {
		return extractRecord(s.file, record.offset)
	}
}
