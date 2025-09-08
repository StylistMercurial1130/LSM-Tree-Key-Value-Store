package disk

import (
	"LsmStorageEngine/types"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/google/uuid"
)

/*
TODO : feed these values as config instead  of constants
*/
const (
	m = 10000
	p = 0.01
)

type Table struct {
	indexBlock *TableIndex
	boolFilter *BloomFilter
	filePath   string
	metaData   MetaData
}

type MetaData struct {
	indexBlockSize  int
	dataBlockSize   int
	bloomFilterSize int
	level           int
}

func ReadMetaDataFromFile(r io.Reader) (MetaData, error) {
	var indexBlockSize int
	var dataBlockSize int
	var bloomFilterSize int
	var level int

	err := binary.Read(r, binary.LittleEndian, &indexBlockSize)

	if err != nil {
		return MetaData{}, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("invalid file passed !"),
		)
	}

	err = binary.Read(r, binary.LittleEndian, &indexBlockSize)

	if err != nil {
		return MetaData{}, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("invalid file passed !"),
		)
	}

	err = binary.Read(r, binary.LittleEndian, &dataBlockSize)

	if err != nil {
		return MetaData{}, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("invalid file passed !"),
		)
	}

	err = binary.Read(r, binary.LittleEndian, &bloomFilterSize)

	if err != nil {
		return MetaData{}, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("invalid file passed !"),
		)
	}

	err = binary.Read(r, binary.LittleEndian, &level)

	if err != nil {
		return MetaData{}, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("invalid file passed !"),
		)
	}

	return MetaData{
		indexBlockSize:  indexBlockSize,
		dataBlockSize:   dataBlockSize,
		bloomFilterSize: bloomFilterSize,
		level:           level,
	}, nil
}

func CreateNewTableToDisk(entries []types.Record, dir string) (*Table, error) {
	tableIndex, bloomFilter, metaData, tableContent := Flush(entries)
	fileName := fmt.Sprintf("%s//L%d_%s.data", dir, metaData.level, uuid.New().String())

	fd, err := os.Create(fileName)
	defer fd.Close()
	if err != nil {
		return nil, types.NewEngineError(
			types.TABLE_FILE_CREATION_ERROR,
			fmt.Sprintf("table file creation error : %s", err.Error()),
		)
	}

	fd.Write(tableContent)

	return &Table{
		indexBlock: tableIndex,
		boolFilter: bloomFilter,
		filePath:   fileName,
		metaData:   metaData,
	}, nil
}

func ReadTablesFromDisk(fileName string) (*Table, error) {
	fd, err := os.Open(fileName)

	if err != nil {
		return nil, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("table file read error : %s", err.Error()),
		)
	}

	metaData, err := ReadMetaDataFromFile(fd)

	if err != nil {
		return nil, err
	}

	indexBlock, err := NewIndexBlockFromFile(fd, metaData.indexBlockSize)

	if err != nil {
		return nil, err
	}

	bloomFilter, err := ReconstructBloomFilterFromFile(fd, m, p)

	if err != nil {
		return nil, err
	}

	return &Table{
		metaData:   metaData,
		boolFilter: &bloomFilter,
		indexBlock: &indexBlock,
		filePath:   fileName,
	}, nil
}

func Flush(entries []types.Record) (*TableIndex, *BloomFilter, MetaData, []byte) {
	dataBlock := NewDataBlock(entries)
	bloomFilter := NewBloomFilterFromEntries(m, p, entries)
	indexBlock := NewIndexBlock(dataBlock)

	metaData := MetaData{
		indexBlockSize:  indexBlock.tableIndexsize,
		dataBlockSize:   dataBlock.dataBlockSize,
		bloomFilterSize: bloomFilter.getBufferSize(),
		level:           0,
	}

	var buffer []byte

	var s []byte = make([]byte, 8)

	binary.LittleEndian.PutUint64(s, uint64(metaData.indexBlockSize))
	buffer = append(buffer, s...)

	binary.LittleEndian.PutUint64(s, uint64(metaData.bloomFilterSize))
	buffer = append(buffer, s...)

	binary.LittleEndian.PutUint64(s, uint64(metaData.dataBlockSize))
	buffer = append(buffer, s...)

	binary.LittleEndian.PutUint64(s, uint64(metaData.level))
	buffer = append(buffer, s...)

	buffer = append(append(append(buffer, bloomFilter.Serialize()...), indexBlock.Encode()...), dataBlock.Encode()...)

	return indexBlock, &bloomFilter, metaData, buffer
}

func (t *Table) get(key []byte) (types.Record, error) {
	// search index block
	// find key location through fd
	// read the entry
	tableFileOffset, found := t.indexBlock.lookUpKeyOffset(key)

	if !found {
		return types.Record{}, types.NewEngineError(
			types.TABLE_KEY_SEARCH_NOT_FOUND,
			"key not found in index",
		)
	}

	fd, err := os.OpenFile(t.filePath, os.O_RDONLY, os.FileMode(os.O_RDONLY))
	defer fd.Close()

	if err != nil {
		return types.Record{}, types.NewEngineError(
			types.TABLE_FILE_OPEN_ERROR,
			fmt.Sprintf("unable to open file %s : %s", t.filePath, err.Error()),
		)
	}

	skipLengths := int(unsafe.Sizeof(0))*4 + t.metaData.bloomFilterSize + t.metaData.indexBlockSize
	_, err = fd.Seek(int64(skipLengths+tableFileOffset), io.SeekStart)

	if err != nil {
		return types.Record{}, types.NewEngineError(
			types.TABLE_KEY_FILE_SEEK_ERR,
			fmt.Sprintf("index block seeek error : %s", err.Error()),
		)
	}

	record, err := types.DecocodeRecordFromFile(fd)

	if err != nil {
		return types.Record{}, types.NewEngineError(
			types.TABLE_RECORD_READ_ERROR,
			fmt.Sprintf("record decode error : %s", err.Error()),
		)
	}

	return record, nil
}

func (t *Table) getAllEntries() ([]types.Record, error) {
	fd, err := os.Open(t.filePath)
	defer fd.Close()

	if err != nil {
		return nil, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("file read error : %s", err.Error()),
		)
	}

	skipLength := int(unsafe.Sizeof(0))*4 + t.metaData.bloomFilterSize + t.metaData.indexBlockSize
	fd.Seek(int64(skipLength), io.SeekStart)

	dataBlockBuffer, err := io.ReadAll(fd)

	if err != nil {
		return nil, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("file read error : %s", err.Error()),
		)
	}

	records, err := types.DecodeRecordsFromBuffer(bytes.NewReader(dataBlockBuffer))
	if err != nil {
		return nil, types.NewEngineError(
			types.BUFFER_READ_ERROR,
			err.Error(),
		)
	}

	return records, nil
}

func (t *Table) GetBoundaries() ([]byte, []byte) {
	return t.indexBlock.lookUpTable[0].key, t.indexBlock.lookUpTable[len(t.indexBlock.lookUpTable)-1].key
}
