package disk

import (
	"LsmStorageEngine/types"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
)

type Table struct {
	indexBlock *TableIndex
	filePath   string
	fd         *os.File
	metaData   MetaData
}

type MetaData struct {
	indexBlockSize int
	dataBlockSize  int
	level          int
}

func CreateNewTableToDisk(entries []types.Entry,dir string) (*Table,error) {
	tableIndex, metaData, tableContent := Flush(entries)	
	fileName := fmt.Sprintf("%s//L%d_%s.data",dir,metaData.level,uuid.New().String())	

	fd, err := os.Create(fileName)
	if err != nil {
		return nil,types.NewEngineError(
			types.TABLE_FILE_CREATION_ERROR,
			fmt.Sprintf("table file creation error : %s",err.Error()),
		)
	}

	fd.Write(tableContent)

	return &Table {
		indexBlock: tableIndex,
		filePath: fileName,
		fd: fd,
		metaData: metaData,
	},nil
}

func Flush(entries []types.Entry) (*TableIndex, MetaData, []byte) {
	dataBlock := NewDataBlock(entries)
	indexBlock := NewIndexBlock(dataBlock)

	metaData := MetaData{
		indexBlockSize: indexBlock.tableIndexsize,
		dataBlockSize:  dataBlock.dataBlockSize,
		level: 0,
	}

	var buffer []byte

	var s []byte
	buffer = append(buffer, binary.LittleEndian.AppendUint64(s, uint64(metaData.indexBlockSize))...)
	s = s[0:]
	buffer = append(buffer, binary.LittleEndian.AppendUint64(s, uint64(metaData.dataBlockSize))...)
	s = s[0:]
	buffer = append(buffer,binary.LittleEndian.AppendUint64(s,uint64(metaData.level))...)
	buffer = append(append(buffer, indexBlock.Encode()...), dataBlock.Encode()...)

	return indexBlock, metaData, buffer
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

	_, err := t.fd.Seek(int64(t.metaData.indexBlockSize+tableFileOffset), io.SeekStart)
	defer t.fd.Seek(0, io.SeekStart)

	if err != nil {
		return types.Record{}, types.NewEngineError(
			types.TABLE_KEY_FILE_SEEK_ERR,
			fmt.Sprintf("index block seeek error : %s", err.Error()),
		)
	}

	record, err := types.DecocodeRecordFromFile(t.fd)

	if err != nil {
		return types.Record{}, types.NewEngineError(
			types.TABLE_RECORD_READ_ERROR,
			fmt.Sprintf("record decode error : %s", err.Error()),
		)
	}

	return record, nil
}
