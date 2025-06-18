package disk

import (
	"LsmStorageEngine/memtable"
	"LsmStorageEngine/types"
	"encoding/binary"

	"github.com/google/uuid"
)

/*
	immutable table is the sstable structure intermeditiary structure
*/ 
type ImmutableTable struct {	
	tableFilePath string // file path
	data		  []types.Record // sorted records
	index		  [][]byte  // lookup for records
}

func (t *ImmutableTable) Flush(m *memtable.Memtable) {
	records := m.GetAll()	

	var index [][]byte
	if records != nil && len(records) != 0 {
		offset := 0
		for _,record := range records {
			var indexField []byte

			buffer := make([]byte,8)
			binary.LittleEndian.PutUint64(buffer,uint64(len(record.Key)))	
			indexField = append(indexField,buffer...)

			indexField = append(indexField, record.Key...)

			buffer = buffer[:0]
			binary.LittleEndian.PutUint64(buffer,uint64(offset))
			indexField = append(indexField,buffer...)
			offset += len(record.Key)

			index = append(index, indexField)
		}
	}
	
	t.tableFilePath = "sstable" + "_" + uuid.NewString() + ".data"
	t.data = records
	t.index = index
}

func (t *ImmutableTable) FlushToDisk() {

}

