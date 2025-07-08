package types

import (
	"encoding/binary"
	"fmt"
	"os"
)

type Record struct {
	Key       []byte
	Value     []byte
	TombStone bool
}

func NewRecord(key []byte, value []byte, tombStone bool) Record {
	return Record{
		Key: key, Value: value, TombStone: tombStone,
	}
}

func DecocodeRecordFromFile(fd *os.File) (Record, error) {
	// key size
	sizeBuf := make([]byte, 8)
	_, err := fd.Read(sizeBuf)

	if err != nil {
		return Record{}, NewEngineError(
			TABLE_KEY_FILE_SEEK_ERR,
			fmt.Sprintf("key size read file err : %s", err.Error()),
		)
	}

	keySize := binary.LittleEndian.Uint64(sizeBuf)

	keyBuffer := make([]byte, keySize)
	_, err = fd.Read(keyBuffer)

	if err != nil {
		return Record{}, NewEngineError(
			TABLE_KEY_FILE_SEEK_ERR,
			fmt.Sprintf("key read file err : %s", err.Error()),
		)
	}

	sizeBuf = sizeBuf[0:]

	_, err = fd.Read(sizeBuf)

	if err != nil {
		return Record{}, NewEngineError(
			TABLE_KEY_FILE_SEEK_ERR,
			fmt.Sprintf("value size read file err : %s", err.Error()),
		)
	}

	valBuffer := make([]byte, binary.LittleEndian.Uint64(sizeBuf))
	_, err = fd.Read(valBuffer)

	if err != nil {
		return Record{}, NewEngineError(
			TABLE_KEY_FILE_SEEK_ERR,
			fmt.Sprintf("value read file err : %s", err.Error()),
		)
	}

	tombStoneBuffer := make([]byte, 1)
	_, err = fd.Read(tombStoneBuffer)

	if err != nil {
		return Record{}, NewEngineError(
			TABLE_KEY_FILE_SEEK_ERR,
			fmt.Sprintf("tombstone read file err : %s", err.Error()),
		)
	}

	var tombStone bool = false
	if tombStoneBuffer[0] == '1' {
		tombStone = true
	}

	return NewRecord(keyBuffer, valBuffer, tombStone), nil
}
