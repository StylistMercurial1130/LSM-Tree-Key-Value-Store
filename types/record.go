package types

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"unsafe"
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
func (e *Record) GetSize() int {
	return len(e.Key) + len(e.Value) + int(unsafe.Sizeof(0)*2)
}

func DecodeRecordsFromBuffer(bufferReader *bytes.Reader) ([]Record, error) {
	read := func(reader *bytes.Reader, len int) ([]byte, error) {
		buff := make([]byte, len)
		_, err := reader.Read(buff)

		if err == io.EOF {
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		return buff, nil
	}

	var records []Record
	for {
		s, err := read(bufferReader, 8)
		if s == nil && err == nil {
			break
		} else if err != nil {
			return nil, NewEngineError(BUFFER_READ_ERROR, err.Error())
		}

		keySize := binary.LittleEndian.Uint64(s)
		key, err := read(bufferReader, int(keySize))
		if key == nil && err == nil {
			break
		} else if err != nil {
			return nil, NewEngineError(BUFFER_READ_ERROR, err.Error())
		}

		s, err = read(bufferReader, 8)
		if s == nil && err == nil {
			break
		} else if err != nil {
			return nil, NewEngineError(BUFFER_READ_ERROR, err.Error())
		}

		valueSize := binary.LittleEndian.Uint64(s)
		value, err := read(bufferReader, int(valueSize))
		if value != nil && err != nil {
			break
		} else if err != nil {
			return nil, NewEngineError(BUFFER_READ_ERROR, err.Error())
		}

		t, err := read(bufferReader, 1)
		if t == nil && err == nil {
			break
		} else if err != nil {
			return nil, NewEngineError(BUFFER_READ_ERROR, err.Error())
		}

		var tombStone bool = false
		if t[0] == '1' {
			tombStone = true
		}

		records = append(records, NewRecord(key, value, tombStone))
	}

	return records, nil
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

	tombStone := false
	if tombStoneBuffer[0] == '1' {
		tombStone = true
	}

	return NewRecord(keyBuffer, valBuffer, tombStone), nil
}

type Element struct {
	Entry Record
	Index int
}

type ElementHeap []Element

func InitHeap(elements []Element) *ElementHeap {
	minHeap := &ElementHeap{}
	*minHeap = elements
	heap.Init(minHeap)

	return minHeap
}

func (h *ElementHeap) Len() int {
	return len(*h)
}

func (h *ElementHeap) Less(i, j int) bool {
	return bytes.Compare((*h)[i].Entry.Key, (*h)[j].Entry.Key) == -1 && (*h)[i].Index == (*h)[j].Index ||
		bytes.Equal((*h)[i].Entry.Key, (*h)[j].Entry.Key) && (*h)[i].Index < (*h)[j].Index
}

func (h ElementHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ElementHeap) Push(x any) {
	*h = append(*h, x.(Element))
}

func (h *ElementHeap) Pop() any {
	curr := *h
	n := len(curr)
	e := curr[n-1]
	*h = curr[0 : n-1]
	return e
}
