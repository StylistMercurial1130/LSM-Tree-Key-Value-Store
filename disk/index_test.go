package disk

import (
	"LsmStorageEngine/types"
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestIndexBlockCreation(t *testing.T) {
	testCases := map[string]struct {
		input  []types.Record
		output []indexRecord
	}{
		"index block creation test": {
			input: []types.Record{
				types.NewRecord([]byte("k1"), []byte("v1"), false),
				types.NewRecord([]byte("k2"), []byte("v2"), false),
			},
			output: []indexRecord{
				{key: []byte("k1"), offset: 0},
				{key: []byte("k2"), offset: 21},
			},
		},
	}

	for testCaseName, testCase := range testCases {
		t.Logf("running test case : %s", testCaseName)

		dataBlock := NewDataBlock(testCase.input)
		indexBlock := NewIndexBlock(dataBlock)

		assert.ElementsMatch(t, indexBlock.lookUpTable, testCase.output)
	}
}

func TestIndexBlockSearchAndGet(t *testing.T) {
	testCases := map[string]struct {
		dataBlockInput  []types.Record
		searchKey       []byte
		searchKeyOffset int
	}{
		"index block key offset search and get": {
			dataBlockInput: []types.Record{
				types.NewRecord([]byte("k1"), []byte("v1"), false),
				types.NewRecord([]byte("k2"), []byte("v1"), false),
				types.NewRecord([]byte("k3"), []byte("v3"), false),
				types.NewRecord([]byte("k4"), []byte("v4"), false),
			},
			searchKey:       []byte("k2"),
			searchKeyOffset: 21,
		},
	}

	for testCaseName, testCase := range testCases {
		t.Logf("running test case : %s", testCaseName)

		dataBlock := NewDataBlock(testCase.dataBlockInput)
		indexBlock := NewIndexBlock(dataBlock)

		offset, found := indexBlock.lookUpKeyOffset(testCase.searchKey)

		if !found {
			t.Errorf("%s key not found in index block", string(testCase.searchKey))
		} else {
			if !assert.Equal(t, testCase.searchKeyOffset, offset) {
				t.Errorf("test failed !, offset not equal")
				return
			}

			encodedDataBlock := dataBlock.Encode()

			dataBlockReader := bytes.NewReader(encodedDataBlock)

			dataBlockReader.Seek(int64(offset), io.SeekStart)

			sizeBuffer := make([]byte, unsafe.Sizeof(0))
			if _, err := dataBlockReader.Read(sizeBuffer); err == nil {
				keySize := binary.LittleEndian.Uint64(sizeBuffer)
				key := make([]byte, keySize)

				if _, err = dataBlockReader.Read(key); err == nil {
					if _, err = dataBlockReader.Read(sizeBuffer); err == nil {
						valueSize := binary.LittleEndian.Uint64(sizeBuffer)
						value := make([]byte, valueSize)

						if _, err = dataBlockReader.Read(value); err == nil {
							tombStoneByte, err := dataBlockReader.ReadByte()

							if err != nil {
								t.Errorf("test failed: error : %s", err.Error())
							}

							tombStone := false

							if tombStoneByte == '1' {
								tombStone = true
							}

							record := types.NewRecord(key, value, tombStone)

							assert.Equal(t, testCase.dataBlockInput[1], record)
						} else {
							t.Errorf("test failed: error : %s", err.Error())
						}
					} else {
						t.Errorf("test failed, error : %s", err.Error())
					}
				} else {
					t.Errorf("test failed, error : %s", err.Error())
				}
			} else {
				t.Errorf("test failed, error : %s", err.Error())
			}
		}
	}
}

func TestIndexBlockEncode(t *testing.T) {
	testCases := map[string]struct {
		dataBlockInput  []types.Record
		outputGenerator func(encodedIndexBlock []byte) (TableIndex, error)
	}{
		"index block key encode": {
			dataBlockInput: []types.Record{
				types.NewRecord([]byte("k1"), []byte("v1"), false),
				types.NewRecord([]byte("k2"), []byte("v1"), false),
				types.NewRecord([]byte("k3"), []byte("v3"), false),
				types.NewRecord([]byte("k4"), []byte("v4"), false),
			},
			outputGenerator: func(encodedIndexBlock []byte) (TableIndex, error) {
				return NewIndexBlockFromBuffer(bytes.NewReader(encodedIndexBlock))
			},
		},
	}

	for testCaseName, testCase := range testCases {
		t.Logf("running test case : %s", testCaseName)

		dataBlock := NewDataBlock(testCase.dataBlockInput)
		indexBlock := NewIndexBlock(dataBlock)
		encodedIndexBlock := indexBlock.Encode()

		_indexBlock, err := testCase.outputGenerator(encodedIndexBlock)

		if err != nil {
			t.Errorf("error executing test case : %s", err.Error())
			return
		}

		assert.Equal(t, _indexBlock.lookUpTable, indexBlock.lookUpTable)
		assert.Equal(t, _indexBlock.tableIndexsize, indexBlock.tableIndexsize)
	}
}
