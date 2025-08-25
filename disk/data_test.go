package disk

import (
	"LsmStorageEngine/types"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataCreation(t *testing.T) {
	tests := map[string]struct {
		input  []types.Record
		output []types.Record
	}{
		"data block creation": {
			input: []types.Record{
				types.NewRecord([]byte("k1"), []byte("v1"), false),
				types.NewRecord([]byte("k2"), []byte("v2"), false),
			},
			output: []types.Record{
				types.NewRecord([]byte("k1"), []byte("v1"), false),
				types.NewRecord([]byte("k2"), []byte("v2"), false),
			},
		},
	}

	for testName, test := range tests {
		t.Logf("Running test case %s", testName)

		dataBlock := NewDataBlock(test.input)

		assert.ElementsMatch(t, dataBlock.entries, test.output)
	}
}

func TestEncode(t *testing.T) {
	tests := map[string]struct {
		input           []types.Record
		outputGenerator func([]byte) ([]types.Record, error)
	}{
		"data block encoding": {
			input: []types.Record{
				types.NewRecord([]byte("k1"), []byte("v1"), false),
				types.NewRecord([]byte("k2"), []byte("v2"), false),
			},
			outputGenerator: func(encodedRecord []byte) ([]types.Record, error) {
				reader := bytes.NewReader(encodedRecord)

				return types.DecodeRecordsFromBuffer(reader)
			},
		},
	}

	for testName, test := range tests {
		t.Logf("Running test case %s", testName)

		dataBlock := NewDataBlock(test.input)
		encodedBlock := dataBlock.Encode()

		output, err := test.outputGenerator(encodedBlock)

		if err != nil {
			t.Errorf("test failed due to error : %s", err.Error())
		} else {
			assert.ElementsMatch(t, output, test.input)
		}
	}
}
