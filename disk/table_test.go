package disk

import (
	"LsmStorageEngine/types"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableCreation(t *testing.T) {
	dataDir := "./data"
	toBytes := func(str string) []byte { return []byte(str) }

	_, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataDir, 0755)

		if err != nil {
			t.Errorf("test failed due to data dir creation error : %s", err.Error())
		}
	}

	entries := []types.Record{
		types.NewRecord(toBytes("k1"), toBytes("v1"), false),
		types.NewRecord(toBytes("k2"), toBytes("v2"), false),
		types.NewRecord(toBytes("k3"), toBytes("v3"), false),
		types.NewRecord(toBytes("k4"), toBytes("v4"), false),
	}

	table, err := CreateNewTableToDisk(entries, dataDir)

	if err != nil {
		t.Errorf("test failed due table creation error : %s", err.Error())
	}

	tableEntries, err := table.getAllEntries()

	if err != nil {
		t.Errorf("test failed due error at table.getAllEntries() : %s", err.Error())
	}

	assert.EqualValues(t, entries, tableEntries)

	err = os.RemoveAll(dataDir)

	if err != nil {
		t.Errorf("test failed due to data dir deleting error : %s", err.Error())
	}
}
