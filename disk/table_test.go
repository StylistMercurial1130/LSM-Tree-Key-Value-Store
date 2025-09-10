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

func TestTableGet(t *testing.T) {
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

	rec, err := table.get([]byte("k3"))

	if err != nil {
		t.Errorf("test failed due table get error : %s", err.Error())
	}

	assert.Equal(t, rec, types.NewRecord(toBytes("k3"), toBytes("v3"), false))

	_, err = table.get([]byte("k5"))
	assert.Error(t, err)

	engineError := err.(*types.EngineError)
	assert.Equal(t, engineError.GetErrorCode(), types.TABLE_KEY_SEARCH_NOT_FOUND)

	err = os.RemoveAll(dataDir)

	if err != nil {
		t.Errorf("test failed due to data dir deleting error : %s", err.Error())
	}
}

func TestGetBoundaries(t *testing.T) {
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

	start, end := table.GetBoundaries()

	assert.Equal(t, start, toBytes("k1"))
	assert.Equal(t, end, toBytes("k4"))
}
