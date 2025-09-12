package disk

import (
	"LsmStorageEngine/types"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const dataDir = "./data"

func generateTable(records []types.Record) (*Table, error) {
	return CreateNewTableToDisk(records, dataDir)
}

func toBytes(s string) []byte { return []byte(s) }

var data []struct {
	records   []types.Record
	generator func(r []types.Record) (*Table, error)
} = []struct {
	records   []types.Record
	generator func(r []types.Record) (*Table, error)
}{
	{
		records: []types.Record{
			types.NewRecord(toBytes("k1"), toBytes("v1"), false),
			types.NewRecord(toBytes("k2"), toBytes("v2"), false),
			types.NewRecord(toBytes("k3"), toBytes("v3"), false),
			types.NewRecord(toBytes("k4"), toBytes("v4"), false),
		},
		generator: generateTable,
	},
	{
		records: []types.Record{
			types.NewRecord(toBytes("k5"), toBytes("v5"), false),
			types.NewRecord(toBytes("k6"), toBytes("v6"), false),
			types.NewRecord(toBytes("k7"), toBytes("v7"), false),
			types.NewRecord(toBytes("k8"), toBytes("v8"), false),
		},
		generator: generateTable,
	},
	{
		records: []types.Record{
			types.NewRecord(toBytes("k9"), toBytes("v9"), false),
			types.NewRecord(toBytes("k10"), toBytes("v10"), false),
			types.NewRecord(toBytes("k11"), toBytes("v11"), false),
			types.NewRecord(toBytes("k12"), toBytes("v12"), false),
		},
		generator: generateTable,
	},
}

func TestGenerateLevel(t *testing.T) {
	level := Level{
		tables: make([]*Table, 0),
	}

	_, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataDir, 0755)

		if err != nil {
			t.Errorf("test failed due to data dir creation error : %s", err.Error())
		}
	}

	for _, d := range data {
		table, err := d.generator(d.records)

		if err != nil {
			t.Errorf("TestGenerateLevel failed due to : %s", err.Error())
			break
		}

		level.push(table)
	}

	assert.Equal(t, len(level.tables), 3)

	lastRecordSet := data[0]
	lastRecord := lastRecordSet.records[len(lastRecordSet.records)-1]

	lastTableEntries, err := level.tables[len(level.tables)-1].getAllEntries()

	if err != nil {
		t.Errorf("TestGenerateLevel failed due to : %s", err.Error())
		return
	}

	assert.Equal(t, lastRecord, lastTableEntries[len(lastTableEntries)-1])

	err = os.RemoveAll(dataDir)

	if err != nil {
		t.Errorf("test failed due to data dir deleting error : %s", err.Error())
	}
}

func TestGetKeyFromLevel(t *testing.T) {
	level := Level{
		tables: make([]*Table, 0),
	}

	_, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataDir, 0755)

		if err != nil {
			t.Errorf("test failed due to data dir creation error : %s", err.Error())
			return
		}
	}

	for _, d := range data {
		table, err := d.generator(d.records)

		if err != nil {
			t.Errorf("TestGenerateLevel failed due to : %s", err.Error())
			break
		}

		level.push(table)
	}

	record, err := level.ScanAllTables(toBytes("k5"))

	if err != nil {
		t.Errorf("test failed due to data dir creation error : %s", err.Error())
		return
	}

	assert.Equal(t, record.Key, toBytes("k5"))
	assert.Equal(t, record.Value, toBytes("v5"))
}

func TestDeleteLevelItems(t *testing.T) {
	level := Level{
		tables: make([]*Table, 0),
	}

	_, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataDir, 0755)

		if err != nil {
			t.Errorf("test failed due to data dir creation error : %s", err.Error())
			return
		}
	}

	for _, d := range data {
		table, err := d.generator(d.records)

		if err != nil {
			t.Errorf("TestGenerateLevel failed due to : %s", err.Error())
			break
		}

		level.push(table)
	}

	len := level.size()
	level.delete(func(table *Table) bool {
		_, err := table.get(toBytes("k6"))

		return err == nil
	})

	assert.Equal(t, level.size(), len-1)
}

func TestGetOverlap(t *testing.T) {
	level := Level{
		tables: make([]*Table, 0),
	}

	_, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataDir, 0755)

		if err != nil {
			t.Errorf("test failed due to data dir creation error : %s", err.Error())
			return
		}
	}

	for _, d := range data {
		table, err := d.generator(d.records)

		if err != nil {
			t.Errorf("TestGenerateLevel failed due to : %s", err.Error())
			break
		}

		level.push(table)
	}

	tables, start, end := level.GetOverlappingTables()

	assert.Equal(t, start, toBytes("k9"))
	assert.Equal(t, end, toBytes("k12"))
	assert.Equal(t, tables, []*Table{level.tables[0]})
}
