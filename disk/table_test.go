package disk

import (
	"LsmStorageEngine/types"
	"testing"
)

func TestTableCreation(t *testing.T) {
	entries := []types.Record{
		types.NewRecord([]byte("k1"), []byte("v1"), false),
		types.NewRecord([]byte("k2"), []byte("v2"), false),
		types.NewRecord([]byte("k3"), []byte("v3"), false),
	}

	CreateNewTableToDisk(entries, "./")
}
