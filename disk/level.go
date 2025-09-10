package disk

import (
	"LsmStorageEngine/types"
	"bytes"
	"fmt"
)

type Level struct {
	tables []*Table
}

func (l *Level) get(index int) (*Table, error) {
	if index > l.size() || index < 0 {
		return nil, types.NewEngineError(
			types.LEVEL_GET_ERROR,
			fmt.Sprintf("could not find table with index %d, its out of bounds", index),
		)
	}

	return l.tables[index], nil
}

func (l *Level) ScanAllTables(key []byte) (types.Record, error) {
	searchStatus := false
	var record types.Record

	for _, table := range l.tables {
		if r, err := table.get(key); err == nil {
			record = r
			searchStatus = true
			break
		} else if err.(*types.EngineError).GetErrorCode() == types.BIT_VECTOR_SEARCH_ERROR {
			return types.Record{}, err
		}
	}

	if !searchStatus {
		return types.Record{}, types.NewEngineError(
			types.TABLE_KEY_SEARCH_NOT_FOUND,
			fmt.Sprintf("could not find key %s", key),
		)
	}

	return record, nil
}

func (l *Level) getRange(start, end int) ([]*Table, error) {
	if start < 0 || end > len(l.tables) {
		return nil, types.NewEngineError(
			types.LEVEL_GET_RANGE_OUT_OF_BOUNDS_ERROR,
			fmt.Sprintf("tables array range (0,%d) is out bounds w.r.t (%d,%d)", len(l.tables), start, end),
		)
	}

	return l.tables[start:end], nil
}

func (l *Level) push(table *Table) {
	l.tables = append(l.tables, table)
}

func (l *Level) delete(comparator func(table *Table) bool) error {
	var newTables []*Table

	for _, table := range l.tables {
		if !comparator(table) {
			newTables = append(newTables, table)
		} else {
			err := table.Delete()

			if err != nil {
				return err
			}
		}
	}

	l.tables = newTables

	return nil
}

func (l *Level) size() int {
	return len(l.tables)
}

func getOverlap(l *Level, start, end []byte) []*Table {
	var overlappingTables []*Table
	for _, table := range l.tables {
		startKey := table.indexBlock.lookUpTable[0].key
		endKey := table.indexBlock.lookUpTable[len(table.indexBlock.lookUpTable)-1].key

		if bytes.Compare(startKey, end) == -1 && bytes.Compare(endKey, start) == -1 {
			overlappingTables = append(overlappingTables, table)
		}
	}

	return overlappingTables
}

func (l *Level) getOverlappingTables() ([]*Table, []byte, []byte) {
	if len(l.tables) == 0 {
		return nil, nil, nil
	}

	start := l.tables[0].indexBlock.lookUpTable[0].key
	end := l.tables[0].indexBlock.lookUpTable[len(l.tables[0].indexBlock.lookUpTable)-1].key

	return getOverlap(l, start, end), start, end
}

func (l *Level) getOverlappingTablesInRange(start, end []byte) []*Table {
	if len(l.tables) == 0 {
		return nil
	}

	return getOverlap(l, start, end)
}
