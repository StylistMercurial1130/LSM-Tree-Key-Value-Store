package disk

import (
	"LsmStorageEngine/types"
	"fmt"
	"math"
	"strings"
	"sync"
)

type DiskManager struct {
	levels     []*Level
	levelRatio int
	l0Target   int
	dir        string
	mu         sync.RWMutex
}

func CreateDiskManager(levelRatio int, l0Target int, dir string) *DiskManager {
	return &DiskManager{
		levels:     make([]*Level, 1),
		levelRatio: levelRatio,
		l0Target:   l0Target,
		dir:        dir,
	}
}

// flushing data to disk and trigger compaction
func (dm *DiskManager) Flush(records []types.Record) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	table, err := CreateNewTableToDisk(records, dm.dir)
	if err != nil {
		return err
	}

	dm.levels[0].push(table)

	err = dm.checkAndCompact()

	if err != nil {
		return err
	}

	return nil
}

func (dm *DiskManager) checkAndCompact() error {
	for levelIndex, level := range dm.levels {
		if level.size() > dm.l0Target*int(math.Pow(float64(dm.levelRatio), float64(levelIndex))) {
			err := dm.compact(levelIndex)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (dm *DiskManager) compact(levelIndex int) error {
	// do the compaction for the ln and ln+1 tables
	if len(dm.levels) < levelIndex+1 {
		dm.levels = append(dm.levels, &Level{})
	}

	lnTables := dm.levels[levelIndex].GetAll()

	nextLevelTables := dm.levels[levelIndex+1].GetAll()

	var t [][]*Table
	t = append(t, lnTables)
	t = append(t, nextLevelTables)

	var r [][]types.Record
	for _, tables := range t {
		for _, table := range tables {
			record, err := table.getAllEntries()
			if err != nil {
				panic(err)
			}

			r = append(r, record)
		}
	}

	mergedRecords := merge(r)
	mergedTables, err := CreateNewTableToDisk(mergedRecords, dm.dir)

	if err != nil {
		return err
	}

	for _, lntable := range lnTables {
		dm.levels[levelIndex].delete(func(table *Table) bool {
			return strings.EqualFold(table.filePath, lntable.filePath)
		})
	}

	for _, table := range nextLevelTables {
		dm.levels[levelIndex+1].delete(func(_table *Table) bool {
			return strings.EqualFold(_table.filePath, table.filePath)
		})
	}

	dm.levels[levelIndex+1].push(mergedTables)

	return nil
}

func merge(records [][]types.Record) []types.Record {
	var r []types.Element
	for idx, record := range records {
		element := types.Element{
			Entry: record[0], Index: idx,
		}
		r = append(r, element)
		records[idx] = record[1:]
	}

	elementHeap := types.InitHeap(r)

	var mergedElements []types.Element
	for elementHeap.Len() != 0 {
		topElement, _ := elementHeap.Pop().(types.Element)
		element := records[topElement.Index][0]

		if len(records[topElement.Index]) != 0 {
			records[topElement.Index] = records[topElement.Index][1:]
		}

		mergedElements = append(mergedElements, topElement)

		elementHeap.Push(element)
	}

	var merged []types.Record
	for _, element := range mergedElements {
		if !element.Entry.TombStone {
			merged = append(merged, element.Entry)
		}
	}

	return merged
}

func (dm *DiskManager) Get(key []byte) (types.Record, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	for _, level := range dm.levels {
		record, err := level.ScanAllTables(key)

		if err != nil &&
			err.(*types.EngineError).GetErrorCode() != types.TABLE_KEY_SEARCH_NOT_FOUND {
			return types.Record{}, err
		} else if err == nil {
			return record, nil
		}
	}

	return types.Record{}, types.NewEngineError(
		types.DISKMANAGER_KEY_NOT_FOUND_ERROR,
		fmt.Sprintf("key (%x) not found in any level!", key),
	)
}
