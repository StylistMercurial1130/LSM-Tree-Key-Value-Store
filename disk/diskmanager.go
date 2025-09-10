package disk

import (
	"LsmStorageEngine/types"
	"math"
	"strings"
)

type DiskManager struct {
	levels     []*Level
	levelRatio int
	l0Target   int
	dir        string
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
	table, err := CreateNewTableToDisk(records, dm.dir)
	if err != nil {
		// for now just return err, will need to handle this properly
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
			if levelIndex == 0 {
				err := dm.compactL0()
				if err != nil {
					return err
				}
			} else {
				err := dm.compact(levelIndex)
				if err != nil {
					return err
				}
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

	lnTables, err := dm.levels[levelIndex].get(0)
	if err != nil {
		return err
	}

	start, end := lnTables.GetBoundaries()

	nextLevelOverlappingTables := dm.levels[levelIndex+1].getOverlappingTablesInRange(start, end)

	var t [][]*Table
	t = append(t, []*Table{lnTables})
	t = append(t, nextLevelOverlappingTables)

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

	dm.levels[levelIndex].delete(func(table Table) bool {
		return strings.EqualFold(table.filePath, lnTables.filePath)
	})

	for _, table := range nextLevelOverlappingTables {
		dm.levels[levelIndex+1].delete(func(_table Table) bool {
			return strings.EqualFold(_table.filePath, table.filePath)
		})
	}

	dm.levels[levelIndex+1].push(mergedTables)

	return nil
}

func (dm *DiskManager) compactL0() error {
	if dm.levels[0] == nil || dm.levels[0].size() == 0 {
		return types.NewEngineError(
			types.TABLE_MERGE_ERROR,
			"level 0 is empty",
		)
	}

	if l0OverlappingTables, start, end := dm.levels[0].getOverlappingTables(); l0OverlappingTables != nil {
		l1OverlappingTables := dm.levels[1].getOverlappingTablesInRange(start, end)
		// perform k way merge here
		var t [][]*Table
		t = append(t, l0OverlappingTables)
		t = append(t, l1OverlappingTables)

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

		// remove tables from L0 and L1
		for _, table := range l0OverlappingTables {
			dm.levels[0].delete(func(_table Table) bool {
				return strings.EqualFold(table.filePath, _table.filePath)
			})
		}

		// push the mergedTable into L0
		for _, table := range l1OverlappingTables {
			dm.levels[1].delete(func(_table Table) bool {
				return strings.EqualFold(table.filePath, _table.filePath)
			})
		}

		dm.levels[1].push(mergedTables)
	}

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

// get a key from the disk if not found or some arbitrary error return valid error
func (dm *DiskManager) Get(key []byte) (types.Record, error) {
	return types.Record{}, nil
}
