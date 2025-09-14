/*
	TODO! : add disk manager creation/initialization and checks
*/

package mem

import (
	"LsmStorageEngine/disk"
	"LsmStorageEngine/types"
	"sync"
)

type Memtable struct {
	mtx          sync.RWMutex
	avl          *AvlTree
	memtableSize int
}

func NewMemtable(memTableSize int) *Memtable {
	return &Memtable{
		avl:          &AvlTree{},
		memtableSize: memTableSize,
	}
}

func (m *Memtable) Put(r types.Record, dm *disk.DiskManager) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.avl.InsertRecord(r)

	if m.avl.GetSize() >= m.memtableSize {
		err := dm.Flush(m.avl.GetAll())

		if err != nil {
			return err
		} else {
			m.avl.Clear()
			return nil
		}
	}

	return nil
}

func (m *Memtable) Delete(key []byte, dm *disk.DiskManager) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	record, err := m.avl.Search(key)

	if err != nil {
		if status, ok := err.(*types.EngineError); ok {
			if status.GetErrorCode() == AVL_KEY_DOES_NOT_EXIST {
				record, err = dm.Get(key)

				if err == nil {
					record.TombStone = true
					m.Put(record, dm)

					return nil
				} else {
					return err
				}
			} else {
				return status
			}
		}
	} else {
		if !record.TombStone {
			m.avl.Insert(record.Key, record.Value, true)
		}
	}

	return nil
}

func (m *Memtable) Get(key []byte, dm *disk.DiskManager) (types.Record, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	record, err := m.avl.Search(key)

	if err != nil {
		if status, ok := err.(*types.EngineError); ok {
			if status.GetErrorCode() == AVL_KEY_DOES_NOT_EXIST {
				record, err = dm.Get(key)

				if err != nil {
					return types.Record{}, err
				} else {
					return record, nil
				}
			}
		}
	}

	return record, nil
}

func (m *Memtable) GetAll() []types.Record {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	return m.avl.GetAll()
}
