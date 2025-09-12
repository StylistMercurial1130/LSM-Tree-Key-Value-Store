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
	mtx          sync.Mutex
	avl          *AvlTree
	dm           *disk.DiskManager
	memtableSize int
}

func NewMemtable() *Memtable {
	return &Memtable{
		avl: &AvlTree{},
	}
}

func (m *Memtable) Put(r types.Record) error {
	m.mtx.Lock()

	m.avl.InsertRecord(r)

	if m.avl.GetSize() >= m.memtableSize {
		return m.dm.Flush(m.avl.GetAll())
	}

	m.mtx.Unlock()

	return nil
}

func (m *Memtable) Delete(key []byte) {
	m.mtx.Lock()

	record, err := m.avl.Search(key)

	if err != nil {
		if status, ok := err.(*types.EngineError); ok {
			if status.GetErrorCode() == AVL_KEY_DOES_NOT_EXIST {
				record, err = m.dm.Get(key)

				if err != nil {
					record.TombStone = true
					m.Put(record)
				}
			}
		}
	} else {
		if !record.TombStone {
			m.avl.Insert(record.Key, record.Value, true)
		}
	}

	m.mtx.Unlock()
}

func (m *Memtable) Get(key []byte) (types.Record, error) {
	m.mtx.Lock()

	record, err := m.avl.Search(key)

	if err != nil {
		if status, ok := err.(*types.EngineError); ok {
			if status.GetErrorCode() == AVL_KEY_DOES_NOT_EXIST {
				record, err = m.dm.Get(key)

				if err != nil {
					return types.Record{}, err
				} else {
					return record, nil
				}
			}
		}
	}

	m.mtx.Unlock()

	return record, nil
}

func (m *Memtable) GetAll() []types.Record {
	return m.avl.GetAll()
}
