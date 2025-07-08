package storageengine

import (
	"LsmStorageEngine/types"
	"sync"
)

type Memtable struct {
	mtx          sync.Mutex
	avl          *AvlTree
	memtableSize int
}

func NewMemtable() *Memtable {
	return &Memtable{
		avl: &AvlTree{},
	}
}

func (m *Memtable) Put(r types.Record) {
	m.avl.InsertRecord(r)

	if m.avl.GetSize() >= m.memtableSize {
	}
}

func (m *Memtable) Delete(key []byte) {

}

func (m *Memtable) Get(key []byte) (types.Record, error) {
	var record types.Record
	if record, err := m.avl.Search(key); err != nil {
		switch err.(*AvlTreeError).errCode {
		case AVL_KEY_DOES_NOT_EXIST:
			{
				// search the sstables to find for the key

			}
		case AVL_TREE_EMPTY:
			{
				return types.Record{}, err
			}
		}
	}

	return record, nil
}

func (m *Memtable) GetAll() []types.Entry {
	return m.avl.GetAll()
}
