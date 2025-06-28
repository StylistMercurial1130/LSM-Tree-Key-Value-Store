package storageengine

import (
	"LsmStorageEngine/types"
	"sync"
)

type Memtable struct {
	mtx     sync.Mutex
	avl     *AvlTree
	sstable *SSTable
}

func NewMemtable() *Memtable {
	return &Memtable{
		avl: &AvlTree{},
	}
}

func (m *Memtable) Put(r types.Record) {

}

func (m *Memtable) Delete(key []byte) {

}

func (m *Memtable) Get(key []byte) (types.Record, error) {
	var record types.Record
	if record, err := m.avl.Search(key); err != nil {
		switch err.(*AvlTreeError).errCode {
		case AVL_KEY_DOES_NOT_EXIST:
			{
				// search the sparse index and load an sstable and search for the key
			}
		case AVL_TREE_EMPTY:
			{
				return types.Record{}, err
			}
		}
	}

	return record, nil
}

func (m *Memtable) GetAll() []types.Record {
	return m.avl.GetAll()
}

func (m *Memtable) Flush() {

}
