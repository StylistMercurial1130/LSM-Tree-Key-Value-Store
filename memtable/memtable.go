package memtable

import "LsmStorageEngine/types"
import "sync"

type Record = types.Record

type Memtable struct {
	mtx sync.Mutex
	avl *AvlTree
}

func NewMemtable() *Memtable {
	return &Memtable {
		avl : &AvlTree{},
	}
}

func (m *Memtable) Put(r Record) {
	
}

func (m *Memtable) Delete(key []byte) {

}

func (m *Memtable) Get(key []byte) []byte {	
	if val := m.avl.Search(key); val != nil {
		// found value in the memtable
		return val
	} else {
		
	}
}

func (m *Memtable) GetAll() []Record {	
	return m.avl.GetAll()
} 

func (m *Memtable) Flush() {

}



