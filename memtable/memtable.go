package memtable

/*
	memtable 
		
*/
type Memtable struct {
	table *AvlTree
	size  int
}

func CreateMemtable() *Memtable {
	return &Memtable {
		table : &AvlTree{},
		size : 0,
	}
}


