package memtable

import (
	"testing"
	"encoding/binary"
)

func serialize(value int) []byte {
	bs := make([]byte,8)
	binary.LittleEndian.PutUint64(bs,uint64(value))

	return bs
}

func deserialize(value []byte) int {
	return int(binary.LittleEndian.Uint64(value))
}

func Test3ValueInsert(t *testing.T) {
	var values = [3]int{1, 2, 3}
	var avlTree AvlTree

	for _,val := range(values) {
		avlTree.Insert(serialize(val),serialize(val),false)
	}

	buff := avlTree.getInorderForm()
	for i,val := range(buff) {
		if (i + 1) != deserialize(val) {
			t.Logf("buff[%d] = %d, they are not equal! insertion is wrong!",i,val)
			t.Fail()
		}
	}

	t.Log("insert({1,2,3}) passed !")
}

func TestDeletionNonLeafNoEmptyChildren(t *testing.T) {
	var values = [3]int{1, 2, 3}
	var avlTree AvlTree

	for _,val := range(values) {
		avlTree.Insert(serialize(val),serialize(val),false)
	}

	avlTree.Delete(serialize(1))
	buff := avlTree.getInorderForm()
	for i,val := range(buff) {
		if (i + 1) != deserialize(val) - 1 {
			t.Logf("buff[%d] = %d, they are not equal! insertion is wrong!",i,val)
			t.Fail()
		}
	}

	t.Log("delete(1) passed !")
}
