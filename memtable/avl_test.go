package memtable

import (
	"testing"
)

func Test3ValueInsert(t *testing.T) {
	var values = [3]int{1, 2, 3}
	var avlTree AvlTree[int]

	for _,val := range(values) {
		avlTree.Insert(val,val)	
	}

	buff := avlTree.getInorderForm()
	for i,val := range(buff) {
		if (i + 1) != val {
			t.Logf("buff[%d] = %d, they are not equal! insertion is wrong!",i,val)
			t.Fail()
		}
	}

	t.Log("insert({1,2,3}) passed !")
}

func TestDeletionNonLeafNoEmptyChildren(t *testing.T) {
	var values = [3]int{1, 2, 3}
	var avlTree AvlTree[int]

	for _,val := range(values) {
		avlTree.Insert(val,val)	
	}

	avlTree.Delete(1)
	buff := avlTree.getInorderForm()
	for i,val := range(buff) {
		if (i + 1) != val - 1 {
			t.Logf("buff[%d] = %d, they are not equal! insertion is wrong!",i,val)
			t.Fail()
		}
	}

	t.Log("delete(1) passed !")
}
