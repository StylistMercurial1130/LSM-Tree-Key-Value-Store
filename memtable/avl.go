package memtable

import (
	"LsmStorageEngine/types"
	"bytes"
	"unsafe"
)

type node struct {
	key       []byte
	value     []byte
	height    int
	tombStone bool
	leftNode  *node
	rightNode *node
}

func newNode(key []byte, value []byte,tombStone bool) *node {
	return &node {
		key : key,
		value : value,
		height : 1,
		tombStone : tombStone,
		leftNode : nil,
		rightNode : nil,
	}
}

func (n *node) getHeight() int {
	if n == nil {
		return 0
	}

	return n.height
}

func (n *node) getInOrder() *node {
	if n.leftNode == nil {
		return n
	}

	return n.leftNode.getInOrder()
}

func (n *node) isLeaf() bool {
	if n.leftNode == nil && n.rightNode == nil {
		return true
	}

	return false
}

type AvlTree struct {
	rootNode *node
	height   int
	count	 int
	size	 int
}


func (t *AvlTree) GetCount() int {
	return t.getCount(t.rootNode)
}

func (t *AvlTree) getCount(rootNode *node) int {
	if rootNode == nil {
		return 0
	}

	return 1 + t.getCount(rootNode.leftNode) + t.getCount(rootNode.rightNode)
}

func (t *AvlTree) GetSize() int {
	return t.getSize(t.rootNode)
}

func (t *AvlTree) getSize(rootNode *node) int {
	if rootNode == nil {
		return 0
	}

	keyValuesize := len(rootNode.key) + len(rootNode.value)

	structuralInformationSize := 
		int(unsafe.Sizeof(rootNode.height)) + 
		int(unsafe.Sizeof(rootNode.leftNode)) + 
		int(unsafe.Sizeof(rootNode.rightNode)) + 
		int(unsafe.Sizeof(rootNode.tombStone))

	size := keyValuesize + structuralInformationSize

	return size + t.getSize(rootNode.leftNode) + t.getSize(rootNode.rightNode)
}

func (t *AvlTree) Insert(key []byte, value []byte,tombStone bool) {
	if t.rootNode == nil {
		t.rootNode = newNode(key, value,tombStone)
		t.height = t.rootNode.height
	} else {
		t.rootNode = t.insert(newNode(key, value, tombStone), t.rootNode)
		t.height = t.rootNode.height
	}
}

func (t *AvlTree) Delete(key []byte) {
	if t.rootNode != nil {
		t.rootNode = t.delete(key, t.rootNode)
		t.height = t.rootNode.height
	}
}

func rightRotation(current *node) *node {
	n := current.leftNode
	current.leftNode = n.rightNode
	n.rightNode = current

	current.height = 1 + max(current.leftNode.getHeight(), current.rightNode.getHeight())
	n.height = 1 + max(n.leftNode.getHeight(), n.rightNode.getHeight())

	return n
}

func leftRotation(current *node) *node {
	n := current.rightNode
	current.rightNode = n.leftNode
	n.leftNode = current

	current.height = 1 + max(current.leftNode.getHeight(), current.rightNode.getHeight())
	n.height = 1 + max(n.leftNode.getHeight(), n.rightNode.getHeight())

	return n
}

func (t *AvlTree) insert(n *node, current *node) *node {
	if current == nil {
		current = n
		return current
	}

	if bytes.Compare(n.key, current.key) < 0 {
		current.leftNode = t.insert(n, current.leftNode)
	} else {
		current.rightNode = t.insert(n, current.rightNode)
	}

	current.height = 1 + max(current.leftNode.getHeight(), current.rightNode.getHeight())

	balanceFactor := current.leftNode.getHeight() - current.rightNode.getHeight()

	// left bias
	if balanceFactor > 1 {
		if bytes.Compare(n.key, current.leftNode.key) > 0 {
			current.leftNode = leftRotation(current.leftNode)
			return rightRotation(current)
		} else {
			return rightRotation(current)
		}
	}

	// right bias
	if balanceFactor < -1 {
		if bytes.Compare(n.key, current.rightNode.key) < 0 {
			current.rightNode = rightRotation(current.rightNode)
			return leftRotation(current)
		} else {
			return leftRotation(current)
		}
	}

	return current
}

func (t *AvlTree) delete(key []byte, current *node) *node {
	if current == nil {
		return current
	}

	switch {
	case bytes.Compare(key, current.key) < 0:
		{
			current.leftNode = t.delete(key, current.leftNode)
		}
	case bytes.Compare(key, current.key) > 0:
		{
			current.rightNode = t.delete(key, current.rightNode)
		}
	case bytes.Equal(key, current.key):
		{
			if current.isLeaf() {
				return nil
			}

			if current.leftNode == nil {
				return current.rightNode
			} else if current.rightNode == nil {
				return current.leftNode
			} else {
				temp := current.rightNode.getInOrder()
				current.key = temp.key
				current.value = temp.value

				current.rightNode = t.delete(temp.key, current.rightNode)
			}
		}
	}

	current.height = 1 + max(current.leftNode.getHeight(), current.rightNode.getHeight())

	balanceFactor := current.leftNode.getHeight() - current.rightNode.getHeight()

	// left bias
	if balanceFactor > 1 {
		if bytes.Compare(key, current.leftNode.key) > 0 {
			current.leftNode = leftRotation(current.leftNode)
			return rightRotation(current)
		} else {
			return rightRotation(current)
		}
	}

	// right bias
	if balanceFactor < -1 {
		if bytes.Compare(key, current.rightNode.key) < 0 {
			current.rightNode = rightRotation(current.rightNode)
			return leftRotation(current)
		} else {
			return leftRotation(current)
		}
	}

	return current
}

func (t *AvlTree) Search(key []byte) []byte {
	if t.rootNode != nil {
		return t.search(key, t.rootNode)
	}

	return nil
}

func (t *AvlTree) search(key []byte, root *node) []byte {
	if root == nil {
		return nil
	}

	if bytes.Equal(root.key, key) {
		return root.value
	}

	if bytes.Compare(key, root.key) > 0 {
		return t.search(key, root.rightNode)
	}

	return t.search(key, root.leftNode)
}

func (t *AvlTree) getInorderForm() [][]byte {
	var buffer [][]byte
	traverseAndAppend(t.rootNode, &buffer)

	return buffer
}

func (t *AvlTree) GetAll() []Record {
	var buffer []Record
	t.getAll(t.rootNode,&buffer)

	return buffer
}

func (t *AvlTree) getAll(n *node,buffer *[]Record) {
	if n == nil {
		return;	
	}

	t.getAll(n.leftNode,buffer)

	record := Record {
		Key : n.key, 
		Value : n.value, 
		TombStone: n.tombStone,
	}
	*buffer = append(*buffer,record)

	t.getAll(n.rightNode,buffer)
}

func traverseAndAppend(n *node, buffer *[][]byte) {
	if n == nil {
		return
	}

	traverseAndAppend(n.leftNode, buffer)
	*buffer = append(*buffer, n.value)
	traverseAndAppend(n.rightNode, buffer)
}
