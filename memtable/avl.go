package memtable

import "bytes"

type node struct {
	key       []byte
	value     []byte
	height    int
	leftNode  *node
	rightNode *node
}

func newNode(key []byte, value []byte) *node {
	_node := new(node)
	_node.key = key
	_node.value = value
	_node.height = 1

	return _node
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
}

func (t *AvlTree) Insert(key []byte, value []byte) {
	if t.rootNode == nil {
		t.rootNode = newNode(key, value)
		t.height = t.rootNode.height
	} else {
		t.rootNode = t.insert(key, value, t.rootNode)
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

func (t *AvlTree) insert(key []byte, value []byte, current *node) *node {
	if current == nil {
		current = newNode(key, value)
		return current
	}

	if bytes.Compare(key, current.key) < 0 {
		current.leftNode = t.insert(key, value, current.leftNode)
	} else {
		current.rightNode = t.insert(key, value, current.rightNode)
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
				current = current.rightNode
			} else if current.rightNode == nil {
				current = current.leftNode
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
	if bytes.Equal(root.key, key) || root == nil {
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

func traverseAndAppend(n *node, buffer *[][]byte) {
	if n == nil {
		return
	}

	traverseAndAppend(n.leftNode, buffer)
	*buffer = append(*buffer, n.value)
	traverseAndAppend(n.rightNode, buffer)
}
