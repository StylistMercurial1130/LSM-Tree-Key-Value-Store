package memtable

type node[V any] struct {
	key       int
	value     V
	height    int
	leftNode  *node[V]
	rightNode *node[V]
}

func newNode[V any](key int, value V) *node[V] {
	_node := new(node[V])
	_node.key = key
	_node.value = value
	_node.height = 1

	return _node
}

func (n *node[V]) getHeight() int {
	if n == nil {
		return 0
	}

	return n.height
}

func (n *node[V]) getInOrder() *node[V] {
	if n.leftNode == nil {
		return n
	}

	return n.leftNode.getInOrder()
}

func (n *node[V]) isLeaf() bool {
	if n.leftNode == nil && n.rightNode == nil {
		return true
	}

	return false
}

type AvlTree[V any] struct {
	rootNode *node[V]
	height   int
}

func (t *AvlTree[V]) Insert(key int, value V) {
	if t.rootNode == nil {
		t.rootNode = newNode(key, value)
		t.height = t.rootNode.height
	} else {
		t.rootNode = t.insert(key, value, t.rootNode)
		t.height = t.rootNode.height
	}
}

func (t *AvlTree[V]) Delete(key int) {
	if t.rootNode != nil {
		t.rootNode = t.delete(key, t.rootNode)
		t.height = t.rootNode.height
	}
}

func rightRotation[V any](current *node[V]) *node[V] {
	n := current.leftNode
	current.leftNode = n.rightNode
	n.rightNode = current

	current.height = 1 + max(current.leftNode.getHeight(), current.rightNode.getHeight())
	n.height = 1 + max(n.leftNode.getHeight(), n.rightNode.getHeight())

	return n
}

func leftRotation[V any](current *node[V]) *node[V] {
	n := current.rightNode
	current.rightNode = n.leftNode
	n.leftNode = current

	current.height = 1 + max(current.leftNode.getHeight(), current.rightNode.getHeight())
	n.height = 1 + max(n.leftNode.getHeight(), n.rightNode.getHeight())

	return n
}

func (t *AvlTree[V]) insert(key int, value V, current *node[V]) *node[V] {
	if current == nil {
		current = newNode(key, value)
		return current
	}

	if key < current.key {
		current.leftNode = t.insert(key, value, current.leftNode)
	} else {
		current.rightNode = t.insert(key, value, current.rightNode)
	}

	current.height = 1 + max(current.leftNode.getHeight(), current.rightNode.getHeight())

	balanceFactor := current.leftNode.getHeight() - current.rightNode.getHeight()

	// left bias
	if balanceFactor > 1 {
		if key > current.leftNode.key {
			current.leftNode = leftRotation(current.leftNode)
			return rightRotation(current)
		} else {
			return rightRotation(current)
		}
	}

	// right bias
	if balanceFactor < -1 {
		if key < current.rightNode.key {
			current.rightNode = rightRotation(current.rightNode)
			return leftRotation(current)
		} else {
			return leftRotation(current)
		}
	}

	return current
}

func (t *AvlTree[V]) delete(key int, current *node[V]) *node[V] {
	if current == nil {
		return current
	}

	switch {
	case key < current.key:
		{
			current.leftNode = t.delete(key, current.leftNode)
		}
	case key > current.key:
		{
			current.rightNode = t.delete(key, current.rightNode)
		}
	case key == current.key:
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
		if key > current.leftNode.key {
			current.leftNode = leftRotation(current.leftNode)
			return rightRotation(current)
		} else {
			return rightRotation(current)
		}
	}

	// right bias
	if balanceFactor < -1 {
		if key < current.rightNode.key {
			current.rightNode = rightRotation(current.rightNode)
			return leftRotation(current)
		} else {
			return leftRotation(current)
		}
	}

	return current
}

func (t *AvlTree[V]) Search(key int) *V {
	if t.rootNode != nil {
		return t.search(key,t.rootNode)
	}

	return nil
}

func (t *AvlTree[V]) search(key int,root *node[V]) *V {
	if root.key == key || root == nil {
		return &root.value
	}

	if key > root.key {
		return t.search(key,root.rightNode)
	}

	return t.search(key,root.leftNode)
}

func (t *AvlTree[V]) getInorderForm() []V {
	var buffer []V
	traverseAndAppend(t.rootNode, &buffer)

	return buffer
}

func traverseAndAppend[V any](n *node[V], buffer *[]V) {
	if n == nil {
		return
	}

	traverseAndAppend(n.leftNode, buffer)
	*buffer = append(*buffer, n.value)
	traverseAndAppend(n.rightNode, buffer)
}
