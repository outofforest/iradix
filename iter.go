package iradix

import (
	"bytes"
)

type item[T comparable] struct {
	edges          edges[T]
	index1, index2 int
}

// Iterator is used to iterate over a set of nodes
// in pre-order.
type Iterator[T comparable] struct {
	t     T
	node  *Node[T]
	stack []item[T]
	skip  int
}

// SeekPrefix is used to seek the iterator to a given prefix.
func (i *Iterator[T]) SeekPrefix(prefix []byte) {
	// Wipe the stack
	i.stack = nil
	search := prefix
	for {
		// Check for key exhaustion.
		if len(search) == 0 {
			i.skip = len(i.node.prefix)
			return
		}

		// Look for an edge.
		_, i.node = i.node.getEdge(search[0])
		switch {
		case i.node == nil:
			return
		case bytes.HasPrefix(search, i.node.prefix):
			search = search[len(i.node.prefix):]
		case bytes.HasPrefix(i.node.prefix, search):
			i.skip = len(search)
			return
		default:
			i.node = nil
			return
		}
	}
}

// SeekLowerBound is used to seek the iterator to the smallest key that is
// greater or equal to the given key. There is no watch variant as it's hard to
// predict based on the radix structure which node(s) changes might affect the
// result.
func (i *Iterator[T]) SeekLowerBound(key []byte) {
	if i.node == nil {
		return
	}
	i.initStack()

	for len(i.stack) > 0 {
		n := i.peek()

		cmp := key
		if len(n.prefix) < len(key) {
			cmp = key[:len(n.prefix)]
		}

		prefixCmp := bytes.Compare(n.prefix, cmp)

		if prefixCmp == 0 && len(n.prefix) == len(key) {
			return
		}

		if prefixCmp > 0 && n.value != i.t {
			return
		}

		i.pop()
		if prefixCmp > 0 {
			i.findMin(n)
			return
		}

		if prefixCmp < 0 {
			i.node = nil
			return
		}

		// Consume the search prefix if the current node has one. Note that this is
		// safe because if n.prefix is longer than the search slice prefixCmp would
		// have been > 0 above and the method would have already returned.
		key = key[len(n.prefix):]

		idx, lbNode := n.getLowerBoundEdge(key[0])
		if lbNode == nil {
			return
		}

		i.stack = append(i.stack, item[T]{edges: n.edges, index1: idx, index2: idx})
	}
}

// Next returns the next node in order.
func (i *Iterator[T]) Next() T {
	if i.stack == nil && i.node != nil {
		i.initStack()
	}

	for len(i.stack) > 0 {
		if n := i.forward(); n != nil && n.value != i.t {
			return n.value
		}
	}

	return i.t
}

// Back moves iterator back.
func (i *Iterator[T]) Back(count uint64) {
	for len(i.stack) > 0 && count > 0 {
		n := i.backward()
		if n != nil && n.value != i.t {
			count--
		}
	}
}

func (i *Iterator[T]) peek() *Node[T] {
	itm := i.stack[len(i.stack)-1]
	return itm.edges[itm.index1]
}

func (i *Iterator[T]) pop() *Node[T] {
	itm := i.stack[len(i.stack)-1]
	n := itm.edges[itm.index1]

	i.stack[len(i.stack)-1].index1++
	i.stack[len(i.stack)-1].index2++
	return n
}

func (i *Iterator[T]) forward() *Node[T] {
	itm := &i.stack[len(i.stack)-1]
	if itm.index2 == len(itm.edges) {
		i.stack = i.stack[:len(i.stack)-1]
		if len(i.stack) > 0 {
			itm := &i.stack[len(i.stack)-1]
			itm.index2 = itm.index1
		}
		return nil
	}

	n := itm.edges[itm.index2]
	itm.index1++
	itm.index2++

	if len(n.edges) > 0 {
		i.stack = append(i.stack, item[T]{edges: n.edges, index1: 0, index2: 0})
	}

	return n
}

func (i *Iterator[T]) backward() *Node[T] {
	itm := &i.stack[len(i.stack)-1]
	if itm.index1 == 0 {
		i.stack = i.stack[:len(i.stack)-1]
		if len(i.stack) == 0 {
			i.stack = nil
			return nil
		}

		itm := &i.stack[len(i.stack)-1]
		if itm.index1 == itm.index2 {
			itm.index1--
			itm.index2--
			return itm.edges[itm.index1]
		}
		return nil
	}

	if itm.index1 == itm.index2 {
		itm.index2--
		n := itm.edges[itm.index2]
		if len(n.edges) > 0 {
			i.stack = append(i.stack, item[T]{edges: n.edges, index1: len(n.edges), index2: len(n.edges)})
			return nil
		}
	}

	itm.index1--
	return itm.edges[itm.index1]
}

func (i *Iterator[T]) findMin(n *Node[T]) {
	for {
		i.stack = append(i.stack, item[T]{edges: n.edges, index1: 0, index2: 0})
		n = n.edges[0]
		if n.value != i.t {
			return
		}
		i.pop()
	}
}

func (i *Iterator[T]) initStack() {
	i.stack = []item[T]{
		{
			edges: edges[T]{
				&Node[T]{
					revision: i.node.revision,
					value:    i.node.value,
					prefix:   i.node.prefix[i.skip:],
					edges:    i.node.edges,
				},
			},
			index1: 0,
			index2: 0,
		},
	}
}
