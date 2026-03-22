package iradix

import (
	"bytes"
)

// New returns an empty Tree.
// Tree implements an immutable radix tree. This can be treated as a
// Dictionary abstract data type. The main advantage over a standard
// hash map is prefix-based lookups and ordered iteration. The immutability
// means that it is safe to concurrently read from a Tree without any
// coordination.
func New[T comparable]() *Node[T] {
	return &Node[T]{}
}

// NewTxn creates new transaction that can be used to mutate the tree.
func NewTxn[T comparable](root *Node[T]) *Txn[T] {
	return &Txn[T]{
		revision: root.revision + 1,
		root:     root,
	}
}

// Txn is a transaction on the tree. This transaction is applied
// atomically and returns a new tree when committed. A transaction
// is not thread safe, and should only be used by a single goroutine.
type Txn[T comparable] struct {
	t        T
	revision uint64

	// root is the modified root for the transaction.
	root *Node[T]
}

// Root returns the current root of the radix tree within this
// transaction. The root is not safe across insert and delete operations,
// but can be used to read the current state during a transaction.
func (t *Txn[T]) Root() *Node[T] {
	return t.root
}

// Get is used to lookup a specific key, returning
// the value and if it was found.
func (t *Txn[T]) Get(k []byte) T {
	return t.root.Get(k)
}

// Insert is used to add or update a given key. The return provides
// the previous value and a bool indicating if any was set.
func (t *Txn[T]) Insert(k []byte, v T) T {
	if k == nil {
		k = []byte{}
	}

	n := &t.root
	search := k
	for {
		nc := t.writeNode(*n)
		*n = nc

		// Handle key exhaustion.
		if len(search) == 0 {
			oldValue := nc.value
			nc.value = v
			return oldValue
		}

		// Look for the edge.
		idx, child := nc.getEdge(search[0])

		// No edge, create one
		if child == nil {
			nc.addEdge(&Node[T]{
				revision: t.revision,
				value:    v,
				prefix:   copyPrefix(search),
			})
			return t.t
		}

		// Determine longest prefix of the search key on match.
		commonPrefix := longestPrefix(search, child.prefix)
		if commonPrefix == len(child.prefix) {
			search = search[commonPrefix:]
			n = &nc.edges[idx]
			continue
		}

		// Split the node.
		splitNode := &Node[T]{
			revision: t.revision,
			prefix:   copyPrefix(search[:commonPrefix]),
		}
		nc.replaceEdge(splitNode)

		// Restore the existing child node.
		modChild := t.writeNode(child)
		modChild.prefix = modChild.prefix[commonPrefix:]
		splitNode.addEdge(modChild)

		// If the new key is a subset, add to this node.
		search = search[commonPrefix:]
		if len(search) == 0 {
			splitNode.value = v
			return t.t
		}

		// Create a new edge for the node.
		splitNode.addEdge(&Node[T]{
			revision: t.revision,
			value:    v,
			prefix:   copyPrefix(search),
		})
		return t.t
	}
}

// Delete is used to delete a given key. Returns the old value if any,
// and a bool indicating if the key was set.
func (t *Txn[T]) Delete(k []byte) T {
	if k == nil {
		k = []byte{}
	}

	newRoot, oldValue := t.delete(t.root, k)
	if newRoot != nil {
		t.root = newRoot
	}
	return oldValue
}

// Commit is used to finalize the transaction and return a new tree.
func (t *Txn[T]) Commit() *Node[T] {
	return t.root
}

// Clone makes an independent copy of the transaction. The new transaction does not track any nodes and has
// TrackMutate turned off. The cloned transaction will contain any uncommitted writes in the original transaction
// but further mutations to either will be independent and result in different radix trees on Commit.
// A cloned transaction may be passed to another goroutine and mutated there independently however each transaction
// may only be mutated in a single thread.
func (t *Txn[T]) Clone() *Txn[T] {
	t.revision++
	return &Txn[T]{
		revision: t.revision,
		root:     t.root,
	}
}

// writeNode returns a node to be modified, if the current node has already been
// modified during the course of the transaction, it is used in-place. Set
// forLeafUpdate to true if you are getting a write node to update the leaf,
// which will set leaf mutation tracking appropriately as well.
func (t *Txn[T]) writeNode(n *Node[T]) *Node[T] {
	if n.revision == t.revision {
		return n
	}

	// Copy the existing node. If you have set forLeafUpdate it will be
	// safe to replace this leaf with another after you get your node for
	// writing. You MUST replace it, because the channel associated with
	// this leaf will be closed when this transaction is committed.
	nc := &Node[T]{
		revision: t.revision,
		value:    n.value,
		prefix:   n.prefix,
	}
	if len(n.edges) != 0 {
		// +2 is for possible new edges, to avoid slice growing later
		nc.edges = make([]*Node[T], len(n.edges), len(n.edges)+2)
		copy(nc.edges, n.edges)
	}

	return nc
}

// mergeChild is called to collapse the given node with its child. This is only
// called when the given node is not a leaf and has a single edge.
func (t *Txn[T]) mergeChild(n *Node[T]) {
	// Mark the child node as being mutated since we are about to abandon
	// it. We don't need to mark the leaf since we are retaining it if it
	// is there.
	child := n.edges[0]

	// Merge the nodes.
	n.prefix = concatPrefixes(n.prefix, child.prefix)
	n.value = child.value
	if len(child.edges) != 0 {
		n.edges = make([]*Node[T], len(child.edges))
		copy(n.edges, child.edges)
	} else {
		n.edges = nil
	}
}

func (t *Txn[T]) delete(n *Node[T], search []byte) (*Node[T], T) {
	// Check for key exhaustion.
	if len(search) == 0 {
		if n.value == t.t {
			return nil, t.t
		}

		// Remove the leaf node.
		nc := t.writeNode(n)
		oldValue := nc.value
		nc.value = t.t

		// Check if this node should be merged.
		if n != t.root && len(nc.edges) == 1 {
			t.mergeChild(nc)
		}
		return nc, oldValue
	}

	// Look for an edge.
	label := search[0]
	idx, child := n.getEdge(label)
	if child == nil || !bytes.HasPrefix(search, child.prefix) {
		return nil, t.t
	}

	// Consume the search prefix
	search = search[len(child.prefix):]
	newChild, oldValue := t.delete(child, search)
	if newChild == nil {
		return nil, t.t
	}

	// Copy this node.
	nc := t.writeNode(n)

	// Delete the edge if the node has no edges.
	if newChild.value == t.t && len(newChild.edges) == 0 {
		nc.delEdge(label)
		if n != t.root && len(nc.edges) == 1 && nc.value == t.t {
			t.mergeChild(nc)
		}
	} else {
		nc.edges[idx] = newChild
	}
	return nc, oldValue
}

func longestPrefix(k1, k2 []byte) int {
	l := len(k1)
	if l2 := len(k2); l2 < l {
		l = l2
	}
	var i int
	//nolint:intrange
	for i = 0; i < l; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func concatPrefixes(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c[copy(c, a):], b)
	return c
}

func copyPrefix(prefix []byte) []byte {
	c := make([]byte, len(prefix))
	copy(c, prefix)
	return c
}
