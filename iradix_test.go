package iradix

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func CopyTree[T any](t *Node[T]) *Node[T] {
	nn := &Node[T]{
		revision: t.revision,
		value:    t.value,
	}
	if t.prefix != nil {
		nn.prefix = make([]byte, len(t.prefix))
		copy(nn.prefix, t.prefix)
	}
	if len(t.edges) != 0 {
		nn.edges = make([]*Node[T], len(t.edges))
		for idx, edge := range t.edges {
			nn.edges[idx] = CopyTree(edge)
		}
	}
	return nn
}

func TestRadix_HugeTxn(t *testing.T) {
	r := New[int]()

	// Insert way more nodes than the cache can fit
	txn1 := NewTxn(r)

	type kv struct {
		k string
		v int
	}

	var pairs []kv
	for i := range 800_000 {
		pair := kv{
			k: randomString(t),
			v: i,
		}
		pairs = append(pairs, pair)
		oldV := txn1.Insert([]byte(pair.k), &pair.v)
		require.Nil(t, oldV)
	}

	sort.Slice(pairs, func(i, j int) bool {
		return strings.Compare(pairs[i].k, pairs[j].k) < 0
	})

	r = txn1.Commit()

	// Collect the output, should be sorted
	var out []int
	it := r.Iterator()
	for {
		v := it.Next()
		if v == nil {
			break
		}
		out = append(out, *v)
	}

	// Verify the match
	if len(out) != len(pairs) {
		t.Fatalf("length mis-match: %d vs %d", len(out), len(pairs))
	}
	for i, o := range out {
		if o != pairs[i].v {
			t.Fatalf("mis-match: %v %v", o, pairs[i].v)
		}
	}
}

func TestRadix(t *testing.T) {
	var minValue, maxValue string
	inp := map[string]int{}
	for i := range 1000 {
		gen := randomString(t)
		inp[gen] = i
		if gen < minValue || i == 0 {
			minValue = gen
		}
		if gen > maxValue || i == 0 {
			maxValue = gen
		}
	}

	r := New[int]()
	rCopy := CopyTree(r)
	for k, v := range inp {
		txn := NewTxn(r)
		oldV := txn.Insert([]byte(k), &v)
		require.Nil(t, oldV)
		newR := txn.Commit()
		if !reflect.DeepEqual(r, rCopy) {
			t.Errorf("r: %#v rc: %#v", r, rCopy)
			t.Errorf("r: %#v rc: %#v", r, rCopy)
		}
		r = newR
		rCopy = CopyTree(r)
	}

	for k, v := range inp {
		out := NewTxn(r).Get([]byte(k))
		if out == nil {
			t.Fatalf("missing key: %v", k)
		}
		if *out != v {
			t.Fatalf("value mis-match: %v %v", out, v)
		}
	}

	// Copy the full tree before delete
	orig := r
	origCopy := CopyTree(r)

	for k, v := range inp {
		txn := NewTxn(r)
		oldV := txn.Delete([]byte(k))
		require.Equal(t, v, *oldV)
		r = txn.Commit()
	}

	if !reflect.DeepEqual(orig, origCopy) {
		t.Fatalf("structure modified")
	}
}

func TestRoot(t *testing.T) {
	r := New[bool]()
	txn := NewTxn(r)
	oldV := txn.Delete(nil)
	require.Nil(t, oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	oldV = txn.Insert(nil, lo.ToPtr(true))
	require.Nil(t, oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	oldV = txn.Insert(nil, lo.ToPtr(false))
	require.True(t, *oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	oldV = txn.Insert(nil, lo.ToPtr(true))
	require.False(t, *oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	val := txn.Get(nil)
	if val == nil || *val != true {
		t.Fatalf("bad: %#v", val)
	}
	r = txn.Commit()
	txn = NewTxn(r)
	oldV = txn.Delete(nil)
	require.True(t, *oldV)
	txn.Commit()

	val = txn.Get(nil)
	if val != nil {
		t.Fatalf("bad: %#v", val)
	}
	txn.Commit()
}

func TestInsertUpdateDelete(t *testing.T) {
	r := New[bool]()
	s := []string{"", "A", "AB"}

	for _, ss := range s {
		txn := NewTxn(r)
		oldV := txn.Insert([]byte(ss), lo.ToPtr(false))
		require.Nil(t, oldV)
		r = txn.Commit()
	}

	for _, ss := range s {
		txn := NewTxn(r)

		v := txn.Get([]byte(ss))
		if v == nil || *v != false {
			t.Fatalf("bad %q", ss)
		}

		r = txn.Commit()
	}

	for _, ss := range s {
		txn := NewTxn(r)
		oldV := txn.Insert([]byte(ss), lo.ToPtr(true))
		require.False(t, *oldV)
		r = txn.Commit()
	}

	for _, ss := range s {
		txn := NewTxn(r)

		v := txn.Get([]byte(ss))
		if v == nil || *v != true {
			t.Fatalf("bad %q", ss)
		}

		oldV := txn.Delete([]byte(ss))
		require.True(t, *oldV)

		v = txn.Get([]byte(ss))
		if v != nil {
			t.Fatalf("bad %q", ss)
		}

		r = txn.Commit()
	}
}

func findIndex(vs []string, v string) int {
	for i, v2 := range vs {
		if v2 == v {
			return i
		}
	}
	return -1
}

func TestIteratePrefix(t *testing.T) {
	r := New[int]()

	keys := []string{
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"foobar",
		"zipzap",
	}
	values := []int{}
	for i, k := range keys {
		txn := NewTxn(r)
		txn.Insert([]byte(k), &i)
		r = txn.Commit()
		values = append(values, i)
	}

	type exp struct {
		inp string
		out []int
	}
	cases := []exp{
		{
			"",
			values,
		},
		{
			"f",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
				findIndex(keys, "foobar"),
			},
		},
		{
			"foo",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
				findIndex(keys, "foobar"),
			},
		},
		{
			"foob",
			[]int{
				findIndex(keys, "foobar"),
			},
		},
		{
			"foo/",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
			},
		},
		{
			"foo/b",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
			},
		},
		{
			"foo/ba",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
			},
		},
		{
			"foo/bar",
			[]int{
				findIndex(keys, "foo/bar/baz"),
			},
		},
		{
			"foo/bar/baz",
			[]int{
				findIndex(keys, "foo/bar/baz"),
			},
		},
		{
			"foo/bar/bazoo",
			[]int{},
		},
		{
			"z",
			[]int{
				findIndex(keys, "zipzap"),
			},
		},
	}

	for idx, test := range cases {
		iter := r.Iterator()
		iter.SeekPrefix([]byte(test.inp))

		// Consume all the keys
		out := []int{}
		for {
			v := iter.Next()
			if v == nil {
				break
			}
			out = append(out, *v)
		}
		if !reflect.DeepEqual(out, test.out) {
			t.Fatalf("mis-match: %d %v %v", idx, out, test.out)
		}
	}
}

func TestIterateLowerBound(t *testing.T) {
	// these should be defined in order
	var fixedLenKeys = []string{
		"00000",
		"00001",
		"00004",
		"00010",
		"00020",
		"20020",
	}

	// these should be defined in order
	var mixedLenKeys = []string{
		"a1",
		"abc",
		"barbazboo",
		"f",
		"foo",
		"found",
		"zap",
		"zip",
	}

	type exp struct {
		keys   []string
		search string
		want   []string
	}
	cases := []exp{
		{
			fixedLenKeys,
			"00000",
			fixedLenKeys,
		},
		{
			fixedLenKeys,
			"00003",
			[]string{
				"00004",
				"00010",
				"00020",
				"20020",
			},
		},
		{
			fixedLenKeys,
			"00010",
			[]string{
				"00010",
				"00020",
				"20020",
			},
		},
		{
			fixedLenKeys,
			"20000",
			[]string{
				"20020",
			},
		},
		{
			fixedLenKeys,
			"20020",
			[]string{
				"20020",
			},
		},
		{
			fixedLenKeys,
			"20022",
			[]string{},
		},
		{
			mixedLenKeys,
			"A", // before all lower case letters
			mixedLenKeys,
		},
		{
			mixedLenKeys,
			"a1",
			mixedLenKeys,
		},
		{
			mixedLenKeys,
			"b",
			[]string{
				"barbazboo",
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			mixedLenKeys,
			"bar",
			[]string{
				"barbazboo",
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			mixedLenKeys,
			"barbazboo0",
			[]string{
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			mixedLenKeys,
			"zippy",
			[]string{},
		},
		{
			mixedLenKeys,
			"zi",
			[]string{
				"zip",
			},
		},

		// This is a case found by TestIterateLowerBoundFuzz simplified by hand. The
		// lowest node should be the first, but it is split on the same char as the
		// second char in the search string. My initial implementation didn't take
		// that into account (i.e. propagate the fact that we already know we are
		// greater than the input key into the recursion). This would skip the first
		// result.
		{
			[]string{
				"bb",
				"bc",
			},
			"ac",
			[]string{"bb", "bc"},
		},

		// This is a case found by TestIterateLowerBoundFuzz.
		{
			//nolint:lll
			[]string{"aaaba", "aabaa", "aabab", "aabcb", "aacca", "abaaa", "abacb", "abbcb", "abcaa", "abcba", "abcbb", "acaaa", "acaab", "acaac", "acaca", "acacb", "acbaa", "acbbb", "acbcc", "accca", "babaa", "babcc", "bbaaa", "bbacc", "bbbab", "bbbac", "bbbcc", "bbcab", "bbcca", "bbccc", "bcaac", "bcbca", "bcbcc", "bccac", "bccbc", "bccca", "caaab", "caacc", "cabac", "cabbb", "cabbc", "cabcb", "cacac", "cacbc", "cacca", "cbaba", "cbabb", "cbabc", "cbbaa", "cbbab", "cbbbc", "cbcbb", "cbcbc", "cbcca", "ccaaa", "ccabc", "ccaca", "ccacc", "ccbac", "cccaa", "cccac", "cccca"},
			"cbacb",
			//nolint:lll
			[]string{"cbbaa", "cbbab", "cbbbc", "cbcbb", "cbcbc", "cbcca", "ccaaa", "ccabc", "ccaca", "ccacc", "ccbac", "cccaa", "cccac", "cccca"},
		},

		// Panic case found be TestIterateLowerBoundFuzz.
		{
			[]string{"gcgc"},
			"",
			[]string{"gcgc"},
		},

		// We SHOULD support keys that are prefixes of each other despite some
		// confusion in the original implementation.
		{
			[]string{"f", "fo", "foo", "food", "bug"},
			"foo",
			[]string{"foo", "food"},
		},

		// We also support the empty key (which is a prefix of every other key) as a
		// valid key to insert and search for.
		{
			[]string{"f", "fo", "foo", "food", "bug", ""},
			"foo",
			[]string{"foo", "food"},
		},
		{
			[]string{"f", "bug", ""},
			"",
			[]string{"", "bug", "f"},
		},
		{
			[]string{"f", "bug", "xylophone"},
			"",
			[]string{"bug", "f", "xylophone"},
		},

		// This is a case we realized we were not covering while fixing
		// SeekReverseLowerBound and could panic before.
		{
			[]string{"bar", "foo00", "foo11"},
			"foo",
			[]string{"foo00", "foo11"},
		},
	}

	for idx, test := range cases {
		t.Run(fmt.Sprintf("case%03d", idx), func(t *testing.T) {
			r := New[string]()

			// Insert keys
			txn := NewTxn(r)
			for _, k := range test.keys {
				old := txn.Insert([]byte(k), &k)
				if old != nil {
					t.Fatalf("duplicate key %s in keys", k)
				}
			}
			r = txn.Commit()

			// Get and seek iterator
			iter := r.Iterator()
			iter.SeekLowerBound([]byte(test.search))

			// Consume all the keys
			out := []string{}
			for {
				v := iter.Next()
				if v == nil {
					break
				}
				out = append(out, *v)
			}
			if !reflect.DeepEqual(out, test.want) {
				t.Fatalf("mis-match: key=%s\n  got=%v\n  want=%v", test.search,
					out, test.want)
			}
		})
	}
}

func TestIterateBack(t *testing.T) {
	// these should be defined in order
	var fixedLenKeys = []string{
		"00000",
		"00001",
		"00004",
		"00010",
		"00011",
		"00012",
		"00020",
		"20020",
	}

	// these should be defined in order
	var mixedLenKeys = []string{
		"a1",
		"abc",
		"barbazboo",
		"f",
		"foo",
		"found",
		"zap",
		"zip",
	}

	type exp struct {
		keys   []string
		prefix string
		lower  string
		steps  []int
		want   []string
	}

	cases := []exp{
		{
			keys:  fixedLenKeys,
			steps: []int{1, -1},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			steps: []int{3, -2},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{0},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00010",
			steps: []int{-1},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{-1},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{1, -2},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{-1, 1},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{1, -1},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{2, -1},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{2, -2},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{3, -1},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{3, -2},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{3, -3},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{4, -1},
			want: []string{
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{4, -2},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{4, -3},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{4, -4},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{5, -1},
			want: []string{
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{5, -2},
			want: []string{
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{5, -3},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{5, -4},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{5, -5},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{6, -1},
			want: []string{
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{6, -2},
			want: []string{
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{6, -3},
			want: []string{
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{6, -4},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{6, -5},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{6, -6},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -1},
			want: []string{
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -2},
			want: []string{
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -3},
			want: []string{
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -4},
			want: []string{
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -5},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -6},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -7},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -1},
			want: []string{
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -2},
			want: []string{
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -3},
			want: []string{
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -4},
			want: []string{
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -5},
			want: []string{
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -6},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -7},
			want: []string{
				"00001",
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{8, -8},
			want:  fixedLenKeys,
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{9, -3},
			want:  []string{},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{3, -2, 1},
			want: []string{
				"00004",
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{4, -2, 1},
			want: []string{
				"00010",
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -3, 3},
			want: []string{
				"20020",
			},
		},
		{
			keys:  fixedLenKeys,
			lower: "00000",
			steps: []int{7, -3, 3, -3},
			want: []string{
				"00011",
				"00012",
				"00020",
				"20020",
			},
		},
		{
			keys:   fixedLenKeys,
			prefix: "2",
			steps:  []int{-1},
			want: []string{
				"20020",
			},
		},
		{
			keys:   fixedLenKeys,
			prefix: "2",
			steps:  []int{1, -1},
			want: []string{
				"20020",
			},
		},
		{
			keys:   fixedLenKeys,
			prefix: "0001",
			lower:  "2",
			steps:  []int{-2},
			want: []string{
				"00010",
				"00011",
				"00012",
			},
		},
		{
			keys:   fixedLenKeys,
			prefix: "0001",
			lower:  "2",
			steps:  []int{-3},
			want: []string{
				"00010",
				"00011",
				"00012",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			steps:  []int{2, -2},
			want: []string{
				"f",
				"foo",
				"found",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			steps:  []int{2, -1},
			want: []string{
				"foo",
				"found",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			lower:  "oo",
			steps:  []int{-1},
			want: []string{
				"f",
				"foo",
				"found",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			lower:  "oo",
			steps:  []int{-1, 2, -1},
			want: []string{
				"foo",
				"found",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			lower:  "oo",
			steps:  []int{-1, 2, -2},
			want: []string{
				"f",
				"foo",
				"found",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			lower:  "ound",
			steps:  []int{-1},
			want: []string{
				"foo",
				"found",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			lower:  "ound",
			steps:  []int{-2},
			want: []string{
				"f",
				"foo",
				"found",
			},
		},
		{
			keys:   mixedLenKeys,
			prefix: "f",
			lower:  "ound",
			steps:  []int{-2, 2, -1},
			want: []string{
				"foo",
				"found",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-1},
			want: []string{
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-2},
			want: []string{
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-3},
			want: []string{
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-4},
			want: []string{
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-5},
			want: []string{
				"barbazboo",
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-6},
			want: []string{
				"abc",
				"barbazboo",
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-7},
			want:  mixedLenKeys,
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-8},
			want:  mixedLenKeys,
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-5, 1},
			want: []string{
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-5, 2},
			want: []string{
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-5, 3},
			want: []string{
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-5, 4},
			want: []string{
				"zap",
				"zip",
			},
		},
		{
			keys:  mixedLenKeys,
			lower: "zip",
			steps: []int{-5, 2, -1},
			want: []string{
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			keys:  []string{"0", "1", "a", "ab", "abc", "abcd"},
			lower: "abcd",
			steps: []int{-1},
			want: []string{
				"abc",
				"abcd",
			},
		},
		{
			keys:  []string{"0", "1", "a", "ab", "abc", "abcd"},
			lower: "abcd",
			steps: []int{-2},
			want: []string{
				"ab",
				"abc",
				"abcd",
			},
		},
		{
			keys:  []string{"0", "1", "a", "ab", "abc", "abcd"},
			lower: "abcd",
			steps: []int{-3},
			want: []string{
				"a",
				"ab",
				"abc",
				"abcd",
			},
		},
		{
			keys:  []string{"0", "1", "a", "ab", "abc", "abcd"},
			lower: "abcd",
			steps: []int{-4},
			want: []string{
				"1",
				"a",
				"ab",
				"abc",
				"abcd",
			},
		},
		{
			keys:  []string{"0", "1", "a", "ab", "abc", "abcd"},
			lower: "abcd",
			steps: []int{-5},
			want: []string{
				"0",
				"1",
				"a",
				"ab",
				"abc",
				"abcd",
			},
		},
		{
			keys:  []string{"0", "1", "a", "ab", "abc", "abcd"},
			lower: "abcd",
			steps: []int{-4, 3},
			want: []string{
				"abc",
				"abcd",
			},
		},
		{
			keys:  []string{"0", "1", "a", "ab", "abc", "abcd"},
			lower: "abcd",
			steps: []int{-4, 3, -2},
			want: []string{
				"a",
				"ab",
				"abc",
				"abcd",
			},
		},
		{
			keys:  []string{"0", "1", "2", "3", "4"},
			lower: "4",
			steps: []int{-1},
			want: []string{
				"3",
				"4",
			},
		},
		{
			keys:  []string{"0", "1", "2", "3", "4"},
			lower: "4",
			steps: []int{-2},
			want: []string{
				"2",
				"3",
				"4",
			},
		},
		{
			keys:  []string{"0", "1", "2", "3", "4"},
			lower: "4",
			steps: []int{-3},
			want: []string{
				"1",
				"2",
				"3",
				"4",
			},
		},
		{
			keys:  []string{"0", "1", "2", "3", "4"},
			lower: "4",
			steps: []int{-4},
			want: []string{
				"0",
				"1",
				"2",
				"3",
				"4",
			},
		},
		{
			keys:  []string{"0", "1", "2", "3", "4"},
			lower: "4",
			steps: []int{-4, 4, -2},
			want: []string{
				"2",
				"3",
				"4",
			},
		},
		{
			keys:  []string{"0", "1", "2", "3", "4"},
			lower: "2",
			steps: []int{-1, 2, -1},
			want: []string{
				"2",
				"3",
				"4",
			},
		},
	}

	for idx, test := range cases {
		t.Run(fmt.Sprintf("case%03d", idx), func(t *testing.T) {
			r := New[string]()

			// Insert keys
			txn := NewTxn(r)
			for _, k := range test.keys {
				old := txn.Insert([]byte(k), &k)
				if old != nil {
					t.Fatalf("duplicate key %s in keys", k)
				}
			}
			r = txn.Commit()

			// Get and seek iterator
			iter := r.Iterator()
			if test.prefix != "" {
				iter.SeekPrefix([]byte(test.prefix))
			}
			if test.lower != "" {
				iter.SeekLowerBound([]byte(test.lower))
			}

			for _, s := range test.steps {
				if s > 0 {
					for range s {
						iter.Next()
					}
				} else {
					iter.Back(uint64(-s))
				}
			}

			// Consume all the keys
			out := []string{}
			for {
				v := iter.Next()
				if v == nil {
					break
				}
				out = append(out, *v)
			}
			require.Equal(t, test.want, out)
		})
	}
}

func TestIterateBackFuzz(t *testing.T) {
	for range 10 {
		r := New[string]()
		txn := NewTxn(r)

		rand := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
		values := make([]string, 0, 1000)
		valuesMap := map[string]struct{}{}
		for len(values) < cap(values) {
			v := randString(rand)
			if _, exists := valuesMap[v]; exists {
				continue
			}
			valuesMap[v] = struct{}{}
			values = append(values, v)
			txn.Insert([]byte(v), &v)
		}
		r = txn.Commit()

		sort.Strings(values)

		for i := range values {
			iter := r.Iterator()
			iter.SeekLowerBound([]byte(values[len(values)-1]))
			iter.Back(uint64(i))

			values2 := make([]string, 0, len(values))
			for {
				v := iter.Next()
				if v == nil {
					break
				}
				values2 = append(values2, *v)
			}

			require.Equal(t, values[len(values)-i-1:], values2)
		}

		start := len(values) / 2
		back := start / 2
		end := start - back + start - back

		iter := r.Iterator()
		iter.SeekLowerBound([]byte(values[start]))
		iter.Back(uint64(back))
		for range start {
			iter.Next()
		}
		iter.Back(uint64(back))
		values2 := make([]string, 0, len(values))
		for {
			v := iter.Next()
			if v == nil {
				break
			}
			values2 = append(values2, *v)
		}

		require.Equal(t, values[end:], values2)
	}
}

func randString(rand *mathrand.Rand) string {
	// Pick a random string from a limited alphabet that makes it easy to read the
	// failure cases.
	const letters = "abcdefg"

	// Ignore size and make them all shortish to provoke bigger chance of hitting
	// prefixes and more intersting tree shapes.
	size := rand.Intn(8)

	b := make([]byte, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type readableString string

func (s readableString) Generate(rand *mathrand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(readableString(randString(rand)))
}

func TestIterateLowerBoundFuzz(t *testing.T) {
	r := New[readableString]()
	set := []string{}

	// This specifies a property where each call adds a new random key to the radix
	// tree.
	//
	// It also maintains a plain sorted list of the same set of keys and asserts
	// that iterating from some random key to the end using LowerBound produces
	// the same list as filtering all sorted keys that are lower.

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		txn := NewTxn(r)
		txn.Insert([]byte(newKey), &newKey)
		r = txn.Commit()

		t.Logf("NewKey: %q, SearchKey: %q", newKey, searchKey)

		// Now iterate the tree from searchKey to the end
		it := r.Iterator()
		result := []string{}
		it.SeekLowerBound([]byte(searchKey))
		for {
			v := it.Next()
			if v == nil {
				break
			}
			result = append(result, string(*v))
		}
		return result
	}

	sliceAddSortAndFilter := func(newKey, searchKey readableString) []string {
		// Append the key to the set and re-sort
		set = append(set, string(newKey))
		sort.Strings(set)

		t.Logf("Current Set: %#v", set)
		t.Logf("Search Key: %#v %v", searchKey, "" >= string(searchKey))

		result := []string{}
		for i, k := range set {
			// Check this is not a duplicate of the previous value. Note we don't just
			// store the last string to compare because empty string is a valid value
			// in the set and makes comparing on the first iteration awkward.
			if i > 0 && set[i-1] == k {
				continue
			}
			if k >= string(searchKey) {
				result = append(result, k)
			}
		}
		return result
	}

	if err := quick.CheckEqual(radixAddAndScan, sliceAddSortAndFilter, nil); err != nil {
		t.Error(err)
	}
}

func TestIteratePrefixAndLowerBound(t *testing.T) {
	r := New[int]()

	keys := []string{
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"foobar",
		"zipzap",
	}
	values := []int{}
	for i, k := range keys {
		txn := NewTxn(r)
		txn.Insert([]byte(k), &i)
		r = txn.Commit()
		values = append(values, i)
	}

	type exp struct {
		prefix string
		bound  string
		out    []int
	}
	cases := []exp{
		{
			"",
			"",
			values,
		},
		{
			"f",
			"oo",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
				findIndex(keys, "foobar"),
			},
		},
		{
			"foo",
			"",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
				findIndex(keys, "foobar"),
			},
		},
		{
			"foob",
			"",
			[]int{
				findIndex(keys, "foobar"),
			},
		},
		{
			"foo/",
			"b",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
			},
		},
		{
			"foo",
			"/b",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
				findIndex(keys, "foobar"),
			},
		},
		{
			"foo/b",
			"ar",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
			},
		},
		{
			"foo/ba",
			"r/baz",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
			},
		},
		{
			"foo/bar",
			"/baz",
			[]int{
				findIndex(keys, "foo/bar/baz"),
			},
		},
		{
			"foo/bar/baz",
			"",
			[]int{
				findIndex(keys, "foo/bar/baz"),
			},
		},
		{
			"foo/bar/",
			"zzz",
			[]int{},
		},
		{
			"z",
			"a",
			[]int{
				findIndex(keys, "zipzap"),
			},
		},
	}

	for idx, test := range cases {
		iter := r.Iterator()
		iter.SeekPrefix([]byte(test.prefix))
		iter.SeekLowerBound([]byte(test.bound))

		// Consume all the keys
		out := []int{}
		for {
			v := iter.Next()
			if v == nil {
				break
			}
			out = append(out, *v)
		}
		if !reflect.DeepEqual(out, test.out) {
			t.Fatalf("mis-match: %d %v %v", idx, out, test.out)
		}
	}
}

func TestMergeChildNilEdges(t *testing.T) {
	r := New[int]()
	txn := NewTxn(r)
	txn.Insert([]byte("foobar"), lo.ToPtr(42))
	txn.Insert([]byte("foozip"), lo.ToPtr(43))
	txn.Delete([]byte("foobar"))
	r = txn.Commit()

	out := []int{}
	it := r.Iterator()
	for {
		v := it.Next()
		if v == nil {
			break
		}
		out = append(out, *v)
	}

	expect := []int{43}
	if !reflect.DeepEqual(out, expect) {
		t.Fatalf("mis-match: %v %v", out, expect)
	}
}

func TestMergeChildVisibility(t *testing.T) {
	r := New[int]()
	txn := NewTxn(r)
	txn.Insert([]byte("foobar"), lo.ToPtr(42))
	txn.Insert([]byte("foobaz"), lo.ToPtr(43))
	txn.Insert([]byte("foozip"), lo.ToPtr(10))
	r = txn.Commit()

	txn1 := NewTxn(r)
	txn2 := NewTxn(r)

	// Ensure we get the expected value foobar and foobaz
	if val := txn1.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn1.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}

	// Delete of foozip will cause a merge child between the
	// "foo" and "ba" nodes.
	txn2.Delete([]byte("foozip"))

	// Insert of "foobaz" will update the slice of the "fooba" node
	// in-place to point to the new "foobaz" node. This in-place update
	// will cause the visibility of the update to leak into txn1 (prior
	// to the fix).
	txn2.Insert([]byte("foobaz"), lo.ToPtr(44))

	// Ensure we get the expected value foobar and foobaz
	if val := txn1.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn1.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobaz")); val == nil || *val != 44 {
		t.Fatalf("bad: %v", val)
	}

	// Commit txn2
	r = txn2.Commit()

	// Ensure we get the expected value foobar and foobaz
	if val := txn1.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn1.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val := NewTxn(r).Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := NewTxn(r).Get([]byte("foobaz")); val == nil || *val != 44 {
		t.Fatalf("bad: %v", val)
	}
}

func TestClone(t *testing.T) {
	r := New[int]()

	t1 := NewTxn(r)
	t1.Insert([]byte("foo"), lo.ToPtr(7))
	t2 := t1.Clone()

	t1.Insert([]byte("bar"), lo.ToPtr(42))
	t2.Insert([]byte("baz"), lo.ToPtr(43))

	if val := t1.Get([]byte("foo")); val == nil || *val != 7 {
		t.Fatalf("bad foo in t1")
	}
	if val := t2.Get([]byte("foo")); val == nil || *val != 7 {
		t.Fatalf("bad foo in t2")
	}
	if val := t1.Get([]byte("bar")); val == nil || *val != 42 {
		t.Fatalf("bad bar in t1")
	}
	if val := t2.Get([]byte("bar")); val != nil {
		t.Fatalf("bar found in t2")
	}
	if val := t1.Get([]byte("baz")); val != nil {
		t.Fatalf("baz found in t1")
	}
	if val := t2.Get([]byte("baz")); val == nil || *val != 43 {
		t.Fatalf("bad baz in t2")
	}
}

func randomString(t *testing.T) string {
	var gen [16]byte
	_, err := rand.Read(gen[:])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return hex.EncodeToString(gen[:])
}
