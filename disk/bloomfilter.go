package disk

import (
	"LsmStorageEngine/types"
	"math"
)

// TODO ! create a bit vector
type BloomFilter struct {
	// the biet set vector where we map an input x as h1(x) | h2(x) | .... | hk(x)
	bitSet types.BitVector
	// the size of this bitset (m) = -n*ln(p) / (ln(2)^2), where n is the no of elements and p is the error rate
	bitSetSize int
	// the no of has functions (k) = m/n * ln(2)
	hashFunctionCount int
}

func NewBloomFilter(n float64, p float64) BloomFilter {
	m := (-1 * n * math.Round(math.Log(p))) / math.Pow(math.Log(2), 2)
	k := (m / n) * math.Log(2)

	return BloomFilter{
		bitSet:            types.NewBitVector(int(math.Ceil(m))),
		bitSetSize:        int(m),
		hashFunctionCount: int(k),
	}
}
