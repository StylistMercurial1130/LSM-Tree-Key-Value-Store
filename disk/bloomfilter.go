package disk

import (
	"LsmStorageEngine/types"
	"encoding/binary"
	"math"

	"github.com/spaolacci/murmur3"
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

func (bf *BloomFilter) Put(key []byte) {
	for seed := range bf.hashFunctionCount {
		setLocation := murmur3.Sum64WithSeed(key, uint32(seed)) % uint64(bf.bitSetSize)
		bf.bitSet.Set(int(setLocation))
	}
}

func (bf *BloomFilter) ContainsKey(key []byte) (bool, error) {
	for seed := range bf.hashFunctionCount {
		setLocation := murmur3.Sum64WithSeed(key, uint32(seed)) % uint64(bf.bitSetSize)

		if isSet, err := bf.bitSet.IsSet(int(setLocation)); err != nil {
			return false, err
		} else if isSet == false {
			return false, nil
		}
	}

	return true, nil
}

func (bf *BloomFilter) Serialize() []byte {
	var serializedBf []byte
	bitSet := bf.bitSet.Bytes()

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(len(bitSet)))
	serializedBf = append(serializedBf, b...)
	serializedBf = append(serializedBf, bitSet...)

	return serializedBf
}
