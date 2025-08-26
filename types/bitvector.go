package types

import (
	"fmt"
	"math"
)

type BitVector struct {
	vector []byte
	length int
}

func NewBitVector(length int) BitVector {
	return BitVector{
		vector: make([]byte, (length+7)/8),
		length: length,
	}
}

func (b *BitVector) set(index int) error {
	if index > b.length {
		return NewEngineError(
			BIT_VECTOR_OUT_OF_BOUNDS,
			fmt.Sprintf("index %d is beyond bounds %d", index, b.length),
		)
	}

	pos := index / 8
	i := index % 8

	b.vector[pos] = b.vector[pos] ^ (1 << i)

	return nil
}

func (b *BitVector) IsSet(index int) (bool, error) {
	if index > b.length {
		return false, NewEngineError(
			BIT_VECTOR_OUT_OF_BOUNDS,
			fmt.Sprintf("index %d is beyond bounds %d", index, b.length),
		)
	}

	pos := index / 8
	i := index % 8

	return (b.vector[pos]&(1<<i) != 0), nil
}
