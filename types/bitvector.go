package types

import (
	"LsmStorageEngine/types"
	"fmt"
	"io"
	"os"
)

type BitVector struct {
	vector []byte
	length int
}

func NewBitVector(length int) BitVector {
	return BitVector{
		vector: make([]byte, (length+7)/8),
		length: (length + 7) / 8,
	}
}

func NewBitSetVectorFromBytes(bytes *[]byte) BitVector {
	return BitVector{
		vector: *bytes,
		length: len(*bytes),
	}
}

func NewBitSetVectorFromFile(file *os.File, length int) (BitVector, error) {
	bytes := make([]byte, length)
	_, err := io.ReadFull(file, bytes)

	if err != nil && err != io.EOF {
		return BitVector{}, types.NewEngineError(
			types.TABLE_READ_FILE_ERROR,
			fmt.Sprintf("file read error : %s", err.Error()),
		)
	}

	return BitVector{
		vector: bytes,
		length: length,
	}, nil
}

func (b *BitVector) Set(index int) error {
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

func (b *BitVector) Bytes() []byte {
	return b.vector
}
