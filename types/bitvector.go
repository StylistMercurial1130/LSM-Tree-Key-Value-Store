package types

type BitVector struct {
	vector []byte
	length int
}

func NewBitVector(length int) BitVector {
	return BitVector{}
}
