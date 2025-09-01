package disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilterPut(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	bf.Put([]byte("k1"))

	containsKey, err := bf.ContainsKey([]byte("k1"))

	if err != nil {
		t.Errorf("test failed due to error : %s", err.Error())
	}

	assert.True(t, containsKey)

	containsKey, err = bf.ContainsKey([]byte("k3"))

	if err != nil {
		t.Errorf("test failed due to error : %s", err.Error())
	}

	assert.False(t, containsKey)
}
