package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitVectorSet(t *testing.T) {
	bv := NewBitVector(5)
	err := bv.Set(1)

	if err != nil {
		t.Errorf("bit vector Set error : %s", err.Error())
	}

	isSet, err := bv.IsSet(1)

	if err != nil {
		t.Errorf("bit vector IsSet error : %s", err.Error())
	}

	assert.True(t, isSet)

	isSet, err = bv.IsSet(2)

	if err != nil {
		t.Errorf("bit vector IsSet error : %s", err.Error())
	}

	assert.Equal(t, isSet, false)

}
