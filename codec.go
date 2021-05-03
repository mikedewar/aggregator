package main

import (
	"encoding/json"
	"errors"
)

type arrayCodec struct{}

func (c *arrayCodec) Encode(value interface{}) ([]byte, error) {
	// all we're really doing here is checking that the inbound is actually
	// a []int64 and then we're storing the JSON representation
	v, ok := value.([]int64)
	if !ok {
		return nil, errors.New("Failure encodoing array")
	}
	return json.Marshal(v)
}

func (c *arrayCodec) Decode(data []byte) (interface{}, error) {
	var v []int64
	err := json.Unmarshal(data, &v)
	return v, err
}

type windowCodec struct{}

// Ascend through the btree creating an array, then serialise the array
// feeling relatively happy this is a time sroted array
func (b *windowCodec) Encode(value interface{}) ([]byte, error) {}
