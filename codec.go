package main

import (
	"encoding/json"
	"errors"

	"github.com/google/btree"
)

// we want to store the window as an ordered list in the state
// so we can retrieve it quickly, deserialise it as an array
// and then filter and aggregate at request time.
//
// we'd like to also to deserialise as a btree when we get a new
// event, so we can insert the new event in order by time, making sure
// that the array is always ordered properly.
//
// The idea here is that we'd like to do as much work on insertion,
// where latency is less of an issue, and minimise the work on request.

type arrayCodec struct{}

func (c *arrayCodec) Encode(value interface{}) ([]byte, error) {
	// all we're really doing here is checking that the inbound is actually
	// an array of Events and then we're storing the JSON representation.
	// if the elements aren't json marshallable then this will return an error
	// I'm not massively convinced we couldn't just marshal value...
	v, ok := value.([]Event)
	if !ok {
		return nil, errors.New("Failure encodoing array")
	}
	return json.Marshal(v)
}

func (c *arrayCodec) Decode(data []byte) (interface{}, error) {
	var v []Event
	err := json.Unmarshal(data, &v)
	return v, err
}

type btreeCodec struct{}

func (b *btreeCodec) Encode(value interface{}) ([]byte, error) {
	// Ascend through the btree creating an array, then serialise the array
	// feeling relatively happy this is a time sorted array
	tree := value.(*btree.BTree)
	out := make([]Event, tree.Len())
	i := 0
	tree.Ascend(func(item btree.Item) bool {
		event := item.(Event)
		out[i] = event
		i++
		return true
	})
	return json.Marshal(out)
}

func (b *btreeCodec) Decode(data []byte) (interface{}, error) {
	// json unmarshal the array
	var v []Event
	err := json.Unmarshal(data, &v)
	if err != nil {
		return nil, err
	}
	// unpack the array into a btree using
	tree := btree.New(2)
	for _, x := range v {
		dup := tree.ReplaceOrInsert(x)
		if dup != nil {
			return nil, errors.New("duplicate event inserted")
		}
	}
	return tree, nil
}

type eventCodec struct{}

func (e *eventCodec) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(Event)
	if !ok {
		return nil, errors.New("Failure encodoing array")
	}
	return json.Marshal(v)
}

func (e *eventCodec) Decode(data []byte) (interface{}, error) {
	var v Event
	err := json.Unmarshal(data, &v)
	return v, err
}
