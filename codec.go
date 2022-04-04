package main

import (
	"encoding/json"
	"errors"
)

type arrayCodec struct{}

func (c *arrayCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *arrayCodec) Decode(data []byte) (interface{}, error) {
	var v []Event
	err := json.Unmarshal(data, &v)
	return v, err
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
