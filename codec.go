package gomsg

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/quintans/toolkit/faults"
)

type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

type GobCodec struct {
}

var _ Codec = GobCodec{}

func (this GobCodec) Encode(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return buf.Bytes(), nil
}

func (this GobCodec) Decode(payload []byte, p interface{}) error {
	var buf bytes.Buffer
	buf.Write(payload)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(p)
	if err != nil {
		return faults.Wrap(err)
	}
	return nil
}

type JsonCodec struct {
}

var _ Codec = JsonCodec{}

func (this JsonCodec) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return b, nil
}

func (this JsonCodec) Decode(payload []byte, p interface{}) error {
	err := json.Unmarshal(payload, p)
	if err != nil {
		return faults.Wrap(err)
	}
	return nil
}
