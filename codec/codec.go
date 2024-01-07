package codec

import (
	"fmt"
	"strconv"
)

// Bytes codec is
type Bytes struct{}

// Encode does a type conversion into []byte
func (d *Bytes) Encode(value interface{}) ([]byte, error) {
	var err error
	data, isByte := value.([]byte)
	if !isByte {
		err = fmt.Errorf("DefaultCodec: value to encode is not of type []byte")
	}
	return data, err
}

// Decode of defaultCodec simply returns the data
func (d *Bytes) Decode(data []byte) (interface{}, error) {
	return data, nil
}

// String is a commonly used codec to encode and decode string <-> []byte
type String struct{}

// Encode encodes from string to []byte
func (c *String) Encode(value interface{}) ([]byte, error) {
	stringVal, isString := value.(string)
	if !isString {
		return nil, fmt.Errorf("String: value to encode is not of type string but %T", value)
	}
	return []byte(stringVal), nil
}

// Decode decodes from []byte to string
func (c *String) Decode(data []byte) (interface{}, error) {
	return string(data), nil
}

// Int64 is a commonly used codec to encode and decode string <-> []byte
type Int64 struct{}

// Encode encodes from string to []byte
func (c *Int64) Encode(value interface{}) ([]byte, error) {
	intVal, isInt := value.(int64)
	if !isInt {
		return nil, fmt.Errorf("Int64: value to encode is not of type int64")
	}
	return []byte(strconv.FormatInt(intVal, 10)), nil
}

// Decode decodes from []byte to string
func (c *Int64) Decode(data []byte) (interface{}, error) {
	intVal, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Error parsing data from string %d: %v", intVal, err)
	}
	return intVal, nil
}
