package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type file struct {
	file      io.WriteCloser
	recovered bool

	bytesWritten int64
}

// NewFile retuns a new on-disk storage.
func NewFile(path string, part int32) (Storage, error) {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating storage directory: %v", err)
	}

	f, err := os.OpenFile(filepath.Join(path, fmt.Sprintf("part-%d", part)), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &file{file: f}, nil
}

func (f *file) Recovered() bool {
	return f.recovered
}

func (f *file) MarkRecovered() error {
	f.recovered = true
	return nil
}

func (f *file) Has(key string) (bool, error) {
	return false, nil
}

func (f *file) Get(key string) ([]byte, error) {
	return nil, nil
}

func (f *file) Set(key string, val []byte) error {
	num, err := f.file.Write(val)
	if err != nil {
		return err
	}

	f.bytesWritten += int64(num)

	if _, err := f.file.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

func (f *file) Delete(string) error {
	return nil
}

func (f *file) GetOffset(def int64) (int64, error) {
	return def, nil
}

func (f *file) SetOffset(val int64) error {
	return nil
}

func (f *file) Iterator() (Iterator, error) {
	return new(NullIter), nil
}

func (f *file) IteratorWithRange(start, limit []byte) (Iterator, error) {
	return new(NullIter), nil
}

func (f *file) Open() error {
	return nil
}

func (f *file) Close() error {
	return f.file.Close()
}
