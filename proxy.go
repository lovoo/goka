package goka

import (
	"github.com/lovoo/goka/storage"
)

type storageProxy struct {
	storage.Storage
	topic     Stream
	partition int32
	stateless bool
	update    UpdateCallback

	openedOnce once
	closedOnce once
}

func (s *storageProxy) Open() error {
	if s == nil {
		return nil
	}
	return s.openedOnce.Do(s.Storage.Open)
}

func (s *storageProxy) Close() error {
	if s == nil {
		return nil
	}
	return s.closedOnce.Do(s.Storage.Close)
}

func (s *storageProxy) Update(k string, v []byte, offset int64, headers Headers) error {
	return s.update(&DefaultUpdateContext{
		storage:   s,
		topic:     s.topic,
		partition: s.partition,
		offset:    offset,
		key:       k,
		value:     v,
		headers:   headers,
	})
}

func (s *storageProxy) Stateless() bool {
	return s.stateless
}

func (s *storageProxy) MarkRecovered() error {
	return s.Storage.MarkRecovered()
}
