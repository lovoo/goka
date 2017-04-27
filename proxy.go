package goka

import (
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/storage"
)

type proxy struct {
	partition int32
	consumer  kafka.Consumer
}

func (p *proxy) Add(topic string, offset int64) {
	p.consumer.AddPartition(topic, p.partition, offset)
}

func (p *proxy) Remove(topic string) {
	p.consumer.RemovePartition(topic, p.partition)
}

func (p *proxy) AddGroup() {
	p.consumer.AddGroupPartition(p.partition)
}

func (p *proxy) Stop() {}

type storageProxy struct {
	storage.Storage
	partition int32
	stateless bool
	update    UpdateCallback
}

func (s storageProxy) Update(k string, v []byte) error {
	return s.update(s.Storage, s.partition, k, v)
}

func (s storageProxy) Stateless() bool {
	return s.stateless
}
