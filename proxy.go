package goka

import (
	"sync"
	"time"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/storage"
)

const (
	delayProxyInterval = 1 * time.Second
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

type delayProxy struct {
	partition int32
	consumer  kafka.Consumer
	stop      bool
	m         sync.Mutex
	wait      []func() bool
}

func (p *delayProxy) Add(topic string, offset int64) {
	p.consumer.AddPartition(topic, p.partition, offset)
}

func (p *delayProxy) Remove(topic string) {
	p.consumer.RemovePartition(topic, p.partition)
}

func (p *delayProxy) AddGroup() {
	if len(p.wait) == 0 {
		p.consumer.AddGroupPartition(p.partition)
		return
	}

	go func() {
		ticker := time.NewTicker(delayProxyInterval)
		defer ticker.Stop()
		for range ticker.C {
			p.m.Lock()
			if p.stop {
				p.m.Unlock()
				return
			}
			// wait for all conditions
			done := true
			for _, r := range p.wait {
				if !r() {
					done = false
					break
				}
			}
			if done {
				p.consumer.AddGroupPartition(p.partition)
			}
			p.m.Unlock()
		}
	}()
}

func (p *delayProxy) Stop() {
	p.m.Lock()
	p.stop = true
	p.m.Unlock()
}

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
