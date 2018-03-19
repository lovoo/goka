package goka

import (
	"fmt"
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

func (p *proxy) Add(topic string, offset int64) error {
	if err := p.consumer.AddPartition(topic, p.partition, offset); err != nil {
		return fmt.Errorf("error adding %s/%d: %v", topic, p.partition, err)
	}
	return nil
}

func (p *proxy) Remove(topic string) error {
	if err := p.consumer.RemovePartition(topic, p.partition); err != nil {
		return fmt.Errorf("error removing %s/%d: %v", topic, p.partition, err)
	}
	return nil
}

func (p *proxy) AddGroup() {
	p.consumer.AddGroupPartition(p.partition)
}

func (p *proxy) Stop() {}

type delayProxy struct {
	proxy
	stop bool
	m    sync.Mutex
	wait []func() bool
}

func (p *delayProxy) waitersDone() bool {
	for _, r := range p.wait {
		if !r() {
			return false
		}
	}
	return true
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
			if p.waitersDone() {
				p.consumer.AddGroupPartition(p.partition)
				p.m.Unlock()
				return
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

func (s *storageProxy) Update(k string, v []byte) error {
	return s.update(s.Storage, s.partition, k, v)
}

func (s *storageProxy) Stateless() bool {
	return s.stateless
}

func (s *storageProxy) MarkRecovered() error {
	return s.Storage.MarkRecovered()
}
