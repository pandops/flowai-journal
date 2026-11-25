package mocks

import (
	"context"
	"sync"
)

// FakeReadiness is a settable httpedge.Readiness.
type FakeReadiness struct {
	mu       sync.Mutex
	Broker   bool
	Consumer bool
	Store    bool
}

func (f *FakeReadiness) BrokerConnected() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.Broker }
func (f *FakeReadiness) ConsumerBound() bool   { f.mu.Lock(); defer f.mu.Unlock(); return f.Consumer }
func (f *FakeReadiness) StoreReady(context.Context) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Store
}

// Set changes all three flags.
func (f *FakeReadiness) Set(broker, consumer, store bool) {
	f.mu.Lock()
	f.Broker, f.Consumer, f.Store = broker, consumer, store
	f.mu.Unlock()
}
