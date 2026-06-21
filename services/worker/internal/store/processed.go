package store

import "sync"

type ProcessedStore struct {
    mu    sync.RWMutex
    items map[string]bool
}

func NewProcessedStore() *ProcessedStore {
    return &ProcessedStore{items: make(map[string]bool)}
}

func (s *ProcessedStore) Exists(id string) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.items[id]
}

func (s *ProcessedStore) MarkDone(id string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.items[id] = true
}