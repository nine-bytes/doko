package util

import "sync"

type Set struct {
	set []interface{}
	capacity int
	sync.RWMutex
}

func NewSet(capacity int) *Set {
	return &Set{set: make([]interface{}, 0, capacity), capacity: capacity}
}

func (s *Set) Add(elem interface{}) bool {
	s.Lock()
	defer s.Unlock()

	if s.capacity != 0 && len(s.set) == s.capacity {
		return false
	}

	s.set = append(s.set, elem)
	return true
}

func (s *Set) Remove(elem interface{}) {
	s.Lock()
	defer s.Unlock()

	for i, e := range s.set {
		if e == elem {
			if i+1 == len(s.set) {
				s.set = append(s.set[0:i])
			} else {
				s.set = append(s.set[0:i], s.set[i+1:]...)
			}

			break
		}
	}
}

func (s *Set) Each(callback func(elem interface{})) {
	s.RLock()
	defer s.RUnlock()

	wg := new(sync.WaitGroup)
	wg.Add(len(s.set))
	for _, e := range s.set {
		go func(e interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			callback(e)
		}(e, wg)
	}

	wg.Wait()
}

func (s *Set) Clean() {
	s.Lock()
	defer s.Unlock()

	s.set = s.set[:0]
}

func (s *Set) Len() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.set)
}

func (s *Set) Cap() int {
	return s.capacity
}
