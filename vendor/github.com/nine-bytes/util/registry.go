package util

import (
	"sync"
)

// ControlRegistry maps a client ID to Control structures
type Registry struct {
	elems map[interface{}]interface{}
	sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{
		elems: make(map[interface{}]interface{}),
	}
}

func (r *Registry) Get(id interface{}) (interface{}, bool) {
	r.RLock()
	defer r.RUnlock()
	return r.elems[id], r.elems[id] != nil
}

func (r *Registry) Add(id, elem interface{}) (oldElem interface{}) {
	r.Lock()         // Lock the ControlRegistry Object
	defer r.Unlock() // Unlock the ControlRegistry when exits method

	oldElem = r.elems[id]
	r.elems[id] = elem // Set the ctl with clientId key

	return
}

func (r *Registry) Del(id, elem interface{}) {
	r.Lock()
	defer r.Unlock()

	if e, ok := r.elems[id]; ok && elem == e {
		delete(r.elems, id)
	}
}
