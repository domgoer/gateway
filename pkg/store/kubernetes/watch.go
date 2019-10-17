package kubernetes

import (
	"github.com/fagongzi/gateway/pkg/store"
)

func (s *Store) Watch(evtCh chan *store.Evt, stopCh chan bool) error {
	panic("implement me")
}
