package etcd

import (
	"fmt"
	"github.com/fagongzi/gateway/pkg/store"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/fagongzi/gateway/pkg/pb/metapb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
)

// Watch watch event from etcd
func (e *Store) Watch(evtCh chan *store.Evt, stopCh chan bool) error {
	e.evtCh = evtCh

	log.Infof("watch event at: <%s>",
		e.prefix)

	e.doWatch()

	return nil
}

func (e *Store) doWatch() {
	watcher := clientv3.NewWatcher(e.rawClient)
	defer watcher.Close()

	ctx := e.rawClient.Ctx()
	for {
		rch := watcher.Watch(ctx, e.prefix, clientv3.WithPrefix())
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				var evtSrc store.EvtSrc
				var evtType store.EvtType

				switch ev.Type {
				case mvccpb.DELETE:
					evtType = store.EventTypeDelete
				case mvccpb.PUT:
					if ev.IsCreate() {
						evtType = store.EventTypeNew
					} else if ev.IsModify() {
						evtType = store.EventTypeUpdate
					}
				}

				key := string(ev.Kv.Key)
				if strings.HasPrefix(key, e.clustersDir) {
					evtSrc = store.EventSrcCluster
				} else if strings.HasPrefix(key, e.serversDir) {
					evtSrc = store.EventSrcServer
				} else if strings.HasPrefix(key, e.bindsDir) {
					evtSrc = store.EventSrcBind
				} else if strings.HasPrefix(key, e.apisDir) {
					evtSrc = store.EventSrcAPI
				} else if strings.HasPrefix(key, e.routingsDir) {
					evtSrc = store.EventSrcRouting
				} else if strings.HasPrefix(key, e.proxiesDir) {
					evtSrc = store.EventSrcProxy
				} else if strings.HasPrefix(key, e.pluginsDir) {
					evtSrc = store.EventSrcPlugin
				} else if strings.HasPrefix(key, e.appliedPluginDir) {
					evtSrc = store.EventSrcApplyPlugin
				} else {
					continue
				}

				log.Debugf("watch event: <%s, %v>",
					key,
					evtType)
				e.evtCh <- e.watchMethodMapping[evtSrc](evtType, ev.Kv)
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

func (e *Store) doWatchWithCluster(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	value := &metapb.Cluster{}
	if len(kv.Value) > 0 {
		protoc.MustUnmarshal(value, []byte(kv.Value))
	}

	return &store.Evt{
		Src:   store.EventSrcCluster,
		Type:  evtType,
		Key:   strings.Replace(string(kv.Key), fmt.Sprintf("%s/", e.clustersDir), "", 1),
		Value: value,
	}
}

func (e *Store) doWatchWithServer(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	value := &metapb.Server{}
	if len(kv.Value) > 0 {
		protoc.MustUnmarshal(value, []byte(kv.Value))
	}

	return &store.Evt{
		Src:   store.EventSrcServer,
		Type:  evtType,
		Key:   strings.Replace(string(kv.Key), fmt.Sprintf("%s/", e.serversDir), "", 1),
		Value: value,
	}
}

func (e *Store) doWatchWithBind(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	// bind key is: bindsDir/clusterID/serverID
	key := strings.Replace(string(kv.Key), fmt.Sprintf("%s/", e.bindsDir), "", 1)
	infos := strings.SplitN(key, "/", 2)

	return &store.Evt{
		Src:  store.EventSrcBind,
		Type: evtType,
		Key:  string(kv.Key),
		Value: &metapb.Bind{
			ClusterID: format.MustParseStrUInt64(infos[0]),
			ServerID:  format.MustParseStrUInt64(infos[1]),
		},
	}
}

func (e *Store) doWatchWithAPI(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	value := &metapb.API{}
	if len(kv.Value) > 0 {
		protoc.MustUnmarshal(value, []byte(kv.Value))
	}

	return &store.Evt{
		Src:   store.EventSrcAPI,
		Type:  evtType,
		Key:   strings.Replace(string(kv.Key), fmt.Sprintf("%s/", e.apisDir), "", 1),
		Value: value,
	}
}

func (e *Store) doWatchWithRouting(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	value := &metapb.Routing{}
	if len(kv.Value) > 0 {
		protoc.MustUnmarshal(value, []byte(kv.Value))
	}

	return &store.Evt{
		Src:   store.EventSrcRouting,
		Type:  evtType,
		Key:   strings.Replace(string(kv.Key), fmt.Sprintf("%s/", e.routingsDir), "", 1),
		Value: value,
	}
}

func (e *Store) doWatchWithProxy(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	value := &metapb.Proxy{}
	if len(kv.Value) > 0 {
		protoc.MustUnmarshal(value, []byte(kv.Value))
	}

	return &store.Evt{
		Src:   store.EventSrcProxy,
		Type:  evtType,
		Key:   strings.Replace(string(kv.Key), fmt.Sprintf("%s/", e.proxiesDir), "", 1),
		Value: value,
	}
}

func (e *Store) doWatchWithPlugin(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	value := &metapb.Plugin{}
	if len(kv.Value) > 0 {
		protoc.MustUnmarshal(value, []byte(kv.Value))
	}

	return &store.Evt{
		Src:   store.EventSrcPlugin,
		Type:  evtType,
		Key:   strings.Replace(string(kv.Key), fmt.Sprintf("%s/", e.pluginsDir), "", 1),
		Value: value,
	}
}

func (e *Store) doWatchWithApplyPlugin(evtType store.EvtType, kv *mvccpb.KeyValue) *store.Evt {
	value := &metapb.AppliedPlugins{}
	if len(kv.Value) > 0 {
		protoc.MustUnmarshal(value, []byte(kv.Value))
	}

	return &store.Evt{
		Src:   store.EventSrcApplyPlugin,
		Type:  evtType,
		Value: value,
	}
}

func (e *Store) init() {
	e.watchMethodMapping[store.EventSrcBind] = e.doWatchWithBind
	e.watchMethodMapping[store.EventSrcServer] = e.doWatchWithServer
	e.watchMethodMapping[store.EventSrcCluster] = e.doWatchWithCluster
	e.watchMethodMapping[store.EventSrcAPI] = e.doWatchWithAPI
	e.watchMethodMapping[store.EventSrcRouting] = e.doWatchWithRouting
	e.watchMethodMapping[store.EventSrcProxy] = e.doWatchWithProxy
	e.watchMethodMapping[store.EventSrcPlugin] = e.doWatchWithPlugin
	e.watchMethodMapping[store.EventSrcApplyPlugin] = e.doWatchWithApplyPlugin
}
