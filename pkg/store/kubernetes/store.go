package kubernetes

import (
	"fmt"
	"github.com/fagongzi/gateway/pkg/pb/metapb"
	"github.com/fagongzi/gateway/pkg/pb/rpcpb"
	"github.com/fagongzi/gateway/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sync"
)

const (
	configKey = "rawData"
)

type Store struct {
	sync.RWMutex

	client kubernetes.Interface
	// namespace here determines which namespace the gateway will store the metadata under in k8s
	namespace           string
	prefix              string
	clustersPrefix      string
	serversPrefix       string
	bindsPrefix         string
	apisPrefix          string
	proxiesPrefix       string
	routingsPrefix      string
	pluginsPrefix       string
	appliedPluginPrefix string
	idPath              string
}

func init() {
	store.SupportSchema["k8s"] = getStoreFrom
}

func getStoreFrom(addr, prefix string, basicAuth store.BasicAuth) (store.Store, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", "")
		if err != nil {
			return nil, err
		}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	s := &Store{
		clustersPrefix:      fmt.Sprintf("%s_clusters", prefix),
		serversPrefix:       fmt.Sprintf("%s_servers", prefix),
		bindsPrefix:         fmt.Sprintf("%s_binds", prefix),
		apisPrefix:          fmt.Sprintf("%s_apis", prefix),
		proxiesPrefix:       fmt.Sprintf("%s_proxies", prefix),
		routingsPrefix:      fmt.Sprintf("%s_routings", prefix),
		pluginsPrefix:       fmt.Sprintf("%s_plugins", prefix),
		appliedPluginPrefix: fmt.Sprintf("%s_applied_plugins", prefix),
		idPath:              fmt.Sprintf("%s_id", prefix),
		client:              client,
		namespace:           "api-gateway",
	}
	return s, nil
}

func (s *Store) Raw() interface{} {
	return s.client
}

func (s *Store) AddBind(bind *metapb.Bind) error {
	panic("implement me")
}

func (s *Store) RemoveBind(bind *metapb.Bind) error {
	panic("implement me")
}

func (s *Store) RemoveClusterBind(id uint64) error {
	panic("implement me")
}

func (s *Store) GetBindServers(id uint64) ([]uint64, error) {
	panic("implement me")
}

func (s *Store) PutCluster(cluster *metapb.Cluster) (uint64, error) {
	panic("implement me")
}

func (s *Store) RemoveCluster(id uint64) error {
	panic("implement me")
}

func (s *Store) GetClusters(limit int64, fn func(interface{}) error) error {
	panic("implement me")
}

func (s *Store) GetCluster(id uint64) (*metapb.Cluster, error) {
	s.RLock()
	defer s.RUnlock()

	cls := new(metapb.Cluster)
	err := s.getPB(s.prefix, id, cls)
	return cls, err
}

func (s *Store) PutServer(svr *metapb.Server) (uint64, error) {
	panic("implement me")
}

func (s *Store) RemoveServer(id uint64) error {
	panic("implement me")
}

func (s *Store) GetServers(limit int64, fn func(interface{}) error) error {
	panic("implement me")
}

func (s *Store) GetServer(id uint64) (*metapb.Server, error) {
	s.RLock()
	defer s.RUnlock()

	svr := new(metapb.Server)
	err := s.getPB(s.prefix, id, svr)
	return svr, err
}

func (s *Store) PutAPI(api *metapb.API) (uint64, error) {
	panic("implement me")
}

func (s *Store) RemoveAPI(id uint64) error {
	panic("implement me")
}

func (s *Store) GetAPIs(limit int64, fn func(interface{}) error) error {
	panic("implement me")
}

func (s *Store) GetAPI(id uint64) (*metapb.API, error) {
	s.RLock()
	defer s.RUnlock()

	api := new(metapb.API)
	err := s.getPB(s.prefix, id, api)
	return api, err
}

func (s *Store) PutRouting(routing *metapb.Routing) (uint64, error) {
	panic("implement me")
}

func (s *Store) RemoveRouting(id uint64) error {
	panic("implement me")
}

func (s *Store) GetRoutings(limit int64, fn func(interface{}) error) error {
	panic("implement me")
}

func (s *Store) GetRouting(id uint64) (*metapb.Routing, error) {
	s.RLock()
	defer s.RUnlock()

	routing := new(metapb.Routing)
	err := s.getPB(s.prefix, id, routing)
	return routing, err
}

func (s *Store) PutPlugin(plugin *metapb.Plugin) (uint64, error) {
	panic("implement me")
}

func (s *Store) RemovePlugin(id uint64) error {
	panic("implement me")
}

func (s *Store) GetPlugins(limit int64, fn func(interface{}) error) error {
	panic("implement me")
}

func (s *Store) GetPlugin(id uint64) (*metapb.Plugin, error) {
	s.RLock()
	defer s.RUnlock()

	plg := new(metapb.Plugin)
	err := s.getPB(s.prefix, id, plg)
	return plg, err
}

func (s *Store) ApplyPlugins(applied *metapb.AppliedPlugins) error {
	panic("implement me")
}

func (s *Store) GetAppliedPlugins() (*metapb.AppliedPlugins, error) {
	panic("implement me")
}

func (s *Store) RegistryProxy(proxy *metapb.Proxy, ttl int64) error {
	panic("implement me")
}

func (s *Store) GetProxies(limit int64, fn func(*metapb.Proxy) error) error {
	panic("implement me")
}

func (s *Store) Clean() error {
	panic("implement me")
}

func (s *Store) SetID(id uint64) error {
	panic("implement me")
}

func (s *Store) BackupTo(to string) error {
	panic("implement me")
}

func (s *Store) Batch(batch *rpcpb.BatchReq) (*rpcpb.BatchRsp, error) {
	panic("implement me")
}

func (s *Store) System() (*metapb.System, error) {
	panic("implement me")
}

func getKey(prefix string, id uint64) string {
	return fmt.Sprintf("%s_%020d", prefix, id)
}

func (s *Store) getPB(prefix string, id uint64, value store.PB) error {
	return s.getPBWithKey(getKey(prefix, id), value, false)
}

func (s *Store) getPBWithKey(key string, value store.PB, allowNotFound bool) error {
	data, err := s.getValue(key)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		if allowNotFound {
			return nil
		}

		return fmt.Errorf("<%s> not found", key)
	}

	err = value.Unmarshal(data)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) getValue(key string) ([]byte, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(key, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	data, ok := cm.Data[configKey]
	if !ok {
		return nil, store.ErrInvalidConfig
	}
	return []byte(data), nil
}
