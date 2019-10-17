package etcd

import (
	"fmt"
	"github.com/fagongzi/gateway/pkg/store"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/fagongzi/gateway/pkg/client"
	pbutil "github.com/fagongzi/gateway/pkg/pb"
	"github.com/fagongzi/gateway/pkg/pb/metapb"
	"github.com/fagongzi/gateway/pkg/pb/rpcpb"
	"github.com/fagongzi/gateway/pkg/plugin"
	"github.com/fagongzi/gateway/pkg/route"
	"github.com/fagongzi/gateway/pkg/util"
	"github.com/fagongzi/util/format"
	"golang.org/x/net/context"
)

const (
	batch = uint64(1000)
	endID = uint64(math.MaxUint64)
)

// Store etcd store impl
type Store struct {
	sync.RWMutex

	prefix           string
	clustersDir      string
	serversDir       string
	bindsDir         string
	apisDir          string
	proxiesDir       string
	routingsDir      string
	pluginsDir       string
	appliedPluginDir string
	idPath           string

	idLock sync.Mutex
	base   uint64
	end    uint64

	evtCh              chan *store.Evt
	watchMethodMapping map[store.EvtSrc]func(store.EvtType, *mvccpb.KeyValue) *store.Evt

	rawClient *clientv3.Client
}

func init(){
	store.SupportSchema["etcd"] = getStoreFrom
}

func getStoreFrom(addr, prefix string, basicAuth store.BasicAuth) (store.Store, error) {
	var addrs []string
	values := strings.Split(addr, ",")

	for _, value := range values {
		addrs = append(addrs, fmt.Sprintf("http://%s", value))
	}

	return NewStore(addrs, prefix, basicAuth)
}

// NewStore create a etcd store
func NewStore(etcdAddrs []string, prefix string, basicAuth store.BasicAuth) (store.Store, error) {
	s := &Store{
		prefix:             prefix,
		clustersDir:        fmt.Sprintf("%s/clusters", prefix),
		serversDir:         fmt.Sprintf("%s/servers", prefix),
		bindsDir:           fmt.Sprintf("%s/binds", prefix),
		apisDir:            fmt.Sprintf("%s/apis", prefix),
		proxiesDir:         fmt.Sprintf("%s/proxies", prefix),
		routingsDir:        fmt.Sprintf("%s/routings", prefix),
		pluginsDir:         fmt.Sprintf("%s/plugins", prefix),
		appliedPluginDir:   fmt.Sprintf("%s/applied/plugins", prefix),
		idPath:             fmt.Sprintf("%s/id", prefix),
		watchMethodMapping: make(map[store.EvtSrc]func(store.EvtType, *mvccpb.KeyValue) *store.Evt),
		base:               100,
		end:                100,
	}

	config := &clientv3.Config{
		Endpoints:   etcdAddrs,
		DialTimeout: store.DefaultTimeout,
	}
	if basicAuth.UserName != "" {
		config.Username = basicAuth.UserName
	}
	if basicAuth.Password != "" {
		config.Password = basicAuth.Password
	}

	cli, err := clientv3.New(*config)

	if err != nil {
		return nil, err
	}

	s.rawClient = cli

	s.init()
	return s, nil
}

// Raw returns the raw client
func (e *Store) Raw() interface{} {
	return e.rawClient
}

// AddBind bind a server to a cluster
func (e *Store) AddBind(bind *metapb.Bind) error {
	e.Lock()
	defer e.Unlock()

	data, err := bind.Marshal()
	if err != nil {
		return err
	}

	return e.put(e.getBindKey(bind), string(data))
}

// Batch batch update
func (e *Store) Batch(batch *rpcpb.BatchReq) (*rpcpb.BatchRsp, error) {
	e.Lock()
	defer e.Unlock()

	rsp := &rpcpb.BatchRsp{}
	ops := make([]clientv3.Op, 0, len(batch.PutServers))
	for _, req := range batch.PutServers {
		value := &req.Server
		err := pbutil.ValidateServer(value)
		if err != nil {
			return nil, err
		}

		op, err := e.putPBWithOp(e.serversDir, value, func(id uint64) {
			value.ID = id
			rsp.PutServers = append(rsp.PutServers, &rpcpb.PutServerRsp{
				ID: id,
			})
		})
		if err != nil {
			return nil, err
		}

		ops = append(ops, op)
	}
	err := e.putBatch(ops...)
	if err != nil {
		return nil, err
	}

	ops = make([]clientv3.Op, 0, len(batch.PutClusters))
	for _, req := range batch.PutClusters {
		value := &req.Cluster
		err := pbutil.ValidateCluster(value)
		if err != nil {
			return nil, err
		}

		op, err := e.putPBWithOp(e.clustersDir, value, func(id uint64) {
			value.ID = id
			rsp.PutClusters = append(rsp.PutClusters, &rpcpb.PutClusterRsp{
				ID: id,
			})
		})
		if err != nil {
			return nil, err
		}

		ops = append(ops, op)
	}
	err = e.putBatch(ops...)
	if err != nil {
		return nil, err
	}

	ops = make([]clientv3.Op, 0, len(batch.AddBinds))
	for _, req := range batch.AddBinds {
		value := &metapb.Bind{
			ClusterID: req.Cluster,
			ServerID:  req.Server,
		}

		data, err := value.Marshal()
		if err != nil {
			return nil, err
		}

		ops = append(ops, e.op(e.getBindKey(value), string(data)))
		rsp.AddBinds = append(rsp.AddBinds, &rpcpb.AddBindRsp{})
	}

	err = e.putBatch(ops...)
	if err != nil {
		return nil, err
	}

	ops = make([]clientv3.Op, 0, len(batch.PutAPIs))
	for _, req := range batch.PutAPIs {
		value := &req.API
		err := pbutil.ValidateAPI(value)
		if err != nil {
			return nil, err
		}

		op, err := e.putPBWithOp(e.apisDir, value, func(id uint64) {
			value.ID = id
			rsp.PutAPIs = append(rsp.PutAPIs, &rpcpb.PutAPIRsp{
				ID: id,
			})
		})
		if err != nil {
			return nil, err
		}

		ops = append(ops, op)
	}
	err = e.putBatch(ops...)
	if err != nil {
		return nil, err
	}

	ops = make([]clientv3.Op, 0, len(batch.PutRoutings))
	for _, req := range batch.PutRoutings {
		value := &req.Routing
		err := pbutil.ValidateRouting(value)
		if err != nil {
			return nil, err
		}

		op, err := e.putPBWithOp(e.routingsDir, value, func(id uint64) {
			value.ID = id
			rsp.PutRoutings = append(rsp.PutRoutings, &rpcpb.PutRoutingRsp{
				ID: id,
			})
		})
		if err != nil {
			return nil, err
		}

		ops = append(ops, op)
	}
	err = e.putBatch(ops...)
	if err != nil {
		return nil, err
	}

	ops = make([]clientv3.Op, 0, len(batch.PutPlugins))
	for _, req := range batch.PutPlugins {
		value := &req.Plugin
		_, err := plugin.NewRuntime(value)
		if err != nil {
			return nil, err
		}

		op, err := e.putPBWithOp(e.pluginsDir, value, func(id uint64) {
			value.ID = id
			rsp.PutPlugins = append(rsp.PutPlugins, &rpcpb.PutPluginRsp{
				ID: id,
			})
		})
		if err != nil {
			return nil, err
		}

		ops = append(ops, op)
	}
	if batch.ApplyPlugins != nil {
		op, err := e.putPBKeyWithOp(e.appliedPluginDir, &batch.ApplyPlugins.Applied)
		if err != nil {
			return nil, err
		}

		ops = append(ops, op)
	}

	err = e.putBatch(ops...)
	if err != nil {
		return nil, err
	}

	return rsp, nil
}

// RemoveBind remove bind
func (e *Store) RemoveBind(bind *metapb.Bind) error {
	e.Lock()
	defer e.Unlock()

	return e.delete(e.getBindKey(bind))
}

// RemoveClusterBind remove cluster all bind servers
func (e *Store) RemoveClusterBind(id uint64) error {
	e.Lock()
	defer e.Unlock()

	return e.delete(e.getClusterBindPrefix(id), clientv3.WithPrefix())
}

// GetBindServers return cluster binds servers
func (e *Store) GetBindServers(id uint64) ([]uint64, error) {
	e.RLock()
	defer e.RUnlock()

	return e.doGetBindServers(id)
}

func (e *Store) doGetBindServers(id uint64) ([]uint64, error) {
	rsp, err := e.get(e.getClusterBindPrefix(id), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return nil, nil
	}

	var values []uint64
	for _, item := range rsp.Kvs {
		v := &metapb.Bind{}
		err := v.Unmarshal(item.Value)
		if err != nil {
			return nil, err
		}

		values = append(values, v.ServerID)
	}

	return values, nil
}

// PutCluster add or update the cluster
func (e *Store) PutCluster(value *metapb.Cluster) (uint64, error) {
	e.Lock()
	defer e.Unlock()

	err := pbutil.ValidateCluster(value)
	if err != nil {
		return 0, err
	}

	return e.putPB(e.clustersDir, value, func(id uint64) {
		value.ID = id
	})
}

// RemoveCluster remove the cluster and it's binds
func (e *Store) RemoveCluster(id uint64) error {
	e.Lock()
	defer e.Unlock()

	opCluster := clientv3.OpDelete(getKey(e.clustersDir, id))
	opBind := clientv3.OpDelete(e.getClusterBindPrefix(id), clientv3.WithPrefix())
	_, err := e.txn().Then(opCluster, opBind).Commit()
	return err
}

// GetClusters returns all clusters
func (e *Store) GetClusters(limit int64, fn func(interface{}) error) error {
	e.RLock()
	defer e.RUnlock()

	return e.getValues(e.clustersDir, limit, func() store.PB { return &metapb.Cluster{} }, fn)
}

// GetCluster returns the cluster
func (e *Store) GetCluster(id uint64) (*metapb.Cluster, error) {
	e.RLock()
	defer e.RUnlock()

	value := &metapb.Cluster{}
	return value, e.getPB(e.clustersDir, id, value)
}

// PutServer add or update the server
func (e *Store) PutServer(value *metapb.Server) (uint64, error) {
	e.Lock()
	defer e.Unlock()

	err := pbutil.ValidateServer(value)
	if err != nil {
		return 0, err
	}

	return e.putPB(e.serversDir, value, func(id uint64) {
		value.ID = id
	})
}

// RemoveServer remove the server
func (e *Store) RemoveServer(id uint64) error {
	e.Lock()
	defer e.Unlock()

	return e.delete(getKey(e.serversDir, id))
}

// GetServers returns all server
func (e *Store) GetServers(limit int64, fn func(interface{}) error) error {
	e.RLock()
	defer e.RUnlock()

	return e.getValues(e.serversDir, limit, func() store.PB { return &metapb.Server{} }, fn)
}

// GetServer returns the server
func (e *Store) GetServer(id uint64) (*metapb.Server, error) {
	e.RLock()
	defer e.RUnlock()

	value := &metapb.Server{}
	return value, e.getPB(e.serversDir, id, value)
}

// PutAPI add or update a API
func (e *Store) PutAPI(value *metapb.API) (uint64, error) {
	err := pbutil.ValidateAPI(value)
	if err != nil {
		return 0, err
	}

	e.Lock()
	defer e.Unlock()

	// load all api every times for validate
	// TODO: maybe need optimization if there are too much apis
	apiRoute := route.NewRoute()
	e.getValues(e.apisDir, 64, func() store.PB { return &metapb.API{} }, func(data interface{}) error {
		v := data.(*metapb.API)
		if v.ID != value.ID && v.Status == metapb.Up {
			apiRoute.Add(v)
		}
		return nil
	})

	if value.Status == metapb.Up {
		err = apiRoute.Add(value)
		if err != nil {
			return 0, err
		}
	}

	return e.putPB(e.apisDir, value, func(id uint64) {
		value.ID = id
	})
}

// RemoveAPI remove a api from store
func (e *Store) RemoveAPI(id uint64) error {
	e.Lock()
	defer e.Unlock()

	return e.delete(getKey(e.apisDir, id))
}

// GetAPIs returns all api
func (e *Store) GetAPIs(limit int64, fn func(interface{}) error) error {
	e.RLock()
	defer e.RUnlock()

	return e.getValues(e.apisDir, limit, func() store.PB { return &metapb.API{} }, fn)
}

// GetAPI returns the api
func (e *Store) GetAPI(id uint64) (*metapb.API, error) {
	e.RLock()
	defer e.RUnlock()

	value := &metapb.API{}
	return value, e.getPB(e.apisDir, id, value)
}

// PutRouting add or update routing
func (e *Store) PutRouting(value *metapb.Routing) (uint64, error) {
	e.Lock()
	defer e.Unlock()

	err := pbutil.ValidateRouting(value)
	if err != nil {
		return 0, err
	}

	return e.putPB(e.routingsDir, value, func(id uint64) {
		value.ID = id
	})
}

// RemoveRouting remove routing
func (e *Store) RemoveRouting(id uint64) error {
	e.Lock()
	defer e.Unlock()

	return e.delete(getKey(e.routingsDir, id))
}

// GetRoutings returns routes in store
func (e *Store) GetRoutings(limit int64, fn func(interface{}) error) error {
	e.RLock()
	defer e.RUnlock()

	return e.getValues(e.routingsDir, limit, func() store.PB { return &metapb.Routing{} }, fn)
}

// GetRouting returns a routing
func (e *Store) GetRouting(id uint64) (*metapb.Routing, error) {
	e.RLock()
	defer e.RUnlock()

	value := &metapb.Routing{}
	return value, e.getPB(e.routingsDir, id, value)
}

// PutPlugin add or update the plugin
func (e *Store) PutPlugin(value *metapb.Plugin) (uint64, error) {
	e.Lock()
	defer e.Unlock()

	err := pbutil.ValidatePlugin(value)
	if err != nil {
		return 0, err
	}

	value.UpdateAt = util.NowWithMillisecond()
	return e.putPB(e.pluginsDir, value, func(id uint64) {
		value.ID = id
	})
}

// RemovePlugin remove the plugin
func (e *Store) RemovePlugin(id uint64) error {
	e.Lock()
	defer e.Unlock()

	applied, err := e.doGetAppliedPlugins()
	if err != nil {
		return err
	}

	for _, appliedID := range applied.AppliedIDs {
		if id == appliedID {
			return fmt.Errorf("%d is already applied", id)
		}
	}

	return e.delete(getKey(e.pluginsDir, id))
}

// GetPlugins returns plugins in store
func (e *Store) GetPlugins(limit int64, fn func(interface{}) error) error {
	e.RLock()
	defer e.RUnlock()

	return e.getValues(e.pluginsDir, limit, func() store.PB { return &metapb.Plugin{} }, fn)
}

// GetPlugin returns the plugin
func (e *Store) GetPlugin(id uint64) (*metapb.Plugin, error) {
	e.RLock()
	defer e.RUnlock()

	value := &metapb.Plugin{}
	return value, e.getPB(e.pluginsDir, id, value)
}

// ApplyPlugins apply plugins
func (e *Store) ApplyPlugins(value *metapb.AppliedPlugins) error {
	e.Lock()
	defer e.Unlock()

	data, err := value.Marshal()
	if err != nil {
		return err
	}

	return e.put(e.appliedPluginDir, string(data))
}

// GetAppliedPlugins returns applied plugins
func (e *Store) GetAppliedPlugins() (*metapb.AppliedPlugins, error) {
	e.RLock()
	defer e.RUnlock()

	return e.doGetAppliedPlugins()
}

func (e *Store) doGetAppliedPlugins() (*metapb.AppliedPlugins, error) {
	value := &metapb.AppliedPlugins{}
	return value, e.getPBWithKey(e.appliedPluginDir, value, true)
}

// RegistryProxy registry
func (e *Store) RegistryProxy(proxy *metapb.Proxy, ttl int64) error {
	key := getAddrKey(e.proxiesDir, proxy.Addr)
	data, err := proxy.Marshal()
	if err != nil {
		return err
	}

	lessor := clientv3.NewLease(e.rawClient)
	defer lessor.Close()

	ctx, cancel := context.WithTimeout(e.rawClient.Ctx(), store.DefaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, ttl)
	cancel()
	if err != nil {
		return err
	}

	_, err = e.rawClient.KeepAlive(e.rawClient.Ctx(), leaseResp.ID)
	if err != nil {
		return err
	}

	return e.put(key, string(data), clientv3.WithLease(leaseResp.ID))
}

// GetProxies returns proxies in store
func (e *Store) GetProxies(limit int64, fn func(*metapb.Proxy) error) error {
	start := util.MinAddrFormat
	end := getAddrKey(e.proxiesDir, util.MaxAddrFormat)
	withRange := clientv3.WithRange(end)
	withLimit := clientv3.WithLimit(limit)

	for {
		resp, err := e.get(getAddrKey(e.proxiesDir, start), withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			value := &metapb.Proxy{}
			err := value.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			fn(value)

			start = util.GetAddrNextFormat(value.Addr)
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

// Clean clean data in store
func (e *Store) Clean() error {
	e.Lock()
	defer e.Unlock()

	return e.delete(e.prefix, clientv3.WithPrefix())
}

// SetID set id
func (e *Store) SetID(id uint64) error {
	e.Lock()
	defer e.Unlock()

	op := clientv3.OpPut(e.idPath, string(format.Uint64ToBytes(id)))
	rsp, err := e.txn().Then(op).Commit()
	if err != nil {
		return err
	}

	if !rsp.Succeeded {
		return store.ErrStaleOP
	}

	e.end = 0
	e.base = 0
	return nil
}

// BackupTo backup to other gateway
func (e *Store) BackupTo(to string) error {
	e.Lock()
	defer e.Unlock()

	targetC, err := client.NewClient(time.Second*10, to)
	if err != nil {
		return err
	}

	defer targetC.Close()

	// Clean
	err = targetC.Clean()
	if err != nil {
		return err
	}

	limit := int64(96)
	batch := &rpcpb.BatchReq{}

	// backup server
	err = e.getValues(e.serversDir, limit, func() store.PB { return &metapb.Server{} }, func(value interface{}) error {
		batch.PutServers = append(batch.PutServers, &rpcpb.PutServerReq{
			Server: *value.(*metapb.Server),
		})

		if int64(len(batch.PutServers)) == limit {
			_, err := targetC.Batch(batch)
			if err != nil {
				return err
			}

			batch = &rpcpb.BatchReq{}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if int64(len(batch.PutServers)) > 0 {
		_, err := targetC.Batch(batch)
		if err != nil {
			return err
		}
	}

	// backup cluster
	batch = &rpcpb.BatchReq{}
	err = e.getValues(e.clustersDir, limit, func() store.PB { return &metapb.Cluster{} }, func(value interface{}) error {
		batch.PutClusters = append(batch.PutClusters, &rpcpb.PutClusterReq{
			Cluster: *value.(*metapb.Cluster),
		})

		if int64(len(batch.PutClusters)) == limit {
			_, err := targetC.Batch(batch)
			if err != nil {
				return err
			}

			batch = &rpcpb.BatchReq{}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if int64(len(batch.PutClusters)) > 0 {
		_, err := targetC.Batch(batch)
		if err != nil {
			return err
		}
	}

	// backup binds
	batch = &rpcpb.BatchReq{}
	err = e.getValues(e.clustersDir, limit, func() store.PB { return &metapb.Cluster{} }, func(value interface{}) error {
		cid := value.(*metapb.Cluster).ID
		servers, err := e.doGetBindServers(cid)
		if err != nil {
			return err
		}

		for _, sid := range servers {
			batch.AddBinds = append(batch.AddBinds, &rpcpb.AddBindReq{
				Cluster: cid,
				Server:  sid,
			})

			if int64(len(batch.AddBinds)) == limit {
				_, err := targetC.Batch(batch)
				if err != nil {
					return err
				}

				batch = &rpcpb.BatchReq{}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if int64(len(batch.AddBinds)) > 0 {
		_, err := targetC.Batch(batch)
		if err != nil {
			return err
		}

		batch = &rpcpb.BatchReq{}
	}

	// backup apis
	batch = &rpcpb.BatchReq{}
	err = e.getValues(e.apisDir, limit, func() store.PB { return &metapb.API{} }, func(value interface{}) error {
		batch.PutAPIs = append(batch.PutAPIs, &rpcpb.PutAPIReq{
			API: *value.(*metapb.API),
		})

		if int64(len(batch.PutAPIs)) == limit {
			_, err := targetC.Batch(batch)
			if err != nil {
				return err
			}

			batch = &rpcpb.BatchReq{}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if int64(len(batch.PutAPIs)) > 0 {
		_, err := targetC.Batch(batch)
		if err != nil {
			return err
		}
	}

	// backup routings
	batch = &rpcpb.BatchReq{}
	err = e.getValues(e.routingsDir, limit, func() store.PB { return &metapb.Routing{} }, func(value interface{}) error {
		batch.PutRoutings = append(batch.PutRoutings, &rpcpb.PutRoutingReq{
			Routing: *value.(*metapb.Routing),
		})

		if int64(len(batch.PutRoutings)) == limit {
			_, err := targetC.Batch(batch)
			if err != nil {
				return err
			}

			batch = &rpcpb.BatchReq{}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if int64(len(batch.PutRoutings)) > 0 {
		_, err := targetC.Batch(batch)
		if err != nil {
			return err
		}
	}

	// backup plugin
	batch = &rpcpb.BatchReq{}
	err = e.getValues(e.pluginsDir, limit, func() store.PB { return &metapb.Plugin{} }, func(value interface{}) error {
		batch.PutPlugins = append(batch.PutPlugins, &rpcpb.PutPluginReq{
			Plugin: *value.(*metapb.Plugin),
		})

		if int64(len(batch.PutPlugins)) == limit {
			_, err := targetC.Batch(batch)
			if err != nil {
				return err
			}

			batch = &rpcpb.BatchReq{}
		}

		return nil
	})
	if err != nil {
		return err
	}

	applied, err := e.doGetAppliedPlugins()
	if err != nil {
		return err
	}
	if applied != nil {
		batch.ApplyPlugins = &rpcpb.ApplyPluginsReq{
			Applied: *applied,
		}
	}

	if int64(len(batch.PutPlugins)) > 0 {
		_, err := targetC.Batch(batch)
		if err != nil {
			return err
		}
	}

	// backup id
	currID, err := e.getID()
	if err != nil {
		return err
	}

	return targetC.SetID(currID)
}

// System returns system info
func (e *Store) System() (*metapb.System, error) {
	e.RLock()
	defer e.RUnlock()

	value := &metapb.System{}
	rsp, err := e.get(e.apisDir, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return nil, err
	}
	value.Count.API = rsp.Count

	rsp, err = e.get(e.clustersDir, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return nil, err
	}
	value.Count.Cluster = rsp.Count

	rsp, err = e.get(e.serversDir, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return nil, err
	}
	value.Count.Server = rsp.Count

	rsp, err = e.get(e.routingsDir, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return nil, err
	}
	value.Count.Routing = rsp.Count

	rsp, err = e.get(e.pluginsDir, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return nil, err
	}
	value.Count.Plugin = rsp.Count

	applied, err := e.doGetAppliedPlugins()
	if err != nil {
		return nil, err
	}
	value.Count.AppliedPlugin = int64(len(applied.AppliedIDs))

	return value, nil
}

func (e *Store) put(key, value string, opts ...clientv3.OpOption) error {
	_, err := e.txn().Then(clientv3.OpPut(key, value, opts...)).Commit()
	return err
}

func (e *Store) op(key, value string, opts ...clientv3.OpOption) clientv3.Op {
	return clientv3.OpPut(key, value, opts...)
}

func (e *Store) putBatch(ops ...clientv3.Op) error {
	if len(ops) == 0 {
		return nil
	}

	_, err := e.txn().Then(ops...).Commit()
	return err
}

func (e *Store) putTTL(key, value string, ttl int64) error {
	lessor := clientv3.NewLease(e.rawClient)
	defer lessor.Close()

	ctx, cancel := context.WithTimeout(e.rawClient.Ctx(), store.DefaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, ttl)
	cancel()

	if err != nil {
		return err
	}

	_, err = e.txn().Then(clientv3.OpPut(key, value, clientv3.WithLease(leaseResp.ID))).Commit()
	return err
}

func (e *Store) delete(key string, opts ...clientv3.OpOption) error {
	_, err := e.txn().Then(clientv3.OpDelete(key, opts...)).Commit()
	return err
}

func (e *Store) putPB(prefix string, value store.PB, do func(uint64)) (uint64, error) {
	if value.GetID() == 0 {
		id, err := e.allocID()
		if err != nil {
			return 0, err
		}

		do(id)
	}

	data, err := value.Marshal()
	if err != nil {
		return 0, err
	}

	return value.GetID(), e.put(getKey(prefix, value.GetID()), string(data))
}

func (e *Store) putPBWithOp(prefix string, value store.PB, do func(uint64)) (clientv3.Op, error) {
	if value.GetID() == 0 {
		id, err := e.allocID()
		if err != nil {
			return clientv3.Op{}, err
		}

		do(id)
	}

	return e.putPBKeyWithOp(getKey(prefix, value.GetID()), value)
}

func (e *Store) putPBKeyWithOp(key string, value store.PB) (clientv3.Op, error) {
	data, err := value.Marshal()
	if err != nil {
		return clientv3.Op{}, err
	}

	return e.op(key, string(data)), nil
}

func (e *Store) getValues(prefix string, limit int64, factory func() store.PB, fn func(interface{}) error) error {
	start := uint64(0)
	end := getKey(prefix, endID)
	withRange := clientv3.WithRange(end)
	withLimit := clientv3.WithLimit(limit)

	for {
		resp, err := e.get(getKey(prefix, start), withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			value := factory()
			err := value.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			fn(value)

			start = value.GetID() + 1
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (e *Store) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(e.rawClient.Ctx(), store.DefaultRequestTimeout)
	defer cancel()

	return clientv3.NewKV(e.rawClient).Get(ctx, key, opts...)
}

func (e *Store) getPB(prefix string, id uint64, value store.PB) error {
	return e.getPBWithKey(getKey(prefix, id), value, false)
}

func (e *Store) getPBWithKey(key string, value store.PB, allowNotFound bool) error {
	data, err := e.getValue(key)
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

func (e *Store) getValue(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(e.rawClient.Ctx(), store.DefaultRequestTimeout)
	defer cancel()

	resp, err := clientv3.NewKV(e.rawClient).Get(ctx, key)
	if nil != err {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	return resp.Kvs[0].Value, nil
}

func (e *Store) allocID() (uint64, error) {
	e.idLock.Lock()
	defer e.idLock.Unlock()

	if e.base == e.end {
		end, err := e.generate()
		if err != nil {
			return 0, err
		}

		e.end = end
		e.base = e.end - batch
	}

	e.base++
	return e.base, nil
}

func (e *Store) generate() (uint64, error) {
	for {
		value, err := e.getID()
		if err != nil {
			return 0, err
		}

		max := value + batch

		// create id
		if value == 0 {
			max := value + batch
			err := e.createID(max)
			if err == store.ErrStaleOP {
				continue
			}
			if err != nil {
				return 0, err
			}

			return max, nil
		}

		err = e.updateID(value, max)
		if err == store.ErrStaleOP {
			continue
		}
		if err != nil {
			return 0, err
		}

		return max, nil
	}
}

func (e *Store) createID(value uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(e.idPath), "=", 0)
	op := clientv3.OpPut(e.idPath, string(format.Uint64ToBytes(value)))
	rsp, err := e.txn().If(cmp).Then(op).Commit()
	if err != nil {
		return err
	}

	if !rsp.Succeeded {
		return store.ErrStaleOP
	}

	return nil
}

func (e *Store) getID() (uint64, error) {
	value, err := e.getValue(e.idPath)
	if err != nil {
		return 0, err
	}

	if len(value) == 0 {
		return 0, nil
	}

	return format.BytesToUint64(value)
}

func (e *Store) updateID(old, value uint64) error {
	cmp := clientv3.Compare(clientv3.Value(e.idPath), "=", string(format.Uint64ToBytes(old)))
	op := clientv3.OpPut(e.idPath, string(format.Uint64ToBytes(value)))
	rsp, err := e.txn().If(cmp).Then(op).Commit()
	if err != nil {
		return err
	}

	if !rsp.Succeeded {
		return store.ErrStaleOP
	}

	return nil
}

func (e *Store) getClusterBindPrefix(id uint64) string {
	return getKey(e.bindsDir, id)
}

func (e *Store) getBindKey(bind *metapb.Bind) string {
	return getKey(e.getClusterBindPrefix(bind.ClusterID), bind.ServerID)
}

func getKey(prefix string, id uint64) string {
	return fmt.Sprintf("%s/%020d", prefix, id)
}

func getAddrKey(prefix string, addr string) string {
	return fmt.Sprintf("%s/%s", prefix, util.GetAddrFormat(addr))
}
