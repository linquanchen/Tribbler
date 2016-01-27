package libstore

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type SortNodeByNodeID []storagerpc.Node

func (s SortNodeByNodeID) Len() int {
	return len(s)
}

func (s SortNodeByNodeID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortNodeByNodeID) Less(i, j int) bool {
	return s[i].NodeID < s[j].NodeID
}

type ValueCacheElement struct {
	lease_end int64
	content   string
}

type ListCacheElement struct {
	lease_end int64
	content   []string
}

type libstore struct {
	servers             []storagerpc.Node
	rpc_connection      []*rpc.Client
	host_port           string
	lease_mode          LeaseMode
	query_record        map[string]*list.List
	query_record_locker *sync.Mutex
	value_cache         map[string]*ValueCacheElement
	value_cache_locker  *sync.Mutex
	list_cache          map[string]*ListCacheElement
	list_cache_locker   *sync.Mutex
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	master_server, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, errors.New("Cannot connect to the master server")
	}

	// Call GetServers to get storage servers' information
	var args storagerpc.GetServersArgs
	var reply storagerpc.GetServersReply
	master_server.Call("StorageServer.GetServers", args, &reply)
	if reply.Status == storagerpc.NotReady {
		for i := 0; i < 5; i++ {
			time.Sleep(1 * time.Second)
			master_server.Call("StorageServer.GetServers", args, &reply)
			if reply.Status == storagerpc.OK {
				break
			}
		}
	}
	master_server.Close()
	if reply.Status == storagerpc.NotReady {
		return nil, errors.New("Storage Server is not ready yet")
	}

	// Register RPC connection for each storage server
	ls := &libstore{}
	// Sort the servers by NodeID
	sort.Sort(SortNodeByNodeID(reply.Servers))
	ls.servers = reply.Servers
	ls.rpc_connection = make([]*rpc.Client, len(ls.servers))
	ls.host_port = myHostPort
	ls.lease_mode = mode
	ls.query_record = make(map[string]*list.List)
	ls.value_cache = make(map[string]*ValueCacheElement)
	ls.list_cache = make(map[string]*ListCacheElement)
	ls.query_record_locker = new(sync.Mutex)
	ls.value_cache_locker = new(sync.Mutex)
	ls.list_cache_locker = new(sync.Mutex)

	go ls.CacheCleaner()
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
	if err != nil {
		return nil, errors.New("Could not register Libstore")
	}
	for i, server := range ls.servers {
		ls.rpc_connection[i], _ = rpc.DialHTTP("tcp", server.HostPort)
	}
	return ls, nil
}

func (ls *libstore) CacheCleaner() {
	for {
		current_time := time.Now().Unix()
		ls.value_cache_locker.Lock()
		for k, v := range ls.value_cache {
			if v.lease_end < current_time {
				delete(ls.value_cache, k)
			}
		}
		ls.value_cache_locker.Unlock()
		ls.list_cache_locker.Lock()
		for k, v := range ls.list_cache {
			if v.lease_end < current_time {
				delete(ls.list_cache, k)
			}
		}
		ls.list_cache_locker.Unlock()
		time.Sleep(storagerpc.QueryCacheSeconds * time.Second)
	}
}

func (ls *libstore) Get(key string) (string, error) {
	var args storagerpc.GetArgs
	var reply storagerpc.GetReply
	current_time := time.Now().Unix()

	// Check whether the key is in the cache
	// If yes, return directly
	ls.value_cache_locker.Lock()
	if value_cache_elem, ok := ls.value_cache[key]; ok {
		if value_cache_elem.lease_end >= current_time {
			ls.value_cache_locker.Unlock()
			return value_cache_elem.content, nil
		} else {
			fmt.Println("In Delete()")
			delete(ls.value_cache, key)
		}
	}
	ls.value_cache_locker.Unlock()

	args.Key = key
	args.HostPort = ls.host_port

	if ls.lease_mode == Normal {
		ls.value_cache_locker.Lock()
		if query_list, ok := ls.query_record[key]; ok {
			query_list.PushBack(current_time)
		} else {
			new_list := list.New()
			new_list.PushBack(current_time)
			ls.query_record[key] = new_list
		}
		if query_list, ok := ls.query_record[key]; ok {
			for elem := query_list.Front(); elem != nil; elem = elem.Next() {
				// fmt.Printf("timestamp: %d now: %d\n", elem.Value.(int64), current_time)
				if elem.Value.(int64)+storagerpc.QueryCacheSeconds < current_time {
					query_list.Remove(elem)
					fmt.Println("delete ", elem.Value.(int64))
				} else {
					break
				}
			}
			if query_list.Len() >= storagerpc.QueryCacheThresh {
				args.WantLease = true
			} else {
				args.WantLease = false
			}
		}
		ls.value_cache_locker.Unlock()
	} else if ls.lease_mode == Always {
		args.WantLease = true
	}

	target_server := ls.GetServerIndex(key)
	ls.rpc_connection[target_server].Call("StorageServer.Get", args, &reply)
	if reply.Status != storagerpc.OK {
		return "", errors.New("Error in get operation")
	}
	if reply.Lease.Granted {
		ls.value_cache_locker.Lock()
		defer ls.value_cache_locker.Unlock()
		ls.value_cache[key] = &ValueCacheElement{lease_end: time.Now().Unix() + int64(reply.Lease.ValidSeconds), content: reply.Value}
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	var args storagerpc.PutArgs
	var reply storagerpc.PutReply

	args.Key = key
	args.Value = value
	target_server := ls.GetServerIndex(key)
	ls.rpc_connection[target_server].Call("StorageServer.Put", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error in put operation")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	var args storagerpc.DeleteArgs
	var reply storagerpc.DeleteReply
	args.Key = key
	target_server := ls.GetServerIndex(key)
	ls.rpc_connection[target_server].Call("StorageServer.Delete", args, &reply)
	if reply.Status != storagerpc.OK {
		fmt.Println("####")
		return errors.New("Error in delete operation")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	var args storagerpc.GetArgs
	var reply storagerpc.GetListReply

	current_time := time.Now().Unix()

	// Check whether the key is in the cache
	// If yes, return directly
	ls.list_cache_locker.Lock()
	if list_cache_elem, ok := ls.list_cache[key]; ok {
		if list_cache_elem.lease_end >= current_time {
			ls.list_cache_locker.Unlock()
			return list_cache_elem.content, nil
		} else {
			delete(ls.list_cache, key)
		}
	}
	ls.list_cache_locker.Unlock()

	args.Key = key
	args.HostPort = ls.host_port

	if ls.lease_mode == Normal {
		ls.list_cache_locker.Lock()
		if query_list, ok := ls.query_record[key]; ok {
			query_list.PushBack(current_time)
		} else {
			new_list := list.New()
			new_list.PushBack(current_time)
			ls.query_record[key] = new_list
		}
		if query_list, ok := ls.query_record[key]; ok {
			for elem := query_list.Front(); elem != nil; elem = elem.Next() {
				if elem.Value.(int64)+storagerpc.QueryCacheSeconds < current_time {
					query_list.Remove(elem)
				} else {
					break
				}
			}
			if query_list.Len() >= storagerpc.QueryCacheThresh {
				args.WantLease = true
			} else {
				args.WantLease = false
			}
		}
		ls.list_cache_locker.Unlock()
	} else if ls.lease_mode == Always {
		args.WantLease = true
	}

	target_server := ls.GetServerIndex(key)
	ls.rpc_connection[target_server].Call("StorageServer.GetList", args, &reply)
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error in Getlist")
	}
	if reply.Lease.Granted {
		ls.list_cache_locker.Lock()
		defer ls.list_cache_locker.Unlock()
		ls.list_cache[key] = &ListCacheElement{lease_end: time.Now().Unix() + int64(reply.Lease.ValidSeconds), content: reply.Value}
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	var args storagerpc.PutArgs
	var reply storagerpc.PutReply

	args.Key = key
	args.Value = removeItem
	target_server := ls.GetServerIndex(key)
	ls.rpc_connection[target_server].Call("StorageServer.RemoveFromList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error in RemoveFromList")
	} else {
		return nil
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	var args storagerpc.PutArgs
	var reply storagerpc.PutReply

	args.Key = key
	args.Value = newItem
	target_server := ls.GetServerIndex(key)
	ls.rpc_connection[target_server].Call("StorageServer.AppendToList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error in AppendToList")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.value_cache_locker.Lock()
	if _, ok := ls.value_cache[args.Key]; ok {
		delete(ls.value_cache, args.Key)
		reply.Status = storagerpc.OK
		ls.value_cache_locker.Unlock()
		return nil
	}
	ls.value_cache_locker.Unlock()
	ls.list_cache_locker.Lock()
	if _, ok := ls.list_cache[args.Key]; ok {
		delete(ls.list_cache, args.Key)
		reply.Status = storagerpc.OK
		ls.list_cache_locker.Unlock()
		return nil
	}
	ls.list_cache_locker.Unlock()
	reply.Status = storagerpc.KeyNotFound
	return nil
}

func (ls *libstore) GetServerIndex(key string) int {
	hash_value := StoreHash(key)

	for index, node := range ls.servers {
		if node.NodeID >= hash_value {
			return index
		}
	}
	return 0
}
