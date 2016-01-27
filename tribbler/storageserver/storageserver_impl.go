package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	//"os"
	"sort"
	"strconv"
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

type storageServer struct {
	nodeID               uint32
	masterServerHostPort string
	numNodes             int
	nodes                []storagerpc.Node
	registerCnt          int
	port                 int
	isMaster             bool
	listener             net.Listener
	/* Two tables. Store string and list of string respectively */
	stringTable map[string]string
	listTable   map[string][]string
	/* Lease */
	leases         map[string]LeaseManager
	registerLocker *sync.Mutex
	/* Each key has a lock */
	tableLocks        map[string]*sync.Mutex
	leasesLock        *sync.Mutex
	lockForTableLocks *sync.Mutex
}

type StorageLease struct {
	hostPort   string
	expireTime int64
}

type LeaseManager struct {
	beingRevoked bool
	leases       []StorageLease
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := &storageServer{}
	ss.nodeID = nodeID
	ss.masterServerHostPort = masterServerHostPort
	ss.numNodes = numNodes
	ss.nodes = make([]storagerpc.Node, numNodes)
	ss.port = port
	ss.isMaster = (len(masterServerHostPort) == 0)
	ss.registerCnt = 0
	ss.stringTable = make(map[string]string)
	ss.listTable = make(map[string][]string)
	ss.leases = make(map[string]LeaseManager)
	ss.registerLocker = new(sync.Mutex)
	ss.tableLocks = make(map[string]*sync.Mutex)
	ss.leasesLock = new(sync.Mutex)
	ss.lockForTableLocks = new(sync.Mutex)
	if ss.isMaster {
		ss.masterServerHostPort = "localhost:" + strconv.Itoa(port)
	}

	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("Error on listen: ", err)
		return nil, errors.New("Error on listen: " + err.Error())
	}
	/* Begin listening for incoming connections*/
	rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	rpc.HandleHTTP()

	ss.listener = ln
	go http.Serve(ln, nil)
	/* Wait until all nodes have joined the consistent hashing ring */
	for {
		client, err := rpc.DialHTTP("tcp", ss.masterServerHostPort)
		if err != nil {
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}

		args := &storagerpc.RegisterArgs{storagerpc.Node{"localhost:" + strconv.Itoa(port), ss.nodeID}}
		var reply storagerpc.RegisterReply
		err = client.Call("StorageServer.RegisterServer", args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			ss.nodes = reply.Servers
			break
		}
		/* Sleep for one second before sending another RegisterServer request */
		time.Sleep(time.Duration(1) * time.Second)
	}

	fmt.Printf("StorageServer %d has start up. Listen on port: %d\n", ss.nodeID, port)
	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if !ss.isMaster {
		return errors.New("I am not a master!")
	}
	ss.registerLocker.Lock()
	defer ss.registerLocker.Unlock()
	/* Check if the node has already registered. */
	hasRegistered := false
	for _, node := range ss.nodes {
		if args.ServerInfo.NodeID == node.NodeID {
			hasRegistered = true
		}
	}
	/* Register new node */
	if !hasRegistered {
		fmt.Printf("Register node %d, NodeID is %d\n", ss.registerCnt, args.ServerInfo.NodeID)
		ss.nodes[ss.registerCnt] = args.ServerInfo
		ss.registerCnt += 1
	}
	/* If all nodes has registered, return OK */
	if ss.registerCnt == ss.numNodes {
		sort.Sort(SortNodeByNodeID(ss.nodes))
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodes
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if !ss.isMaster {
		return errors.New("I am not a master!")
	}
	ss.registerLocker.Lock()
	defer ss.registerLocker.Unlock()
	if ss.registerCnt == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodes
	} else {
		reply.Status = storagerpc.NotReady
		reply.Servers = ss.nodes
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	value, exist := ss.stringTable[args.Key]
	if !exist {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = value
		if args.WantLease {
			granted := ss.GrantLease(args.Key, args.HostPort)
			reply.Lease = storagerpc.Lease{granted, storagerpc.LeaseSeconds}
		}
	}
	//fmt.Println("Get Value ", value, " for key ", args.Key)
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.lockForTableLocks.Lock()
	/* Lock table for writing */
	lock, exist := ss.tableLocks[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.tableLocks[args.Key] = lock
	}
	ss.lockForTableLocks.Unlock()
	lock.Lock()
	defer lock.Unlock()

	ss.RevokeLease(args.Key)

	_, exist = ss.stringTable[args.Key]
	if !exist {
		reply.Status = storagerpc.KeyNotFound
	} else {

		delete(ss.stringTable, args.Key)
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	list, exist := ss.listTable[args.Key]
	if !exist {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Value = list
		reply.Status = storagerpc.OK
		if args.WantLease {
			granted := ss.GrantLease(args.Key, args.HostPort)
			reply.Lease = storagerpc.Lease{granted, storagerpc.LeaseSeconds}
		}
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//fmt.Println("Put value: ", args.Value, " to key ", args.Key)
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.lockForTableLocks.Lock()
	/* Lock table for writing */
	lock, exist := ss.tableLocks[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.tableLocks[args.Key] = lock
	}
	ss.lockForTableLocks.Unlock()

	lock.Lock()
	defer lock.Unlock()

	ss.RevokeLease(args.Key)

	ss.stringTable[args.Key] = args.Value
	reply.Status = storagerpc.OK

	return nil
}

/* TODO: Use locks! */
func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	/* Check if key is in this server's range */
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	/* Lock table for writing */
	ss.lockForTableLocks.Lock()
	lock, exist := ss.tableLocks[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.tableLocks[args.Key] = lock
	}
	ss.lockForTableLocks.Unlock()
	lock.Lock()
	defer lock.Unlock()

	ss.RevokeLease(args.Key)

	list := ss.listTable[args.Key]
	exist = false
	for _, item := range list {
		if item == args.Value {
			exist = true
			break
		}
	}
	if exist {
		reply.Status = storagerpc.ItemExists
	} else {
		ss.listTable[args.Key] = append(list, args.Value)
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	/* Lock table for writing */
	ss.lockForTableLocks.Lock()
	lock, exist := ss.tableLocks[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.tableLocks[args.Key] = lock
	}
	ss.lockForTableLocks.Unlock()

	lock.Lock()
	defer lock.Unlock()

	ss.RevokeLease(args.Key)

	list := ss.listTable[args.Key]
	exist = false
	for i, item := range list {
		if item == args.Value {
			exist = true
			ss.listTable[args.Key] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if !exist {
		reply.Status = storagerpc.ItemNotFound
	} else {
		reply.Status = storagerpc.OK
	}
	return nil
}

/* Check if the key is in this server's range */
func (ss *storageServer) IsKeyInRange(key string) bool {
	hashValue := libstore.StoreHash(key)
	var correctNodeID uint32
	correctNodeID = ss.nodes[0].NodeID
	for _, node := range ss.nodes {
		if node.NodeID >= hashValue {
			correctNodeID = node.NodeID
			break
		}
	}
	return correctNodeID == ss.nodeID
}

func (ss *storageServer) GrantLease(key string, hostPort string) bool {
	leaseManager, exist := ss.leases[key]
	if !exist {
		leaseManager = LeaseManager{false, make([]StorageLease, 0)}
	}
	/* The lease on the key is currently being revoked. Reject this lease request */
	if leaseManager.beingRevoked {
		return false
	}
	/* Grant a new lease */
	expireTime := time.Now().Unix() + storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	newLease := StorageLease{hostPort, expireTime}
	leaseManager.leases = append(leaseManager.leases, newLease)
	ss.leasesLock.Lock()
	ss.leases[key] = leaseManager
	ss.leasesLock.Unlock()
	return true
}

func (ss *storageServer) RevokeLease(key string) {
	ss.leasesLock.Lock()
	/* Stop granting new leases for the key. */
	leaseManager, exist := ss.leases[key]
	if !exist {
		leaseManager = LeaseManager{true, make([]StorageLease, 0)}
	}
	ss.leases[key] = LeaseManager{true, make([]StorageLease, 0)}
	ss.leasesLock.Unlock()

	/* Revoke all leases on that key */
	for _, lease := range leaseManager.leases {
		/* After this many seconds, the lease expires */
		remainTime := lease.expireTime - time.Now().Unix()
		/* Already expired */
		if remainTime <= 0 {
			continue
		}
		replyCh := make(chan bool)
		/* send a RevokeLease RPC to all holders of valid leases for the key */
		go func() {
			client, err := rpc.DialHTTP("tcp", lease.hostPort)
			if err != nil {
				fmt.Println("DialHTTP error when dailing: ", lease.hostPort)
			}
			args := &storagerpc.RevokeLeaseArgs{key}
			var reply storagerpc.RevokeLeaseReply
			client.Call("LeaseCallbacks.RevokeLease", args, &reply)
			replyCh <- true
			client.Close()
		}()
		/* Either all lease holders have replied with a RevokeLeaseReply containing an OK status,
		or all of the leases have expired*/
		select {
		case <-replyCh:
			break
		case <-time.After(time.Duration(remainTime) * time.Second):
			break
		}
	}
	/* Reinitial LeaseManager for this key */
	ss.leasesLock.Lock()
	ss.leases[key] = LeaseManager{false, make([]StorageLease, 0)}
	ss.leasesLock.Unlock()
}
