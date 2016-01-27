package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SortTribbleByTimestamp []string

func (s SortTribbleByTimestamp) Len() int {
	return len(s)
}

func (s SortTribbleByTimestamp) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortTribbleByTimestamp) Less(i, j int) bool {
	time1 := strings.SplitN(s[i], "_", 3)[1]
	time2 := strings.SplitN(s[j], "_", 3)[1]
	int_time1, _ := strconv.ParseInt(time1, 16, 64)
	int_time2, _ := strconv.ParseInt(time2, 16, 64)
	return int_time1 > int_time2
}

type tribServer struct {
	lib_store libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	tribServer := new(tribServer)
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, errors.New("Libstore failed.")
	}

	tribServer.lib_store = ls

	// Create the server socket that will listen for incoming RPCs.
	_, port, _ := net.SplitHostPort(myHostPort)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
	// return nil, errors.New("not implemented")
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	user_id := util.FormatUserKey(args.UserID)

	_, err := ts.lib_store.Get(user_id)
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	err = ts.lib_store.Put(user_id, args.UserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	user_id := util.FormatUserKey(args.UserID)
	target_user_id := util.FormatUserKey(args.TargetUserID)

	// If the user_id doesn't exist
	// Return error
	_, err := ts.lib_store.Get(user_id)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// If the target_user_id doesn't exist
	// Return error
	_, err = ts.lib_store.Get(target_user_id)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	user_sub := util.FormatSubListKey(args.UserID)
	err = ts.lib_store.AppendToList(user_sub, args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	user_id := util.FormatUserKey(args.UserID)
	target_user_id := util.FormatUserKey(args.TargetUserID)

	// If the user_id doesn't exist
	// Return error
	_, err := ts.lib_store.Get(user_id)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// If the target_user_id doesn't exist
	// Return error
	_, err = ts.lib_store.Get(target_user_id)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	user_sub := util.FormatSubListKey(args.UserID)
	err = ts.lib_store.RemoveFromList(user_sub, args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	user_id := util.FormatUserKey(args.UserID)

	// If the user_id doesn't exist
	// Reture NoSuchUser
	_, err := ts.lib_store.Get(user_id)
	if err != nil {
		fmt.Println("===========", err, "============")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	user_id_list := util.FormatSubListKey(args.UserID)
	user_list, err := ts.lib_store.GetList(user_id_list)
	//if err != nil {
	//		reply.Status = tribrpc.NoSuchUser
	//		return nil
	//	}

	reply.UserIDs = user_list
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	user_id := util.FormatUserKey(args.UserID)

	// If the user_id doesn't exist
	// Reture NoSuchUser
	_, err := ts.lib_store.Get(user_id)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	now := time.Now()
	tribble := &tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   now,
		Contents: args.Contents,
	}

	tribble_str, _ := json.Marshal(tribble)

	post_key := util.FormatPostKey(args.UserID, time.Now().UnixNano())
	err = ts.lib_store.Put(post_key, string(tribble_str))
	//if err != nil {
	//	reply.Status = tribrpc.NoSuchUser
	//	fmt.Println(1)
	//	return nil
	//}

	user_trib_list_key := util.FormatTribListKey(args.UserID)
	err = ts.lib_store.AppendToList(user_trib_list_key, post_key)
	//if err != nil {
	//	reply.Status = tribrpc.NoSuchUser
	//	fmt.Println(2)
	//	return nil
	//}

	reply.Status = tribrpc.OK
	reply.PostKey = post_key
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	user_id := util.FormatUserKey(args.UserID)

	// If the user_id doesn't exist
	// Reture NoSuchUser
	_, err := ts.lib_store.Get(user_id)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	user_trib_list_key := util.FormatTribListKey(args.UserID)
	err = ts.lib_store.RemoveFromList(user_trib_list_key, args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		fmt.Println(2)
		return nil
	}

	err = ts.lib_store.Delete(args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	user_id := util.FormatUserKey(args.UserID)

	// If the user_id doesn't exist
	// Reture NoSuchUser
	_, err := ts.lib_store.Get(user_id)
	if err != nil {
		fmt.Println(1)
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Return a list of tribble_ids
	raw_tribble_list, _ := ts.lib_store.GetList(util.FormatTribListKey(args.UserID))

	if raw_tribble_list == nil {
		reply.Status = tribrpc.OK
		return nil
	}

	// Sort the tribble_ids by timestamp reversely
	sort.Sort(SortTribbleByTimestamp(raw_tribble_list))

	counter := 0
	tribble_list := make([]tribrpc.Tribble, 100)
	for _, tid := range raw_tribble_list {
		tribble, err := ts.lib_store.Get(tid)
		if err == nil {
			// var t tribrpc.Tribble
			json.Unmarshal([]byte(tribble), &tribble_list[counter])
			counter += 1
		}
		if counter >= 100 {
			break
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribble_list[:counter]
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// Getsublist by key
	// Get tribble list by user_ids in sublist
	// sort the tribble list by the timestamp
	// retrive the first 100 tribble (Be careful about the delete operation)
	user_id := util.FormatUserKey(args.UserID)

	// If the user_id doesn't exist
	// Reture NoSuchUser
	_, err := ts.lib_store.Get(user_id)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Get user's subscription_list
	user_id_list := util.FormatSubListKey(args.UserID)
	// list of IDs
	user_list, _ := ts.lib_store.GetList(user_id_list)

	if user_list == nil {
		reply.Status = tribrpc.OK
		return nil
	}

	// Get tribble_ids from the user_ids in the subscription list
	tribble_id_list := make([]string, 0)
	for _, usr_id := range user_list {
		t_id_list, err := ts.lib_store.GetList(util.FormatTribListKey(usr_id))
		if err == nil {
			tribble_id_list = append(tribble_id_list, t_id_list...)
		}
	}

	sort.Sort(SortTribbleByTimestamp(tribble_id_list))

	counter := 0
	tribble_list := make([]tribrpc.Tribble, 100)
	for _, tid := range tribble_id_list {
		tribble, err := ts.lib_store.Get(tid)
		if err == nil {
			json.Unmarshal([]byte(tribble), &tribble_list[counter])
			counter += 1
		}
		if counter >= 100 {
			break
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribble_list[:counter]
	return nil
}
