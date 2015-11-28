package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// import "time"

import "crypto/rand"
import "math/big"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary string
	me      string
	seq     int64
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.me = fmt.Sprintf("%s%d", me, nrand())

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.

	if ck.primary == "" {
		if view, success := ck.vs.Get(); success && view.Primary != "" {
			ck.primary = view.Primary
		}
	}
	for {
		args := GetArgs{key}
		var reply GetReply
		if success := call(ck.primary, "PBServer.Get", args, &reply); success && reply.Err == "" {
			return reply.Value
		}
		// time.Sleep(viewservice.PingInterval)
		if view, success := ck.vs.Get(); success && view.Primary != "" {
			ck.primary = view.Primary
		}
	}
	return "???"
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	ck.seq++
	seq := ck.seq
	if ck.primary == "" {
		if view, success := ck.vs.Get(); success && view.Primary != "" {
			ck.primary = view.Primary
		}
	}
	for {
		args := PutAppendArgs{key, value, op, ck.me, seq}
		var reply PutAppendReply
		if success := call(ck.primary, "PBServer.PutAppend", args, &reply); success && reply.Err == "" {
			return
		}
		// time.Sleep(viewservice.PingInterval)
		if view, success := ck.vs.Get(); success && view.Primary != "" {
			ck.primary = view.Primary
		}
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
