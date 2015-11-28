package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	kvstore  map[string]string
	visitlog map[string]int64
	view     viewservice.View
}

// only service if server is primary
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.view.Primary {
		reply.Err = Err("not primary, now is " + pb.view.Primary + ", I'm " + pb.me)
		return fmt.Errorf("%s", "not primary, now is "+pb.view.Primary+", I'm "+pb.me)
	}
	reply.Value = pb.kvstore[args.Key]
	return nil
}

// only service if service is primary
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.me != pb.view.Primary {
		reply.Err = Err("not primary, now is " + pb.view.Primary + ", I'm " + pb.me)
		return fmt.Errorf("%s", "not primary, now is "+pb.view.Primary+", I'm "+pb.me)
	}
	if args.Op == OpPut {
		syncArgs := &PutAppendSyncArgs{args.Key, args.Value, args.Me, args.Seq}
		if err := pb.sync(syncArgs); err != nil {
			reply.Err = Err(err.Error())
			return err
		}
		pb.kvstore[args.Key] = args.Value
	} else if args.Op == OpAppend {
		// append itself do not fit at-most-once semantic, should handle it in server side
		var syncArgs PutAppendSyncArgs
		if pb.visitlog["update."+args.Me] == args.Seq {
			syncArgs = PutAppendSyncArgs{args.Key, pb.kvstore[args.Key], args.Me, args.Seq}
		} else {
			syncArgs = PutAppendSyncArgs{args.Key, pb.kvstore[args.Key] + args.Value, args.Me, args.Seq}
		}
		if err := pb.sync(&syncArgs); err != nil {
			reply.Err = Err(err.Error())
			return err
		}
		pb.kvstore[args.Key] = syncArgs.Value
		pb.visitlog["update."+args.Me] = args.Seq
	} else {
		fmt.Println("un-match/error op:", args.Op)
	}
	return nil
}

// put/append request will forward to here
func (pb *PBServer) SyncFromPrimary(args *PutAppendSyncArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Backup != pb.me {
		reply.Err = "not backup"
		return fmt.Errorf("not backup")
	}
	pb.kvstore[args.Key] = args.Value
	pb.visitlog["update."+args.Me] = args.Seq
	return nil
}

func (pb *PBServer) FlushFromPrimary(args *FlushArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Backup != pb.me {
		reply.Err = "not backup"
		return fmt.Errorf("not backup")
	}
	pb.kvstore = args.KV
	pb.visitlog = args.Visited
	return nil
}

func (pb *PBServer) sync(args *PutAppendSyncArgs) error {
	if pb.view.Backup == "" {
		return nil
	}
	var reply PutAppendReply
	ok := call(pb.view.Backup, "PBServer.SyncFromPrimary", args, &reply)
	if ok && reply.Err == "" {
		return nil
	}
	return fmt.Errorf("sync err: %s", reply.Err)
}

// TODO: flush could also fail? shouldn't handle it?
func (pb *PBServer) flush2(backup string) error {
	var reply PutAppendReply
	args := FlushArgs{pb.kvstore, pb.visitlog}
	ok := call(backup, "PBServer.FlushFromPrimary", args, &reply)
	if ok && reply.Err == "" {
		return nil
	}
	return fmt.Errorf("flush error: %s", reply.Err)
}

func (pb *PBServer) flush(view viewservice.View) error {
	var reply PutAppendReply
	args := FlushArgs{pb.kvstore, pb.visitlog}
	cnt := 0
	for {
		ok := call(view.Backup, "PBServer.FlushFromPrimary", args, &reply)
		if ok && reply.Err == "" {
			return nil
		}
		newview, _ := pb.vs.Get()
		if newview.Viewnum != view.Viewnum {
			// if view changed, tick() will know
			break
		} else {
			cnt++
			fmt.Println("will flush again", cnt)
		}
	}
	return fmt.Errorf("flush error: %s", reply.Err)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
// sync backup kv store.
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		// pb is now the primary && backup have been changed
		if view.Backup != "" && pb.me == pb.view.Primary && view.Viewnum != pb.view.Viewnum {
			pb.flush(view)
		}
	} else {
		// fmt.Println("view err!!!", err, pb.me)
	}
	// update even if view error. put myself into restarting status
	pb.view = view
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.kvstore = make(map[string]string)
	pb.visitlog = make(map[string]int64)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
