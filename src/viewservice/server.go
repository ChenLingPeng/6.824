package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	view      View
	ack       bool // change to false when view change and to true if primary
	pingCount map[string]uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.pingCount[args.Me] = 0
	vs.rpccount++
	if vs.view.Viewnum == 0 {
		// when the viewservice first starts, it should accept any server at all as the first primary
		vs.view.Primary = args.Me
		vs.view.Viewnum = 1
		vs.ack = false
	} else {
		if vs.view.Primary == args.Me {
			// from primary
			if args.Viewnum == 0 {
				// primary crash & re-start
				if vs.view.Backup == "" {
					// no backup, continue as primary. lucky boy...
					vs.view.Viewnum++
					vs.ack = false
				} else {
					vs.view.Primary = vs.view.Backup
					vs.view.Backup = args.Me
					vs.view.Viewnum++
					vs.ack = false
				}
			} else if args.Viewnum == vs.view.Viewnum {
				// ack this view
				// if not equal, should re-ack for new view
				vs.ack = true
			}
		} else {
			if vs.ack == false {
				// change nothing
				reply.View = vs.view
				return nil
			}
			if vs.view.Backup == "" {
				vs.view.Backup = args.Me
				vs.view.Viewnum++
				vs.ack = false
			} else if vs.view.Primary == "" {
				// will this happen?
				fmt.Println("something happended: the primary is missing and backup is not empty")
				if args.Me == vs.view.Backup && args.Viewnum == vs.view.Viewnum {
					// the backup will be promoted as primary since it has the fresh view
					vs.view.Primary = vs.view.Backup
					vs.view.Backup = ""
					vs.ack = false
				}
			}
		}
	}
	reply.View = vs.view

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for key, _ := range vs.pingCount {
		if key != vs.view.Primary && key != vs.view.Backup {
			delete(vs.pingCount, key) // delete when iterate?
		}
	}
	viewChange := false
	if vs.view.Backup != "" {
		vs.pingCount[vs.view.Backup] = vs.pingCount[vs.view.Backup] + 1
		if vs.ack && vs.pingCount[vs.view.Backup] > DeadPings {
			// backup is dead now
			vs.view.Backup = ""
			viewChange = true // view changed
		}
	}
	if vs.view.Primary != "" {
		vs.pingCount[vs.view.Primary] = vs.pingCount[vs.view.Primary] + 1
		if vs.ack && vs.pingCount[vs.view.Primary] > DeadPings {
			// primary is dead now
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			viewChange = true // view changed
		}
	}
	if viewChange {
		vs.view.Viewnum++
		vs.ack = false
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.pingCount = make(map[string]uint)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
