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
	curview View
	primary_missed uint
	backup_missed uint
	primary_acked bool
	after_internal_change bool
	backup_initialized bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	if args.Me == vs.curview.Primary {
		if args.Viewnum == 0 { // The primary becomes alive...
			if vs.curview.Backup != "" && vs.backup_initialized {
				vs.curview.Primary, vs.curview.Backup = vs.curview.Backup, vs.curview.Primary
				vs.curview.Viewnum ++
				vs.primary_acked = false
				vs.backup_missed = 0
			}
		} else {
			if args.Viewnum == vs.curview.Viewnum {
				vs.primary_acked = true
				vs.after_internal_change = false
			}
		}
		vs.primary_missed ++
	} else if args.Me == vs.curview.Backup {
		vs.backup_missed = 0
		if args.Viewnum != 0 {
			vs.backup_initialized = true
		} else {
			vs.backup_missed ++
		}
	} else {
		if vs.curview.Primary == "" {
			vs.curview.Primary = args.Me
			vs.primary_acked = true
			vs.curview.Viewnum ++
			//vs.primary_missed = 0
		} else if vs.curview.Backup == "" {
			fmt.Println("Register new backup")
			vs.curview.Backup = args.Me
			if !vs.after_internal_change {
				vs.curview.Viewnum ++
				//vs.after_internal_change = false
			}
			vs.backup_initialized = false
			vs.primary_acked = false
			vs.backup_missed = 0
		}
	}
	reply.View = vs.curview
	if !vs.primary_acked { reply.View.Viewnum -- }
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	reply.View = vs.curview
	//if !vs.primary_acked { reply.View.Viewnum -- }
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	modified := false
	vs.mu.Lock()
	if vs.curview.Primary != "" {
		vs.primary_missed ++
		if vs.primary_missed >= DeadPings && vs.primary_acked {
			vs.primary_missed = 0
			vs.curview.Primary = ""
			modified = true
		}
	}
	if vs.curview.Backup != "" {
		vs.backup_missed ++
		if vs.backup_missed == DeadPings {
			vs.backup_missed = 0
			vs.curview.Backup = ""
			modified = true
		}
	}
	// Promote backup if necessary
	if vs.curview.Primary == "" && vs.curview.Backup != ""  && vs.backup_initialized{
		vs.curview.Primary = vs.curview.Backup
		vs.curview.Backup = ""
		vs.primary_missed = vs.backup_missed
		vs.backup_missed = 0
		vs.backup_initialized = false
		vs.primary_acked = false
		modified = true
		fmt.Println("Promote!")
		vs.after_internal_change = true
	}

	if modified {
		vs.curview.Viewnum ++
	}
	vs.mu.Unlock()
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
	vs.curview = View{0, "", ""}
	vs.primary_acked = false

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
