package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

// ViewServer
// a single point of failure.
//
type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	pingMap     map[string]int // store the ping Num for each server
	ackMap      map[string]uint
	view        View
	currentTick int
}

//
// get the primary
//
func (vs *ViewServer) getPrimary() string {
	return vs.view.Primary
}

//
// get the backup
//
func (vs *ViewServer) getBackup() string {
	return vs.view.Backup
}

//
// set the primary
//
func (vs *ViewServer) setPrimary(args PingArgs) {
	vs.view.Primary = args.Me
	vs.view.Viewnum = args.Viewnum + 1
	vs.pingMap[args.Me] = vs.currentTick
}

//
// set the backup
//
func (vs *ViewServer) setBackup(args PingArgs) {
    // log.Printf("[viewserver.setBackup]: set %s as backup", args.Me)
	vs.view.Backup = args.Me
	vs.view.Viewnum++
	vs.pingMap[args.Me] = vs.currentTick
}

//
// is the primary
//
func (vs *ViewServer) isPrimary(name string) bool {
	return name == vs.view.Primary
}

//
// is the backup
//
func (vs *ViewServer) isBackup(name string) bool {
	return name == vs.view.Backup
}

//
// has the primary
//
func (vs *ViewServer) hasPrimary() bool {
	return vs.view.Primary != ""
}

//
// has the backup
//
func (vs *ViewServer) hasBackup() bool {
	return vs.view.Backup != ""
}

//
// primary acked or not
//
func (vs *ViewServer) isPrimaryAck() bool {
	return vs.ackMap[vs.getPrimary()] == vs.view.Viewnum
}

//
// promote backup to primary
//
func (vs *ViewServer) promoteBackup() {
	// no backup, the system is down
	if vs.view.Backup == "" {
		log.Fatalf("No Backup when promote process")
		return
	}

	vs.pingMap[vs.getPrimary()] = vs.pingMap[vs.getBackup()]
	vs.ackMap[vs.getPrimary()] = vs.ackMap[vs.getBackup()]
	vs.view.Primary = vs.view.Backup
	vs.view.Backup = ""
	vs.view.Viewnum++
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	// log.Printf("[viewserver.Ping]: Ping %s with ack view %d", args.Me, args.Viewnum)
	// log.Printf("[viewserver.Ping]: currentTick %d", vs.currentTick)
	// log.Printf("[viewserver.Ping]: primary ACK %d, viewNum %d", vs.ackMap[vs.getPrimary()], vs.view.Viewnum)
	switch {
	// first server ping
	case !vs.hasPrimary() && vs.view.Viewnum == 0:
		vs.setPrimary(*args)

	// is the primary, whether has backup or not
	case vs.isPrimary(args.Me):
		// primary crash and reboot
		if args.Viewnum == 0 {
			vs.promoteBackup()
		} else {
			vs.ackMap[vs.getPrimary()] = args.Viewnum
			vs.pingMap[vs.getPrimary()] = vs.currentTick
		}

	// no backup, has a primary
	case vs.hasPrimary() && !vs.hasBackup() && vs.isPrimaryAck():
		vs.setBackup(*args)

	case vs.isBackup(args.Me):
		if vs.view.Viewnum == 0 && vs.isPrimaryAck() {
			vs.setBackup(*args)
		} else if vs.view.Viewnum != 0 {
			vs.pingMap[vs.getBackup()] = vs.currentTick
		}
	}
	reply.View = vs.view
	// log.Println("[viewserver.Ping]: reply view ", reply.View)
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.view
	// log.Printf("[viewserver.Get]: Get View %d, %s, %s", reply.View.Viewnum, reply.View.Primary, reply.View.Backup)
	vs.mu.Unlock()
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
	vs.currentTick++
	// must check the backup first, because the primary check will destroy the backup state
	if vs.hasBackup() && vs.currentTick-vs.pingMap[vs.getBackup()] >= DeadPings && vs.isPrimaryAck() {
		// log.Printf("[viewserver.tick]: backup outdate")
		vs.view.Backup = ""
		vs.view.Viewnum++
	}
	if vs.currentTick-vs.pingMap[vs.getPrimary()] >= DeadPings && vs.isPrimaryAck() {
		// log.Printf("[viewserver.tick]: primary outdate")
		vs.promoteBackup()
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
	vs.view = View{0, "", ""}
	vs.ackMap = make(map[string]uint)
	vs.pingMap = make(map[string]int)
	vs.currentTick = 0

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
