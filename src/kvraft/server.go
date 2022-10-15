package kvraft

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
}

type Msg struct {
	ok    bool
	value string
	index int
}

func (kv *KVServer) dprintf(logger *log.Logger, fmts string, args ...any) {
	if DEBUG {
		pc, _, _, ok := runtime.Caller(2)
		details := runtime.FuncForPC(pc)
		funcName := ""
		if ok && details != nil {
			tmp := strings.Split(details.Name(), ".")
			funcName = tmp[len(tmp)-1]
		}
		prefix := fmt.Sprintf("[%d]%s: ", kv.me, funcName)
		logger.Printf(prefix+fmts, args...)
	}
}

func (kv *KVServer) debug(fmts string, args ...any) {
	kv.dprintf(debugLogger, fmts, args...)
}
func (kv *KVServer) info(fmts string, args ...any) {
	kv.dprintf(infoLogger, fmts, args...)
}
func (kv *KVServer) warn(fmts string, args ...any) {
	kv.dprintf(warningLogger, fmts, args...)
}

type KVServer struct {
	// mu      sync.Mutex
	cond    sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	servers []*labrpc.ClientEnd
	// use as a one-capacity-queue, must hold cond
	applied *Msg
	db      map[string]string
}

func (kv *KVServer) getLeader() int {
	var leader int
	var state raft.State
	for {
		leader, state = kv.rf.State()
		if state != raft.CANDIDATE {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	return leader
}

func (kv *KVServer) Get(args *GetArgs, reply *RequestReply) {
	// Your code here.
	leader := kv.getLeader()
	kv.debug("current leader %d", leader)
	reply.CurLeader = leader
	if leader != kv.me {
		kv.servers[leader].Call("KVServer.Get", args, reply)
	} else {
		kv.debug("key: %q", args.Key)
		op := Op{
			Type: "Get",
			Key:  args.Key,
		}
		kv.debug("actuire cond")
		kv.cond.L.Lock()
		index, _, _ := kv.rf.Start(op)
		var m *Msg
		for {
			if kv.applied.index == index {
				m = kv.applied
				break
			}
			kv.debug("waiting for apply")
			kv.cond.Wait()
		}
		kv.cond.L.Unlock()
		if m.ok {
			reply.Value = m.value
			kv.debug("{%q:%q}", args.Key, m.value)
			reply.Err = OK
		} else {
			kv.warn("no such key: %q", args.Key)
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *RequestReply) {
	// Your code here.
	leader := kv.getLeader()
	kv.debug("current leader %d", leader)
	reply.CurLeader = leader
	if leader != kv.me {
		kv.servers[leader].Call("KVServer.PutAppend", args, reply)
	} else {
		kv.debug("{%q:%q}", args.Key, args.Value)
		op := Op{
			Type: args.Op,
			Key:  args.Key,
		}
		kv.debug("actuire cond")
		kv.cond.L.Lock()
		index, _, _ := kv.rf.Start(op)
		for {
			if kv.applied.index == index {
				break
			}
			kv.debug("waiting for apply")
			kv.cond.Wait()
		}
		kv.cond.L.Unlock()
	}
}

func (kv *KVServer) Applier() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}
		kv.debug("actuire cond")
		kv.cond.L.Lock()
		if m.CommandValid {
			var value string
			ok := true
			op := m.Command.(Op)
			kv.debug("apply command %s@%d", op, m.CommandIndex)
			switch op.Type {
			case "Get":
				value, ok = kv.db[op.Key]
			case "Append":
				value, ok := kv.db[op.Key]
				if ok {
					kv.db[op.Key] = value + op.Value
				} else {
					kv.db[op.Key] = op.Value
				}
			case "Put":
				kv.db[op.Key] = op.Value
			}
			kv.applied = &Msg{
				ok:    ok,
				value: value,
				index: m.CommandIndex,
			}
			kv.cond.Broadcast()
		}
		kv.cond.L.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	debug("StartKVServer: create kvserver on %d", me)
	labgob.Register(Op{})

	kv := new(KVServer)

	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)
	kv.applied = &Msg{}

	// You may need initialization code here.
	kv.servers = servers
	kv.cond = *sync.NewCond(&sync.Mutex{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.Applier()

	return kv
}
