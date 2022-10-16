package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

func (ck *Clerk) dprintf(logger *log.Logger, fmts string, args ...any) {
	if DEBUG {
		pc, _, _, ok := runtime.Caller(2)
		details := runtime.FuncForPC(pc)
		funcName := ""
		if ok && details != nil {
			tmp := strings.Split(details.Name(), ".")
			funcName = tmp[len(tmp)-1]
		}
		prefix := fmt.Sprintf("[Clerk]%s: ", funcName)
		logger.Printf(prefix+fmts, args...)
	}
}

func (ck *Clerk) debug(fmts string, args ...any) {
	ck.dprintf(debugLogger, fmts, args...)
}
func (ck *Clerk) info(fmts string, args ...any) {
	ck.dprintf(infoLogger, fmts, args...)
}
func (ck *Clerk) warn(fmts string, args ...any) {
	ck.dprintf(warningLogger, fmts, args...)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader atomic.Uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader.Store(0)
	// You'll have to add code here.
	return ck
}

// Clerks send Put(), Append(), and Get() RPCs to the kvserver
//   whose associated Raft is the leader.
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Call(RPCName string, args interface{}, reply *RequestReply) {
	for {
		ck.debug("call server %d", ck.leader.Load())
		ok := ck.servers[ck.leader.Load()].Call(RPCName, args, reply)
		if ok {
			if reply.Err != ErrWrongLeader {
				return
			}
			newLeader := (reply.CurLeader) % len(ck.servers)
			ck.warn("wrong leader %d, cur leader %d", ck.leader.Load(), newLeader)
			ck.leader.Store(uint32(newLeader))
		} else {
			oldLeader := ck.leader.Load()
			ck.leader.Store((ck.leader.Load() + 1) % uint32(len(ck.servers)))
			ck.warn("failed connect to server %d, retry %d", oldLeader, ck.leader.Load())
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	reply := RequestReply{}
	ck.debug("key: %q", key)
	// You will have to modify this function.
	ck.Call("KVServer.Get", &args, &reply)
	if reply.Err == OK {
		ck.debug("value: %q", reply.Value)
		return reply.Value
	}
	ck.warn("no such key: %q", key)
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := RequestReply{}
	ck.debug("%s{%q:%q}", op, key, value)
	ck.Call("KVServer.PutAppend", &args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
