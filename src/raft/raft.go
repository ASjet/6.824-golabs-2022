package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      atomic.Bool         // set by Kill()
	apply     chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	timerFire atomic.Bool
	newCmd    *sync.Cond
	applyCmd  *sync.Cond

	/* +++++State+++++ */

	// Persistent state on all servers
	// Update to stable storage before responding to RPCs
	// hold mu
	isLeader    atomic.Bool
	state       State
	currentTerm int
	votedFor    int

	// hold newCmd lock
	log    []LogEntry
	offset int

	// Volatile state on all servers
	// hold applyCmd lock
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// Reinitialize after election
	// no lock
	nextIndex  []atomic.Int32
	matchIndex []int

	/* -----State----- */
	snapshotBuf []byte
	ss          *sync.Mutex
}

func (rf *Raft) dprintf(logger *log.Logger, fmts string, args ...any) {
	if DEBUG {
		pc, _, _, ok := runtime.Caller(2)
		details := runtime.FuncForPC(pc)
		funcName := ""
		if ok && details != nil {
			tmp := strings.Split(details.Name(), ".")
			funcName = tmp[len(tmp)-1]
		}
		prefix := fmt.Sprintf("[%d]%s: ", rf.me, funcName)
		logger.Printf(prefix+fmts, args...)
	}
}

func (rf *Raft) debug(fmts string, args ...any) {
	rf.dprintf(debugLogger, fmts, args...)
}
func (rf *Raft) info(fmts string, args ...any) {
	rf.dprintf(infoLogger, fmts, args...)
}
func (rf *Raft) warn(fmts string, args ...any) {
	rf.dprintf(warningLogger, fmts, args...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isLeader.Load()
	rf.mu.Unlock()
	return term, isLeader
}

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	// Your code here (2B).
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Lock()
		rf.newCmd.L.Lock()
		next := len(rf.log) + rf.offset
		rf.debug("new command %v at log entry %d@%d", command, next, term)
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		rf.persist()
		rf.debug("%s", logStr(rf.log, rf.offset))
		rf.newCmd.L.Unlock()
		rf.mu.Unlock()
		rf.newCmd.Broadcast()
		index = next
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.dead.Store(true)
	// Your code here, if desired.
	rf.applyCmd.Broadcast()
	rf.newCmd.Broadcast()
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	debug("Make: create raft server on %d", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.newCmd = sync.NewCond(&sync.Mutex{})
	rf.applyCmd = sync.NewCond(&sync.Mutex{})
	rf.apply = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.dead.Store(false)
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = NIL_LEADER
	rf.log = []LogEntry{{0, nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.offset = 0
	rf.snapshotBuf = []byte{}
	rf.ss = &sync.Mutex{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.offset
	rf.lastApplied = rf.offset

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()

	return rf
}
