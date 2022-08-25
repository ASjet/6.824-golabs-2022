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
	//	"bytes"

	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	LEADER State = State(iota)
	FOLLOWER
	CANDIDATE
	NIL_LEADER                = -1
	ELECTION_TIMEOUT_DURATION = 250 // ms
	HEARTBEAT_INTERVAL        = 150 // ms
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

type State int

func (s State) String() string {
	switch s {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	}
	return "SERVER"
}

type CommandType interface{}
type LogEntry struct {
	Term    int
	Command CommandType
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      atomic.Bool         // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* +++++State+++++ */

	// Persistent state on all servers
	// Update to stable storage before responding to RPCs
	state       State
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// Reinitialize after election
	nextIndex  []int
	matchIndex []int

	/* -----State----- */

	timerFire atomic.Bool
}

// call with holding lock
func (rf *Raft) follow(leader, term int) {
	rf.currentTerm = term
	rf.votedFor = leader
	if leader == rf.me {
		rf.state = CANDIDATE
	} else {
		rf.state = FOLLOWER
	}
}

func (rf *Raft) makeLog(logger *log.Logger, fmts string, args ...any) {
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
	rf.makeLog(debugLogger, fmts, args...)
}
func (rf *Raft) info(fmts string, args ...any) {
	rf.makeLog(infoLogger, fmts, args...)
}
func (rf *Raft) warn(fmts string, args ...any) {
	rf.makeLog(warningLogger, fmts, args...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int,
	snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs,
	reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
// This is a RPC call
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.debug("receive vote request at term %d from candidate %d",
		args.Term, args.CandidateId)
	defer rf.mu.Unlock()
	rf.mu.Lock()

	// Set default reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.debug("%s at term %d, voted %d",
		rf.state, rf.currentTerm, rf.votedFor)

	switch {
	case rf.currentTerm > args.Term:
		// Reject: stale term
		reply.VoteGranted = false
		rf.info("reject: stale term %d, current term %d",
			args.Term, rf.currentTerm)
		return
	case rf.currentTerm < args.Term:
		rf.info("%s at term %d ==> %s at term %d",
			rf.state, rf.currentTerm, FOLLOWER, args.Term)
		rf.follow(args.CandidateId, args.Term)
		fallthrough
	case rf.currentTerm == args.Term:
		if rf.votedFor == NIL_LEADER || rf.votedFor == args.CandidateId {
			logAhead := args.LastLogIndex - len(rf.log) + 1
			// Candidate's log is at least as up-to-date as receiver's
			if logAhead >= 0 {
				// Grant
				// Reset timer
				rf.timerFire.Store(false)
				rf.follow(args.CandidateId, args.Term)
				rf.persist()
				reply.VoteGranted = true
				rf.debug("candidate ahead %d log entries", logAhead)
				rf.debug("grant: vote candidate %d at term %d",
					args.CandidateId, args.Term)
				return
			} else {
				// Reject: stale log entries
				rf.debug("reject: candidate behind %d log entries", logAhead)
				return
			}
		}
	}

	// Reject
	rf.debug("reject: already voted candidate %d", rf.votedFor)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// This is a RPC call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.mu.Unlock()
	rf.mu.Lock()

	// Set default reply
	reply.Term = rf.currentTerm
	reply.Success = true

	if rf.currentTerm > args.Term {
		// Stale leader term
		rf.debug("stale leader %d at term %d, current leader %d at term %d",
			args.LeaderId, args.Term, rf.votedFor, rf.currentTerm)
		reply.Success = false
		return
	}

	// Reset timer
	rf.timerFire.Store(false)

	if rf.votedFor != args.LeaderId {
		// New leader
		rf.info("new leader %d at term %d", args.LeaderId, args.Term)
	}
	rf.follow(args.LeaderId, args.Term)
	rf.persist()

	if rf.currentTerm == args.PreLogTerm {
		if len(rf.log) >= args.PrevLogIndex {
			// Stale leader entries
			return
		}
		// TODO Update entries
	}

	// Heartbeat
	if len(args.Entries) == 0 {
		rf.debug("heartbeat message from leader %d at term %d",
			args.LeaderId, args.Term)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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

// Must call with holding lock
func (rf *Raft) claimLeadership(term int) {
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  []LogEntry{},
	}
	rf.debug("%s at term %d", rf.state, rf.currentTerm)
	rf.mu.Unlock()

	dura := time.Millisecond * time.Duration(HEARTBEAT_INTERVAL)
	stop := atomic.Bool{}
	for !(stop.Load() || rf.killed()) {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(index int) {
				rf.debug("send heartbeat to server %d", index)
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(index, &args, &reply) {
					if reply.Term > args.Term {
						rf.mu.Lock()
						rf.follow(NIL_LEADER, reply.Term)
						rf.persist()
						rf.mu.Unlock()
						stop.Store(true)
					}
				}
			}(i)
		}
		time.Sleep(dura)
	}
}

type Vote struct {
	Vote   int
	Server int
}

func (rf *Raft) newElection() {
	// Init candidate state
	rf.mu.Lock()
	rf.follow(rf.me, rf.currentTerm+1)
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.info("start new election on term %d", rf.currentTerm)
	rf.mu.Unlock()

	ch := make(chan Vote)
	go rf.rollVote(ch, args.Term)

	wg := sync.WaitGroup{}
	// Send vote requests
	for i := range rf.peers {
		// Skip self
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(index int) {
			rf.debug("request vote from server %d at term %d", index, args.Term)
			reply := RequestVoteReply{}
			if rf.sendRequestVote(index, &args, &reply) {
				if reply.VoteGranted {
					// Granted
					ch <- Vote{1, index}
				} else {
					if args.Term < reply.Term {
						// Stale, convert to follower
						rf.mu.Lock()
						rf.follow(NIL_LEADER, reply.Term)
						rf.persist()
						rf.mu.Unlock()
						ch <- Vote{-1, index}
					} else {
						// Rejected
						ch <- Vote{0, index}
					}
				}
			} else {
				// No reply
				ch <- Vote{-2, index}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	close(ch)
}

func (rf *Raft) rollVote(ch chan Vote, term int) {
	all := len(rf.peers)
	half, votes, resp := all/2, 1, 1
	// Receive vote response
	for v := range ch {
		if v.Vote >= 0 {
			resp++
			votes += v.Vote
			if v.Vote == 1 {
				rf.debug("got vote from server %d, now %d/%d/%d vote(s) at term %d",
					v.Server, votes, resp, all, term)
			} else {
				rf.debug("got reject from server %d, now %d/%d/%d vote(s) at term %d",
					v.Server, votes, resp, all, term)
			}
			if votes > half {
				// Win immidiately if the candidate got majority votes
				rf.timerFire.Store(false)
				break
			}
		} else {
			switch v.Vote {
			case -1:
				curterm, _ := rf.GetState()
				rf.info("stale term %d, latest %d, convert to follower",
					term, curterm)
				return
			case -2:
				rf.debug("no reply from server %d at term %d", v.Server, term)
			}
		}
	}

	// Count result
	rf.mu.Lock()
	rf.debug("state after election at term %d: %s", term, rf.state)
	if rf.currentTerm == term {
		// Still at the same term
		if votes > half {
			// Win the election
			rf.state = LEADER
			rf.persist()
			rf.info("won the election at term %d! got %d/%d/%d votes",
				term, votes, resp, all)
			rf.claimLeadership(term)
			return
		} else {
			// Lose the election or no winner
			rf.info("lost the election at term %d, got %d/%d/%d votes",
				term, votes, resp, all)
		}
	}
	rf.mu.Unlock()
}

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
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rand.Seed(makeSeed())
		dura := ELECTION_TIMEOUT_DURATION + rand.Int31n(ELECTION_TIMEOUT_DURATION)
		term, isleader := rf.GetState()
		rf.debug("new timer: %d ms on term %d", dura, term)

		// Set timer
		rf.timerFire.Store(true)
		time.Sleep(time.Millisecond * time.Duration(dura))
		if isleader || rf.killed() {
			// Cancel timer
			rf.timerFire.Store(false)
		}
		if rf.timerFire.Load() {
			// Timer fired
			rf.mu.Lock()
			if rf.currentTerm != term {
				// Stale term
				rf.mu.Unlock()
				continue
			}
			rf.debug("timer fired on term %d", term)
			switch rf.votedFor {
			case NIL_LEADER:
				rf.info("init election")
			case rf.me:
				rf.info("election of term %d timeout", term)
			default:
				rf.info("lost communication with leader %d", rf.votedFor)
			}
			rf.mu.Unlock()
			go rf.newElection()
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.dead.Store(false)
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = NIL_LEADER
	rf.log = []LogEntry{{0, nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
