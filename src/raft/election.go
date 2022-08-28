package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	LEADER State = State(iota)
	FOLLOWER
	CANDIDATE
	NIL_LEADER               = -1
	ELECTION_TIMEOUT_MINIMUM = 300 // ms
	ELECTION_TIMEOUT_SPAN    = 500 // ms
	HEARTBEAT_INTERVAL       = 150 //ms
)

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

type Vote struct {
	Vote   int
	Server int
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

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

// This is a RPC call
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug("new candidate %d at term %d, currently voted %d at term %d",
		args.CandidateId, args.Term, rf.votedFor, rf.currentTerm)

	// Set default reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//  1. Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		rf.info("reject: expect election term at least %d, got %d",
			rf.currentTerm, args.Term)
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != NIL_LEADER &&
		rf.votedFor != args.CandidateId {
		rf.info("reject: already voted candidate %d", rf.votedFor)
		return
	}

	rf.newCmd.L.Lock()
	defer rf.newCmd.L.Unlock()
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	rf.debug("LastLog: candidate: %d@%d, local: %d@%d",
		args.LastLogIndex, args.LastLogTerm, lastIndex, lastTerm)
	rf.debug("%s", logStr(rf.log, 0))

	// Election restriction:
	// Candidate's log is at least as up-to-date as receiver's
	// More up-to-date definitions:
	// 1. last log entry's term is equal or higher
	if lastTerm > args.LastLogTerm {
		rf.info("reject: expect lastLogTerm at least %d, got %d",
			lastTerm, args.LastLogTerm)
		// update term to avoid keeping conflict in following election
		// NOT SURE if it is necessary
		// rf.currentTerm = args.Term
		return
	}
	// 2. length of log entries is equal or longer when last term equal
	if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		rf.info("reject: expect lastLogIndex at least %d, got %d",
			lastIndex, args.LastLogIndex)
		// update term
		// NOT SURE if it is necessary
		// rf.currentTerm = args.Term
		return
	}

	//  2. If votedFor is null or candidateId, and candidate’s log is
	//     at least as up-to-date as receiver’s log, grant vote
	// Reset timer
	rf.timerFire.Store(false)
	rf.debug("%s@%d ==> %s@%d", rf.state, rf.currentTerm, FOLLOWER, args.Term)
	rf.follow(args.CandidateId, args.Term)
	rf.persist()
	rf.info("grant: vote candidate %d @%d", args.CandidateId, args.Term)
	reply.VoteGranted = true
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
	rf.isLeader.Store(false)

	// wake all agreementWith goroutine to exit
	rf.newCmd.Broadcast()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should be started and
		// to randomize sleeping time using time.Sleep().
		rand.Seed(makeSeed())
		dura := ELECTION_TIMEOUT_MINIMUM + rand.Int31n(ELECTION_TIMEOUT_SPAN)
		term, isleader := rf.GetState()

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
			switch rf.votedFor {
			case NIL_LEADER:
				rf.info("init election")
			case rf.me:
				rf.info("election of term %d timeout", term)
			default:
				rf.info("lost communication with leader %d at term %d",
					rf.votedFor, term)
			}
			rf.mu.Unlock()
			go rf.newElection()
		}
	}
}

func (rf *Raft) newElection() {
	// Init candidate state
	rf.mu.Lock()
	rf.newCmd.L.Lock()
	rf.follow(rf.me, rf.currentTerm+1)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.info("new election at term %d, lastLog: %d@%d",
		rf.currentTerm, args.LastLogIndex, args.LastLogTerm)
	rf.debug("%s", logStr(rf.log, 0))
	rf.persist()
	rf.newCmd.L.Unlock()
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
				} else if args.Term < reply.Term {
					// Stale, convert to follower
					rf.mu.Lock()
					rf.newCmd.L.Lock()
					rf.follow(NIL_LEADER, reply.Term)
					rf.persist()
					rf.newCmd.L.Unlock()
					rf.mu.Unlock()
					ch <- Vote{-1, index}
				} else {
					// Rejected
					ch <- Vote{0, index}
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
		curterm, isleader := rf.GetState()
		if curterm > term {
			// if already in next term
			if !isleader {
				rf.warn("stale election @%d, latest @%d, convert to follower",
					term, curterm)
			}
			return
		}
		if v.Vote >= 0 {
			resp++
			votes += v.Vote
			if v.Vote == 1 {
				rf.debug("grant from server %d, now %d/%d/%d votes at term %d",
					v.Server, votes, resp, all, term)
			} else {
				rf.debug("reject from server %d, now %d/%d/%d votes at term %d",
					v.Server, votes, resp, all, term)
			}
			if votes > half {
				// Win immidiately if the candidate got majority votes
				rf.timerFire.Store(false)
				break
			}
		} else {
			rf.warn("no reply from server %d at term %d", v.Server, term)
		}
	}

	// Count result
	rf.mu.Lock()
	if rf.currentTerm == term {
		// Still at the same term
		rf.debug("state after election @%d: %s", term, rf.state)
		if votes > half {
			// Win the election
			rf.state = LEADER
			rf.isLeader.Store(true)
			rf.info("won the election at term %d! got %d/%d/%d votes",
				term, votes, resp, all)
			rf.newCmd.L.Lock()
			rf.persist()
			rf.newCmd.L.Unlock()
			rf.mu.Unlock()
			go rf.sendHeartbeat(term)
			rf.agreement(term)
			return
		} else {
			// Lose the election or no winner, wait for winner's heartbeat
			rf.info("lost the election at term %d, got %d/%d/%d votes",
				term, votes, resp, all)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(term int) {
	for rf.isLeader.Load() && !rf.killed() {
		rf.newCmd.L.Lock()
		rf.applyCmd.L.Lock()
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.commitIndex,
			PrevLogTerm:  rf.log[rf.commitIndex].Term,
		}
		rf.applyCmd.L.Unlock()
		rf.newCmd.L.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(index int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, &args, &reply)
				if ok && reply.Term > args.Term {
					rf.mu.Lock()
					if rf.isLeader.Load() {
						rf.warn("stale term %d, latest %d, convert to follower",
							args.Term, reply.Term)
						rf.follow(NIL_LEADER, reply.Term)
						rf.newCmd.L.Lock()
						rf.persist()
						rf.newCmd.L.Unlock()
					}
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(time.Millisecond * time.Duration(HEARTBEAT_INTERVAL))
	}
}
