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
	NIL_LEADER                = -1
	ELECTION_TIMEOUT_DURATION = 250 // ms
	HEARTBEAT_INTERVAL        = 150 //ms
)

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
	rf.debug("receive vote request at term %d from candidate %d",
		args.Term, args.CandidateId)
	defer rf.mu.Unlock()
	rf.mu.Lock()

	// Set default reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.debug("%s at term %d, voted %d",
		rf.state, rf.currentTerm, rf.votedFor)

	if rf.currentTerm > args.Term {
		// Reject: stale term
		reply.VoteGranted = false
		rf.debug("reject: stale term %d, current term %d",
			args.Term, rf.currentTerm)
		return
	}

	if rf.currentTerm < args.Term {
		// update current term and follow
		rf.info("%s at term %d ==> %s at term %d",
			rf.state, rf.currentTerm, FOLLOWER, args.Term)
		rf.follow(NIL_LEADER, args.Term)
	}

	if rf.votedFor == NIL_LEADER || rf.votedFor == args.CandidateId {
		lastIndex := len(rf.log) - 1
		lastTerm := rf.log[lastIndex].Term
		logAhead := args.LastLogIndex - lastIndex
		// Candidate's log is at least as up-to-date as receiver's
		// more up-to-date definitions:
		// 1. last log entry's term is equal or higher and
		// 2. length of log entries is equal or longer
		if lastTerm <= args.LastLogTerm && logAhead >= 0 {
			// Grant thus term match and index match
			// Reset timer
			rf.timerFire.Store(false)
			rf.follow(args.CandidateId, args.Term)
			rf.persist()
			reply.VoteGranted = true
			rf.debug("candidate ahead %d log entries", logAhead)
			rf.debug("grant: vote candidate %d at term %d",
				args.CandidateId, args.Term)
		} else {
			// Reject: stale log entries
			rf.debug("reject: expect last log term %d, got %d; %d entries behind",
				lastTerm, args.LastLogTerm, logAhead)
		}
	} else {
		// Reject
		rf.debug("reject: already voted candidate %d", rf.votedFor)
	}
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
			rf.isLeader.Store(true)
			rf.persist()
			rf.info("won the election at term %d! got %d/%d/%d votes",
				term, votes, resp, all)
			rf.mu.Unlock()
			go rf.sendHeartbeat(term)
			rf.agreement(term)
			return
		} else {
			// Lose the election or no winner
			rf.info("lost the election at term %d, got %d/%d/%d votes",
				term, votes, resp, all)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(term int) {
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
		Entries:  nil,
	}
	for rf.isLeader.Load() && !rf.killed() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(index int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, &args, &reply)
				if ok && reply.Term > args.Term {
					rf.debug("current term %d, lastest is %d, convert to follower",
						args.Term, reply.Term)
					rf.mu.Lock()
					rf.follow(NIL_LEADER, reply.Term)
					rf.persist()
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(time.Millisecond * time.Duration(HEARTBEAT_INTERVAL))
	}
}
