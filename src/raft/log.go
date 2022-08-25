package raft

import (
	"time"
)

const (
	RETRY_INTERVAL = time.Millisecond * 100
)

type ReplicaLog struct {
	Index  int
	Server int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
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
	reply.Success = false

	if rf.currentTerm > args.Term {
		// Stale leader term
		rf.debug("stale leader %d at term %d, current leader %d at term %d",
			args.LeaderId, args.Term, rf.votedFor, rf.currentTerm)
		return
	}

	// Reset timer
	rf.timerFire.Store(false)

	// log if is new leader
	if rf.votedFor != args.LeaderId {
		rf.info("new leader %d at term %d", args.LeaderId, args.Term)
	}
	rf.follow(args.LeaderId, args.Term)
	defer rf.persist()

	if len(args.Entries) == 0 {
		// Heartbeat
		rf.debug("heartbeat message from leader %d at term %d",
			args.LeaderId, args.Term)
		reply.Success = true
		return
	}

	// Sync log entries
	// Assert lastIndex <= args.PreLogIndex due to Election Restriction
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	if lastIndex < args.PrevLogIndex {
		// Out-of-date entries, request for prev entry
		rf.info("doesn't contain log entry at index %d, expect %d",
			args.PrevLogIndex, lastIndex)
		return
	}

	if lastTerm != args.PrevLogTerm {
		// Conflict term, delete, request for prev entry
		rf.log = rf.log[:lastIndex-1]
		rf.info("conflict entry at index %d of term %d, got term %d",
			lastIndex, lastTerm, args.PrevLogTerm)
		return
	}

	// Append new entries
	rf.log = append(rf.log, args.Entries...)
	last := len(rf.log) - 1

	// Update commit index
	oldcommit := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.info("update commit index %d ==> %d", oldcommit, rf.commitIndex)
	}
	reply.Success = true
	rf.info("last entry %d at term %d ==> %d at term %d",
		lastIndex, lastTerm, last, rf.log[last].Term)
}

func (rf *Raft) agreement(term int) {
	// init
	allPeers := len(rf.peers)
	rf.nextIndex = make([]int, allPeers)
	rf.matchIndex = make([]int, allPeers)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.info("init leader states")

	next := len(rf.log)
	ch := make(chan ReplicaLog)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
		go rf.agreementWith(term, i, ch)
	}

	half := allPeers / 2
	for rf.isLeader.Load() && !rf.killed() {
		rf.mu.Lock()
		last := len(rf.log) - 1
		commit := rf.commitIndex
		rf.mu.Unlock()

		if last == commit {
			rf.c.L.Lock()
			rf.c.Wait()
			rf.c.L.Unlock()
			continue
		}

		rf.info("replica log %d at term %d", last, term)
		// count replicas
		cnt := 0
		for rl := range ch {
			if rl.Index > last {
				cnt++
			}
			if cnt > half {
				// advance commit
				rf.mu.Lock()
				rf.info("commit log %d ==> %d at term %d", rf.commitIndex, last, term)
				rf.commitIndex = last
				rf.mu.Unlock()
				break
			}
		}
	}
}

func (rf *Raft) agreementWith(term, index int, ch chan ReplicaLog) {
	rf.info("establish agreement with follower %d at term %d", index, term)
	for rf.isLeader.Load() && !rf.killed() {
		rf.mu.Lock()
		next := len(rf.log)
		prev := rf.nextIndex[index] - 1
		commit := rf.commitIndex
		rf.mu.Unlock()

		if next-prev == 1 {
			// Wait for new entry arrive
			rf.c.L.Lock()
			rf.c.Wait()
			rf.c.L.Unlock()
			continue
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prev,
			PrevLogTerm:  rf.log[prev].Term,
			Entries:      rf.log[prev+1:],
			LeaderCommit: commit,
		}
		debug("send log[%d:] to follower %d", rf.nextIndex[index], index)
		reply := AppendEntriesReply{}
		if !rf.sendAppendEntries(index, &args, &reply) {
			// Failed to issue RPC, retry
			debug("failed to send RPC to follower %d, wait for retry...", index)
			time.Sleep(RETRY_INTERVAL)
			continue
		}
		if !reply.Success {
			if reply.Term > args.Term {
				rf.debug("current term %d, lastest is %d, convert to follower",
					args.Term, reply.Term)
				rf.mu.Lock()
				rf.follow(NIL_LEADER, reply.Term)
				rf.persist()
				rf.mu.Unlock()
				break
			}
			debug("server %d not contain entry %d, backoff to %d",
				index, rf.nextIndex[index], rf.nextIndex[index]-1)
			rf.nextIndex[index]--
			continue
		}

		info("update nextIndex[%d] from %d to %d at term %d",
			index, prev, next, term)
		// Update follower records
		rf.nextIndex[index] = next
		rf.matchIndex[index] = next - 1
	}
}
