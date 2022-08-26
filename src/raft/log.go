package raft

import (
	"strconv"
	"strings"
	"time"
)

const (
	RETRY_INTERVAL = time.Millisecond * 100
)

type ReplicaLog struct {
	Index  int
	Server int
}

func logStr(log []LogEntry, offset int) string {
	s := strings.Builder{}
	s.WriteRune('|')
	for i, l := range log {
		if i+offset == 0 {
			continue
		}
		s.WriteString(strconv.Itoa(i + offset))
		s.WriteRune('@')
		s.WriteString(strconv.Itoa(l.Term))
		s.WriteRune('|')
	}
	return s.String()
}

func (rf *Raft) applyLog() {
	rf.applyCmd.L.Lock()
	last := rf.lastApplied
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.apply <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}
	now := rf.lastApplied
	rf.applyCmd.L.Unlock()
	rf.debug("applied log[%d:%d]", last+1, now+1)
	rf.applyCmd.Signal()
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

func (rf *Raft) agreement(term int) {
	// init
	allPeers := len(rf.peers)
	rf.nextIndex = make([]int, allPeers)
	rf.matchIndex = make([]int, allPeers)
	rf.applyCmd.L.Lock()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCmd.L.Unlock()
	rf.newCmd.L.Lock()
	next := len(rf.log)
	rf.newCmd.L.Unlock()
	rf.info("init leader states")

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
	rf.newCmd.L.Lock()
	last := len(rf.log) - 1
	rf.debug("%s", logStr(rf.log, 0))
	rf.newCmd.L.Unlock()
	if last == 0 {
		last = 1
	}

	// count replicas
	cnt := 0
	for rl := range ch {
		rf.matchIndex[rl.Server] = rl.Index
		rf.debug("match server %d at log %d@%d",
			rl.Server, rl.Index, rf.log[rl.Index].Term)
		if rl.Index >= last {
			cnt++
		}
		if cnt > half {
			// advance commit
			rf.applyCmd.L.Lock()
			oldcommit := rf.commitIndex + 1
			rf.commitIndex = last
			rf.info("commit log[%d:%d]", oldcommit, last+1)
			rf.applyCmd.L.Unlock()

			go rf.applyLog()

			rf.newCmd.L.Lock()
			last = len(rf.log) - 1
			rf.newCmd.L.Unlock()
		}
	}
}

func (rf *Raft) agreementWith(term, index int, ch chan ReplicaLog) {
	rf.info("establish agreement with follower %d at term %d", index, term)
	for rf.isLeader.Load() && !rf.killed() {
		rf.newCmd.L.Lock()
		last := len(rf.log)
		next := rf.nextIndex[index]
		prev := next - 1

		if next > 0 && last-next == 0 {
			// Wait for new command arrive
			rf.debug("wait for new command...")
			rf.newCmd.Wait()
			rf.newCmd.L.Unlock()
			continue
		}
		rf.newCmd.L.Unlock()
		if prev < 0 {
			prev = 0
		}

		rf.applyCmd.L.Lock()
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prev,
			PrevLogTerm:  rf.log[prev].Term,
			Entries:      rf.log[next:last],
			LeaderCommit: rf.commitIndex,
		}
		rf.applyCmd.L.Unlock()

		rf.debug("send log[%d:%d]%s to follower %d, prev %d@%d",
			next, last, logStr(args.Entries, next), index,
			args.PrevLogIndex, args.PrevLogTerm)

		reply := AppendEntriesReply{}
		if !rf.sendAppendEntries(index, &args, &reply) {
			// Failed to issue RPC, retry
			rf.warn("failed to send RPC to follower %d, retring...", index)
			time.Sleep(RETRY_INTERVAL)
			continue
		}
		if !reply.Success {
			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.isLeader.Load() {
					rf.warn("stale term %d, latest %d, convert to follower",
						args.Term, reply.Term)
					rf.follow(NIL_LEADER, reply.Term)
					rf.newCmd.L.Lock()
					rf.persist()
					rf.newCmd.L.Unlock()
					rf.mu.Unlock()
					// THIS MAY CAUSE DATA RACE AND PANIC ON SENDING CLOSE CHAN
					close(ch)
					return
				}
				rf.mu.Unlock()
				break
			}
			nextIndex := rf.nextIndex[index]
			rf.debug("server %d not contain log %d@%d, backoff to %d@%d",
				index, nextIndex, rf.log[nextIndex].Term,
				nextIndex-1, rf.log[nextIndex-1].Term)
			rf.nextIndex[index]--
			continue
		}

		rf.info("update nextIndex[%d] from %d to %d at term %d",
			index, next, last, term)
		// Update follower records
		rf.nextIndex[index] = last
		if rf.isLeader.Load() {
			ch <- ReplicaLog{last - 1, index}
		}
	}
}

// This is a RPC call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	if rf.currentTerm < args.Term || rf.votedFor != args.LeaderId {
		rf.info("new leader %d at term %d", args.LeaderId, args.Term)
		rf.follow(args.LeaderId, args.Term)
	}

	// Sync log entries, assert (lastIndex <= PrevLogIndex) or
	// (lastTerm < PrevLogTerm && lastIndex > args.PrevLogIndex)
	// due to Election Restriction
	rf.newCmd.L.Lock()
	defer rf.newCmd.L.Unlock()

	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	// Update commit index
	oldcommit := rf.commitIndex
	if args.LeaderCommit > oldcommit {
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.info("update commit index %d@%d ==> %d@%d",
			oldcommit, rf.log[oldcommit].Term,
			rf.commitIndex, rf.log[rf.commitIndex].Term)
		go rf.applyLog()
	}

	if len(args.Entries) == 0 {
		// Heartbeat
		rf.debug("heartbeat from leader %d at term %d",
			args.LeaderId, args.Term)
		reply.Success = true
		return
	}

	rf.debug("new entries%s from leader %d at term %d",
		logStr(args.Entries, args.PrevLogIndex+1), args.LeaderId, args.Term)

	rf.debug("PrevLog: %d@%d, lastLog: %d@%d",
		args.PrevLogIndex, args.PrevLogTerm, lastIndex, lastTerm)
	rf.debug("%s", logStr(rf.log, 0))

	if lastIndex < args.PrevLogIndex {
		// Out-of-date entries, request for prev entry
		rf.info("doesn't contain log entry at index %d, expect %d",
			args.PrevLogIndex, lastIndex)
		return
	}

	lastIndex = args.PrevLogIndex
	lastTerm = rf.log[lastIndex].Term

	if lastTerm != args.PrevLogTerm {
		// Conflict term, delete followed entries, request for prev entry
		rf.log = rf.log[:lastIndex]
		rf.info("conflict: local entry %d@%d, got %d@%d",
			lastIndex, lastTerm, args.PrevLogIndex, args.PrevLogTerm)
		rf.persist()
		return
	}

	// Append new entries from index args.PrevLogIndex+1
	rf.log = append(rf.log[:lastIndex+1], args.Entries...)
	last := len(rf.log) - 1
	rf.debug("append log entries at [%d:%d] from leader %d at term %d",
		lastIndex+1, last+1, args.LeaderId, args.Term)
	rf.debug("%s", logStr(rf.log, 0))

	reply.Success = true
	rf.persist()
}
