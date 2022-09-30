package raft

import (
	"fmt"
	"sync/atomic"
)

type ReplicaLog struct {
	Index  int
	Server int
}

func (rf *Raft) applyLog() {
	rf.commitCond.L.Lock()
	commit := rf.commitIndex
	rf.commitCond.L.Unlock()

	rf.applyMu.Lock()
	for !rf.killed() {
		if rf.lastApplied < commit {
			last := rf.lastApplied
			// lock offset and log
			rf.logCond.L.Lock()
			for rf.lastApplied < commit {
				if rf.lastApplied < rf.offset {
					rf.lastApplied = rf.offset
				} else {
					rf.lastApplied++
				}
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.getLogCmd(rf.lastApplied),
					CommandIndex: rf.lastApplied,
				}
				// release newCmd while waiting for sending message to channel
				rf.logCond.L.Unlock()
				rf.apply <- msg
				rf.logCond.L.Lock()
			}
			rf.logCond.L.Unlock()
			rf.debug("applied log[%d:%d]", last+1, rf.lastApplied+1)
		}

		rf.commitCond.L.Lock()
		if rf.lastApplied < rf.commitIndex {
			commit = rf.commitIndex
			rf.commitCond.L.Unlock()
			continue
		}
		rf.debug("wait for new apply")
		rf.applyMu.Unlock()
		rf.commitCond.Wait()
		rf.applyMu.Lock()
		commit = rf.commitIndex
		rf.commitCond.L.Unlock()
	}
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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) agreement(term int) {
	rf.info("init leader states")

	// persist state
	rf.mu.Lock()
	rf.logCond.L.Lock()
	rf.persist()
	rf.logCond.L.Unlock()
	rf.mu.Unlock()

	// init follower records
	allPeers := len(rf.peers)
	rf.nextIndex = make([]atomic.Int32, allPeers)
	rf.matchIndex = make([]int, allPeers)

	rf.logCond.L.Lock()
	next := rf.lastLogIndex() + 1

	rf.info("Start establish agreement")
	rf.debug("%s", logStr(rf.log, rf.offset))
	rf.logCond.L.Unlock()

	rf.commitCond.L.Lock()
	rf.commitIndex = next - 1
	rf.commitCond.L.Unlock()

	// start agreement establish goroutine
	ch := make(chan ReplicaLog, allPeers)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i].Store(int32(next))
		rf.matchIndex[i] = 0
		go rf.agreementWith(term, i, ch)
	}

	half := allPeers / 2
	last := next

	// count replicas
	for rl := range ch {
		rf.matchIndex[rl.Server] = rl.Index
		// count >= N, be careful of count the same server multiple times
		cnt := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= last {
				cnt++
			}
		}
		rf.debug("match server %d at log index %d, now %d servers >= N(%d)",
			rl.Server, rl.Index, cnt, last)
		if cnt > half {
			// advance commit
			rf.commitCond.L.Lock()
			commitStart := rf.commitIndex + 1
			rf.commitIndex = last
			rf.info("commit log[%d:%d]", commitStart, last+1)
			rf.commitCond.L.Unlock()
			rf.commitCond.Signal()

			rf.logCond.L.Lock()
			for last == rf.lastLogIndex() {
				rf.debug("no log need to be commit, wait for new command")
				rf.logCond.Wait()
			}
			last = rf.lastLogIndex()
			rf.debug("update N to %d", last)
			rf.logCond.L.Unlock()
		}
	}
	for range ch {
		// Drain the channel
	}
}

func (rf *Raft) agreementWith(term, index int, ch chan ReplicaLog) {
	rf.info("establish agreement with follower %d @%d", index, term)
	init := true
	for rf.isLeader.Load() && !rf.killed() {
		rf.logCond.L.Lock()
		next := int(rf.nextIndex[index].Load())

		last := rf.lastLogIndex()
		for (!init || last <= 0) && last < next {
			// Wait for new command arrive
			rf.debug("nextIndex: %d, lastLog: %d@%d, wait for new command...",
				next, last, rf.getLogTerm(last))
			rf.logCond.Wait()
			last = rf.lastLogIndex()
		}
		init = false
		prev := next - 1

		if rf.offset > 0 && prev-rf.offset < 0 {
			// the entry is not in current logs, send snapshot instead
			args := SnapshotArgs{
				Term:      term,
				LeaderId:  rf.me,
				LastIndex: rf.offset,
				LastTerm:  rf.log[0].Term,
				Offset:    0,
				Data:      rf.persister.ReadSnapshot(),
				Done:      true,
			}
			reply := SnapshotReply{}
			rf.debug("send snapshot %d@%d to follower %d",
				args.LastIndex, args.LastTerm, index)
			rf.logCond.L.Unlock()
			if !rf.sendInstallSnapshot(index, &args, &reply) {
				if !rf.isLeader.Load() {
					return
				}
				// Failed to issue RPC, retry
				rf.warn("failed to send installSnapshot to follower %d, retring...", index)
				continue
			}
			curterm, isleader := rf.GetState()
			if curterm > term || !isleader {
				return
			}
			// Stale leader
			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.isLeader.Load() {
					rf.warn("stale term %d, latest %d, convert to follower",
						args.Term, reply.Term)
					rf.follow(NIL_LEADER, reply.Term)
					rf.logCond.L.Lock()
					rf.persist()
					rf.logCond.L.Unlock()
					rf.mu.Unlock()
					close(ch)
					return
				}
				rf.mu.Unlock()
				return
			}

			rf.info("update nextIndex[%d] from %d to %d @%d",
				index, next, last+1, term)
			// Update follower records
			rf.nextIndex[index].Store(int32(last + 1))
			ch <- ReplicaLog{last, index}
			continue
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prev,
			PrevLogTerm:  rf.getLogTerm(prev),
			Entries:      rf.logSlice(next, last+1),
		}
		rf.logCond.L.Unlock()

		rf.commitCond.L.Lock()
		args.LeaderCommit = rf.commitIndex
		rf.commitCond.L.Unlock()

		rf.debug("send log[%d:%d] to follower %d, prev %d@%d, commit %d",
			next, last+1, index, args.PrevLogIndex,
			args.PrevLogTerm, args.LeaderCommit)

		reply := AppendEntriesReply{}
		if !rf.sendAppendEntries(index, &args, &reply) {
			if !rf.isLeader.Load() {
				return
			}
			// Failed to issue RPC, retry
			rf.warn("failed to send AppendEntries to follower %d, retring...", index)
			continue
		}
		curterm, isleader := rf.GetState()
		if curterm > term || !isleader {
			return
		}
		if !reply.Success {
			// Stale leader
			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.isLeader.Load() {
					rf.warn("stale term %d, latest %d, convert to follower",
						args.Term, reply.Term)
					rf.follow(NIL_LEADER, reply.Term)
					rf.logCond.L.Lock()
					rf.persist()
					rf.logCond.L.Unlock()
					rf.mu.Unlock()
					close(ch)
					return
				}
				rf.mu.Unlock()
				return
			}
			// Follower doesn't contain the entry at prevLogIndex
			if args.PrevLogIndex > reply.XLen {
				rf.nextIndex[index].Store(int32(reply.XLen + 1))
				rf.debug("server %d doesn't contain log at %d, backoff to %d",
					index, args.PrevLogIndex, reply.XLen)
				continue
			}
			// Follower contain conflict entry at prevLogIndex
			if rf.offset > reply.XIndex {
				// send snapshot
				continue
			}
			rf.logCond.L.Lock()
			oldNextIndex := rf.nextIndex[index].Load()
			if rf.getLogTerm(reply.XIndex) == reply.XTerm {
				// The leader has entries at term reply.XTerm
				// Get the last index of log entry at term reply.XTerm
				for i := reply.XIndex; i < reply.XLen; i++ {
					if rf.getLogTerm(i+1) > reply.XTerm {
						rf.nextIndex[index].Store(int32(i))
						break
					}
				}
			} else {
				// The leader doesn't have entry at term reply.XTerm
				rf.nextIndex[index].Store(int32(reply.XIndex))
			}
			newNextIndex := rf.nextIndex[index].Load()
			rf.debug("server %d has conflict log %d@%d, backoff to %d@%d",
				index, oldNextIndex-1, reply.XTerm,
				// newNextIndex-1, rf.log[newNextIndex-1-int32(rf.offset)].Term)
				newNextIndex-1, rf.getLogTerm(int(newNextIndex)-1))
			rf.logCond.L.Unlock()
			continue
		} else {
			rf.info("update nextIndex[%d] from %d to %d @%d",
				index, next, last+1, term)
			// Update follower records
			rf.nextIndex[index].Store(int32(last + 1))
			ch <- ReplicaLog{last, index}
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

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.debug("stale leader %d @%d, current leader %d @%d",
			args.LeaderId, args.Term, rf.votedFor, rf.currentTerm)
		return
	}
	// Reset timer
	rf.timerFire.Store(false)

	if rf.currentTerm < args.Term || rf.votedFor != args.LeaderId {
		rf.info("new leader %d @%d", args.LeaderId, args.Term)
		rf.follow(args.LeaderId, args.Term)
	}

	// Sync log entries, assert (lastIndex <= PrevLogIndex) or
	// (lastTerm < PrevLogTerm && lastIndex > args.PrevLogIndex)
	// due to Election Restriction
	rf.commitCond.L.Lock()
	defer rf.commitCond.L.Unlock()
	rf.logCond.L.Lock()
	defer rf.logCond.L.Unlock()

	lastIndex := rf.lastLogIndex()
	if lastIndex < 0 {
		lastIndex = rf.offset
	}

	reply.XLen = lastIndex
	if lastIndex-rf.offset < 0 {
		fmt.Printf("lastIndex: %d, rf.offset: %d, log length: %d\n",
			lastIndex, rf.offset, len(rf.log))
	}
	lastTerm := rf.getLogTerm(lastIndex)
	prevIndex := args.PrevLogIndex

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	//    whose term matches prevLogTerm
	if lastIndex < prevIndex {
		rf.info("doesn't contain log entry at index %d, expect %d",
			prevIndex, lastIndex)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but
	//    different terms), delete the existing entry and all that follow it
	prevTerm := rf.getLogTerm(prevIndex)
	if prevTerm != args.PrevLogTerm {
		rf.trimLog(prevIndex, -1)
		if len(rf.log) == 0 {
			rf.log = []LogEntry{{args.PrevLogTerm, nil}}
		}
		rf.info("conflict log entry %d@%d, got %d@%d, drop log[%d:]",
			prevIndex, prevTerm, prevIndex, args.PrevLogTerm, prevIndex)
		rf.debug("new log length %d", len(rf.log))
		// return false in case of more than one conflict entry
		reply.XTerm = prevTerm
		reply.XIndex = 1
		for i := prevIndex; i-rf.offset > 0; i-- {
			if rf.getLogTerm(i-1) < prevTerm {
				reply.XIndex = i
				break
			}
		}
		return
	}

	reply.Success = true

	if len(args.Entries) == 0 {
		// Heartbeat, dont move forward
		rf.debug("heartbeat from leader %d @%d",
			args.LeaderId, args.Term)
	} else {

		rf.debug("new entries from leader %d @%d",
			args.LeaderId, args.Term)
		rf.debug("got%s", logStr(args.Entries, args.PrevLogIndex+1))

		rf.debug("PrevLog: %d@%d, lastLog: %d@%d, commit: %d",
			args.PrevLogIndex, args.PrevLogTerm, lastIndex, lastTerm,
			args.LeaderCommit)

		// 4. Append any new entries not already in the log
		rf.trimLog(prevIndex+1, -1)
		rf.log = append(rf.log, args.Entries...)
		lastIndex := rf.lastLogIndex()
		rf.debug("append at [%d:%d] from leader %d @%d, now lastLog %d@%d",
			prevIndex+1, lastIndex+1, args.LeaderId, args.Term,
			lastIndex, rf.getLogTerm(lastIndex))
		rf.debug("new log length %d", len(rf.log))
		rf.debug("%s", logStr(rf.log, rf.offset))
	}

	// 5. If leaderCommit > commitIndex,
	//    set commitIndex = min(leaderCommit, index of last new entry)
	oldcommit := rf.commitIndex
	if oldcommit != lastIndex && args.LeaderCommit > oldcommit {
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.info("update commit index %d@%d ==> %d@%d",
			oldcommit, rf.getLogTerm(oldcommit),
			rf.commitIndex, rf.getLogTerm(rf.commitIndex))
		rf.commitCond.Signal()
	}
	rf.persist()
}
