package raft

import (
	"strconv"
	"strings"
	"time"
)

const (
	RETRY_INTERVAL = time.Millisecond * 10
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
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			last := rf.lastApplied
			rf.newCmd.L.Lock()
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				rf.apply <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
			}
			rf.newCmd.L.Unlock()
			rf.debug("applied log[%d:%d]", last+1, rf.lastApplied+1)
		}
		rf.applyCmd.Wait()
	}
	rf.applyCmd.L.Unlock()
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
	rf.info("init leader states")
	allPeers := len(rf.peers)
	rf.nextIndex = make([]int, allPeers)
	rf.matchIndex = make([]int, allPeers)
	rf.applyCmd.L.Lock()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCmd.L.Unlock()
	rf.newCmd.L.Lock()
	next := len(rf.log) - 1
	if next == 0 {
		// If there is no log, wait for new command
		rf.debug("log is empty, wait for new command")
		rf.newCmd.Wait()
	}
	next = len(rf.log) - 1
	rf.info("Start establish agreement at")
	rf.debug("%s", logStr(rf.log, 0))
	rf.newCmd.L.Unlock()

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
			rf.applyCmd.L.Lock()
			commitStart := rf.commitIndex + 1
			rf.commitIndex = last
			rf.info("commit log[%d:%d]", commitStart, last+1)
			rf.applyCmd.L.Unlock()
			rf.applyCmd.Signal()

			rf.newCmd.L.Lock()
			if last == len(rf.log)-1 {
				rf.debug("no log need to be commit, wait for new command")
				rf.newCmd.Wait()
			}
			last = len(rf.log) - 1
			rf.newCmd.L.Unlock()
		}
	}
}

func (rf *Raft) agreementWith(term, index int, ch chan ReplicaLog) {
	rf.info("establish agreement with follower %d at term %d", index, term)
	for rf.isLeader.Load() && !rf.killed() {
		rf.newCmd.L.Lock()
		last := len(rf.log) - 1
		next := rf.nextIndex[index]
		prev := next - 1

		if next > 0 && last-next < 0 {
			// Wait for new command arrive
			rf.debug("nextIndex: %d, lastLog: %d@%d, wait for new command...",
				next, last, rf.log[last].Term)
			rf.newCmd.Wait()
			rf.newCmd.L.Unlock()
			continue
		}
		if prev < 0 {
			prev = 0
		}
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prev,
			PrevLogTerm:  rf.log[prev].Term,
			Entries:      rf.log[next : last+1],
		}
		rf.newCmd.L.Unlock()

		rf.applyCmd.L.Lock()
		args.LeaderCommit = rf.commitIndex
		rf.applyCmd.L.Unlock()

		rf.debug("send log[%d:%d] to follower %d, prev %d@%d, commit %d",
			next, last+1, index, args.PrevLogIndex,
			args.PrevLogTerm, args.LeaderCommit)
		rf.debug("sent%s", logStr(args.Entries, next))

		reply := AppendEntriesReply{}
		if !rf.sendAppendEntries(index, &args, &reply) {
			if !rf.isLeader.Load() {
				return
			}
			// Failed to issue RPC, retry
			rf.warn("failed to send RPC to follower %d, retring...", index)
			time.Sleep(RETRY_INTERVAL)
			continue
		}
		curterm, isleader := rf.GetState()
		if curterm > term || !isleader {
			return
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
					close(ch)
					return
				}
				rf.mu.Unlock()
				return
			}
			nextIndex := rf.nextIndex[index]
			rf.newCmd.L.Lock()
			rf.debug("server %d not contain log %d@%d, backoff to %d@%d",
				index, nextIndex, rf.log[nextIndex].Term,
				nextIndex-1, rf.log[nextIndex-1].Term)
			rf.newCmd.L.Unlock()
			rf.nextIndex[index]--
			continue
		} else {
			rf.info("update nextIndex[%d] from %d to %d at term %d",
				index, next, last+1, term)
			// Update follower records
			rf.nextIndex[index] = last + 1
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
	prevTerm := rf.log[prevIndex].Term
	if prevTerm != args.PrevLogTerm {
		rf.log = rf.log[:prevIndex]
		rf.info("conflict log entry %d@%d, got %d@%d, drop log[%d:]",
			prevIndex, prevTerm, prevIndex, args.PrevLogTerm, prevIndex)
		// return false in case of more than one conflict entry
		return
	}

	if len(args.Entries) == 0 {
		// Heartbeat
		rf.debug("heartbeat from leader %d at term %d",
			args.LeaderId, args.Term)
	} else {
		rf.debug("new entries from leader %d at term %d",
			args.LeaderId, args.Term)
		rf.debug("got%s", logStr(args.Entries, args.PrevLogIndex+1))

		rf.debug("PrevLog: %d@%d, lastLog: %d@%d",
			args.PrevLogIndex, args.PrevLogTerm, lastIndex, lastTerm)

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log[:prevIndex+1], args.Entries...)
		lastIndex := len(rf.log) - 1
		rf.debug("append at [%d:%d] from leader %d @%d, now lastLog %d@%d",
			prevIndex+1, lastIndex+1, args.LeaderId, args.Term,
			lastIndex, rf.log[lastIndex].Term)
		rf.debug("%s", logStr(rf.log, 0))
	}

	// 5. If leaderCommit > commitIndex,
	//    set commitIndex = min(leaderCommit, index of last new entry)
	rf.applyCmd.L.Lock()
	oldcommit := rf.commitIndex
	if oldcommit != lastIndex && args.LeaderCommit > oldcommit {
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.info("update commit index %d@%d ==> %d@%d",
			oldcommit, rf.log[oldcommit].Term,
			rf.commitIndex, rf.log[rf.commitIndex].Term)
		rf.applyCmd.Signal()
	}
	rf.applyCmd.L.Unlock()

	reply.Success = true
	rf.persist()
}
