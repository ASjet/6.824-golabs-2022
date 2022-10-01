package raft

// 1. the tester calls Snapshot() periodically

// 2. If the leader that going to send log is before the offset, it should call
// InstallSnapshot RPC instead of AppendEntries RPC

// 3. The CondInstallSnapshot should just return true

// 4. After the server received InstallSnapshot RPC, it send snapshot via applyCh

// 5. When a server restarts, the application layer reads the persisted snapshot
//    and restores its saved state.

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int,
	snapshot []byte) bool {

	// Your code here (2D).

	return true
}

type SnapshotArgs struct {
	Term      int
	LeaderId  int
	LastIndex int
	LastTerm  int
	Offset    int // Deprecated
	Data      []byte
	Done      bool // Deprecated
}

type SnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs,
	reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(term, index, last, next int) (bool, ReplicaLog) {
	args := SnapshotArgs{
		Term:      term,
		LeaderId:  rf.me,
		LastIndex: rf.offset,
		LastTerm:  rf.getLogTerm(rf.offset),
		Offset:    0,
		Data:      rf.persister.ReadSnapshot(),
		Done:      true,
	}
	reply := SnapshotReply{}
	rf.debug("send snapshot %d@%d to follower %d",
		args.LastIndex, args.LastTerm, index)
	rf.logCond.L.Unlock()
	if !rf.sendInstallSnapshot(index, &args, &reply) {
		if rf.isLeader.Load() {
			// Failed to issue RPC, retry
			rf.warn("failed to send installSnapshot to follower %d, retring...", index)
		}
		return false, ReplicaLog{}
	}
	curterm, isleader := rf.GetState()
	if curterm > term || !isleader {
		// stale term
		return false, ReplicaLog{}
	}
	if reply.Term > args.Term {
		// Stale leader
		rf.mu.Lock()
		if rf.isLeader.Load() {
			rf.warn("stale term %d, latest %d, convert to follower",
				args.Term, reply.Term)
			rf.follow(NIL_LEADER, reply.Term)
			rf.logCond.L.Lock()
			rf.persist()
			rf.logCond.L.Unlock()
		}
		rf.mu.Unlock()
		return false, ReplicaLog{}
	}

	rf.info("update nextIndex[%d] from %d to %d @%d",
		index, next, last+1, term)
	// Update follower records
	rf.nextIndex[index].Store(int32(last + 1))
	return true, ReplicaLog{last, index}
}

// This is a RPC call
func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.debug("receive snapshot([:%d]) from leader %d @%d",
		args.LastIndex+1, args.LeaderId, args.Term)

	curterm, isleader := rf.GetState()
	reply.Term = curterm

	// 1. Reply immediately if term < currentTerm
	if isleader && args.Term < curterm {
		rf.debug("reject snapshot from server %d @%d, current @%d",
			args.LeaderId, args.Term, curterm)
		return
	}

	rf.mu.Lock()
	if rf.votedFor != args.LeaderId || curterm != args.Term {
		// Update leader
		rf.debug("new leader %d @%d", args.LeaderId, args.Term)
		rf.follow(args.LeaderId, args.Term)
	}

	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.logCond.L.Lock()

	if args.LastIndex < rf.offset {
		rf.logCond.L.Unlock()
		rf.mu.Unlock()
		return
	}

	lastIndex := rf.lastLogIndex()
	rf.debug("currtne log length %d, offset %d", len(rf.log), rf.offset)
	//  6. If existing log entry has same index and term as snapshot’s last included entry,
	//     retain log entries following it and reply
	if lastIndex >= args.LastIndex && rf.getLogTerm(args.LastIndex) == args.LastTerm {
		rf.debug("trim log[:%d]", args.LastIndex)
		// keep the log at index as the dummy head which has index 0
		rf.trimLog(-1, args.LastIndex)
		rf.debug("new log length %d", len(rf.log))
	} else {
		//  7. Discard the entire log
		rf.debug("discard entire log")
		rf.log = []LogEntry{{args.LastTerm, nil}}
		rf.debug("new log length %d", len(rf.log))
		rf.offset = args.LastIndex
	}

	rf.debug("set offset to %d", args.LastIndex)

	rf.commitCond.L.Lock()
	rf.applyMu.Lock()

	rf.persistSnapshot(args.Data)
	rf.lastApplied = args.LastIndex
	rf.commitIndex = args.LastIndex

	rf.applyMu.Unlock()
	rf.commitCond.L.Unlock()
	rf.logCond.L.Unlock()
	rf.mu.Unlock()

	//  8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.debug("send snapshot message to upper layer, last %d@%d", args.LastIndex, args.LastTerm)
	rf.apply <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastTerm,
		SnapshotIndex: args.LastIndex,
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.debug("make snapshot up to index %d", index)
	rf.mu.Lock()
	rf.logCond.L.Lock()

	last := rf.lastLogIndex()
	if last < index {
		rf.warn("snapshot index is higher than last, expect at most %d, got %d",
			last, index)
		rf.commitCond.L.Lock()
		rf.applyMu.Lock()
		rf.debug("discard entire log")
		rf.log = []LogEntry{{rf.currentTerm, nil}}
		rf.offset = index
		rf.lastApplied = index
		rf.commitIndex = index
		rf.applyMu.Unlock()
		rf.commitCond.L.Unlock()
		rf.persistSnapshot(snapshot)
	} else {
		rf.debug("discard log[:%d]([:%d])", index, index-rf.offset)
		rf.trimLog(-1, index)
		rf.debug("new log length %d", len(rf.log)-1)
		rf.debug("%s", logStr(rf.log, rf.offset))
		rf.persistSnapshot(snapshot)
	}

	rf.logCond.L.Unlock()
	rf.mu.Unlock()
}
