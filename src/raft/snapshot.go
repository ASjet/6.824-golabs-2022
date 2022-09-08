package raft

// 1. the tester calls Snapshot() periodically

// 2. If the leader that going to send log is before the offset, it should call
// InstallSnapshot RPC instead of AppendEntries RPC

// 3. The CondInstallSnapshot should just return true

// When should send snapshot message via applyCh?

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
	Offset    int
	Data      []byte
	Done      bool
}

type SnapshotReply struct {
	Term int
}

//  1. Reply immediately if term < currentTerm
//  2. Create new snapshot file if first chunk (offset is 0)
//  3. Write data into snapshot file at given offset
//  4. Reply and wait for more data chunks if done is false
//  5. Save snapshot file, discard any existing or partial snapshot with a smaller index
//  6. If existing log entry has same index and term as snapshot’s last
//     included entry, retain log entries following it and reply
//  7. Discard the entire log
//  8. Reset state machine using snapshot contents (and load
//
// snapshot’s cluster configuration)
// This is a RPC call
func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.ss.Lock()
	defer rf.ss.Unlock()
	// 2. Create new snapshot file if first chunk (offset is 0)
	if len(rf.snapshotBuf) == 0 {
		rf.snapshotBuf = args.Data
	} else {
		// 3. Write data into snapshot file at given offset
		rf.snapshotBuf = append(rf.snapshotBuf[:args.Offset], args.Data...)
	}

	// 4. Reply and wait for more data chunks if done is false
	if !args.Done {
		return
	}

	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.debug("make snapshot up to index %d", index)
	rf.ss.Lock()
	rf.snapshotBuf = snapshot
	rf.ss.Unlock()
	rf.newCmd.L.Lock()
	defer rf.newCmd.L.Unlock()
	// keep the log at index as the dummy head which has index 0
	if index-rf.offset >= 0 {
		rf.log = rf.log[index-rf.offset:]
		rf.debug("discarded log[:%d]([:%d])", index, index-rf.offset)
		rf.offset = index
		rf.debug("%s", logStr(rf.log, rf.offset))
	} else {
		rf.debug("snapshot index is higher than last, expect at most %d, got %d",
			len(rf.log)+rf.offset-1, index)
	}
}
