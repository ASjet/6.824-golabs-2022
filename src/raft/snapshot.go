package raft

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
	rf.newCmd.L.Lock()
	defer rf.newCmd.L.Unlock()
	rf.apply <- ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  -1,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  rf.log[index-rf.offset-1].Term,
		SnapshotIndex: index,
	}
	rf.log = rf.log[index-rf.offset:]
	rf.offset = index
}
