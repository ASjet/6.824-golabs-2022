package raft

import (
	"bytes"

	"6.824/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.offset)
	rf.persister.SaveRaftState(buf.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	buf := bytes.NewBuffer(data)
	d := labgob.NewDecoder(buf)
	var term, voted, offset int
	var log []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voted) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&offset) != nil {
		rf.warn("unable to read persist data")
	} else {
		rf.currentTerm = term
		rf.votedFor = voted
		rf.log = log
		rf.offset = offset
	}
}
