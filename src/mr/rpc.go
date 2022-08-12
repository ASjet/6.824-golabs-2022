package mr

import (
	"fmt"
	"os"
	"strconv"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.
type RegistryArgs struct {
	Sock string
}
type RegistryReply struct {
	WorkerID int
	Sockname string
}

type PingArgs struct {
	ID    int
	State int
}

type PingReply struct {
	State    int
	TaskType int
	TaskID   int
}

type TaskDoneArgs struct {
	WorkerID int
	Type     int
	TaskID   int
	Output   []string
}

type TaskDoneReply struct {
	SyncState int
}

func (t *TaskDoneArgs) String() string {
	return fmt.Sprintf("Done{worker_%d %s %d-%v}", t.WorkerID, taskname[t.Type], t.TaskID, t.Output)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
