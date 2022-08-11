package mr

import "fmt"

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.
type RegisterReply struct {
	WorkerID int
}

type PingArgs struct {
	ID    int
	State int
}

type TaskDoneArgs struct {
	WorkerID int
	Type     int
	TaskID   int
	Output   []string
}

func (t *TaskDoneArgs) String() string {
	return fmt.Sprintf("Done{worker_%d %s %d-%v}", t.WorkerID, taskname[t.Type], t.TaskID, t.Output)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	return "./824-mr-cord"
}
