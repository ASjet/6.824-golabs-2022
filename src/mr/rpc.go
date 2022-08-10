package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.
type AcquireArgs struct {
	WorkerID int
}

type AcquireReply struct {
	Task    Task
	NReduce int
}

type TaskDoneArgs struct {
	WorkerID int
	Type     int
	TaskID   int
	Output   string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	return "./824-mr-cord"
}
