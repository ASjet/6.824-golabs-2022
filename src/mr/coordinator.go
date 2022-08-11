package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	IDLE int = iota
	IN_PROGRESS
	COMPLETED

	WAIT
	MAP
	REDUCE
	EXIT

	DEBUG          = false
	INFO           = true
	HEALTH_TIMEOUT = time.Second * 10
	WAIT_DURATION  = time.Millisecond * 1
)

var statename = []string{
	IDLE:        "IDLE",
	IN_PROGRESS: "IN_PROGRESS",
	COMPLETED:   "COMPLETED",
}

var taskname = []string{
	WAIT:   "WAIT",
	MAP:    "MAP",
	REDUCE: "REDUCE",
	EXIT:   "EXIT",
}

func debug(fmts string, args ...any) {
	if DEBUG {
		log.Printf(fmts, args...)
	}
}

func info(fmts string, args ...any) {
	if INFO || DEBUG {
		fmt.Printf(fmts+"\n", args...)
	}
}

type Input struct {
	Paths  []string
	State  int
	Worker *WorkerRecord
	Mu     sync.Mutex
}

func (i *Input) AssignTo(w *WorkerRecord) {
	i.Mu.Lock()
	i.State = IN_PROGRESS
	i.Worker = w
	i.Mu.Unlock()
}

func (i *Input) DoneBy(w *WorkerRecord) bool {
	defer i.Mu.Unlock()
	i.Mu.Lock()
	if i.State != COMPLETED {
		i.State = COMPLETED
		i.Worker = w
		w.AssignNewTask(&TASK_WAIT)
		return true
	}
	return false
}

// Worker task
type Task struct {
	Type    int
	ID      int
	NReduce int
	Paths   []string
	Read    bool
}

func (t Task) String() string {
	return fmt.Sprintf("Task{%s: %d-%v, read: %v}", taskname[t.Type], t.ID, t.Paths, t.Read)
}

func (t *Task) CopyTo(dst *Task) {
	dst.ID = t.ID
	dst.Type = t.Type
	dst.NReduce = t.NReduce
	dst.Read = false
	dst.Paths = dst.Paths[:0]
	dst.Paths = append(dst.Paths, t.Paths...)
}

func (t Task) State() int {
	if t.Type == MAP || t.Type == REDUCE {
		return IN_PROGRESS
	} else {
		return IDLE
	}
}

var TASK_EXIT = Task{EXIT, -1, 0, nil, false}
var TASK_WAIT = Task{WAIT, -1, 0, nil, false}

// Worker record
type WorkerRecord struct {
	Sock    string
	State   int
	task    *Task
	LastCom time.Time
	Updated bool
	Mu      sync.Mutex
}

func (w *WorkerRecord) String() string {
	return fmt.Sprintf("Worker{%s: %s, updated: %v}", statename[w.State], w.task, w.Updated)
}

func (w *WorkerRecord) FreshState(state int) *WorkerRecord {
	w.Mu.Lock()
	w.State = state
	w.LastCom = time.Now()
	w.Mu.Unlock()
	return w
}

func (w *WorkerRecord) IsTimeout() bool {
	defer w.Mu.Unlock()
	w.Mu.Lock()
	dura := time.Since(w.LastCom)
	return dura > HEALTH_TIMEOUT
}

func (w *WorkerRecord) AssignNewTask(t *Task) *WorkerRecord {
	w.Mu.Lock()
	debug("AssignNewTask: Assign new task %s to %s.", t, w)
	w.State = t.State()
	w.task = t
	w.Updated = true
	w.Mu.Unlock()
	debug("AssignNewTask: now the worker is %s", w)
	return w
}

func (w *WorkerRecord) HasNewTask(t *Task) bool {
	defer w.Mu.Unlock()
	w.Mu.Lock()
	if w.Updated {
		debug("HasNewTask: new task in worker %s", w)
		w.task.CopyTo(t)
		w.Updated = false
		debug("HasNewTask: new task assigned: %s", t)
		return true
	} else {
		t.Read = true
	}
	return false
}

// Coordinator data struct
type Coordinator struct {
	// Your definitions here.
	Workers  []*WorkerRecord
	WorkerMu sync.Mutex

	maps    []Input
	mapLeft atomic.Int32

	nReduce    int
	reduceLeft atomic.Int32
	reduces    []Input

	// MapTask sync.WaitGroup
	// mapDone    int32
	// reduceDone int32

	AllDone bool
}

// Your code here -- RPC handlers for the worker to call.

// Register worker to coordinator
func (c *Coordinator) RegistryWorker(args *Info, reply *RegisterReply) error {
	debug("RegistryWorker: New worker registration")
	record := WorkerRecord{Sock: args.Sock}
	record.FreshState(IDLE).AssignNewTask(&TASK_WAIT)
	c.WorkerMu.Lock()
	// Worker ID
	id := len(c.Workers)
	c.Workers = append(c.Workers, &record)
	c.WorkerMu.Unlock()
	reply.WorkerID = id
	info("RegistryWorker: assign work id: %d", id)
	return nil
}

// Post worker health status to coordinator
func (c *Coordinator) Ping(args *PingArgs, reply *Task) error {
	debug("Ping: worker_%d: %s", args.ID, statename[args.State])
	defer c.WorkerMu.Unlock()
	c.WorkerMu.Lock()
	if args.ID > len(c.Workers) {
		return fmt.Errorf("ping:invalid worker id: %d", args.ID)
	}
	w := c.Workers[args.ID]
	if !w.HasNewTask(reply) {
		w.FreshState(args.State)
	}
	debug("Ping: reply: %s", reply)
	return nil
}

func (c *Coordinator) GetWorker() *WorkerRecord {
	for {
		c.WorkerMu.Lock()
		for _, w := range c.Workers {
			w.Mu.Lock()
			if w.State != IDLE {
				w.Mu.Unlock()
				continue
			}
			w.Mu.Unlock()
			c.WorkerMu.Unlock()
			return w
		}
		c.WorkerMu.Unlock()
		// Wait until worker avaliable
		debug("GetWorker: Waiting for avaliable worker")
		time.Sleep(WAIT_DURATION)
	}
}

func (c *Coordinator) Schedule() {
	// Do not schedule reduce task until all map tasks done
	info("Schedule: Start schedule map tasks")
	for c.mapLeft.Load() > 0 {
		// Pick a map task
		for mid := range c.maps {
			m := &c.maps[mid]
			m.Mu.Lock()
			if m.State != IDLE {
				m.Mu.Unlock()
				continue
			}
			t := &Task{MAP, mid, c.nReduce, m.Paths, false}
			m.Mu.Unlock()
			info("Schedule: Got %s", t)
			// Pick a worker
			w := c.GetWorker()
			w.Mu.Lock()
			info("Schedule: Got %s", w)
			w.Mu.Unlock()
			w.AssignNewTask(t)

			// Update task status
			m.AssignTo(w)
		}
		// Wait until all map tasks done
		debug("Schedule: Waiting for map tasks done")
		time.Sleep(WAIT_DURATION)
	}
	info("Schedule: Start schedule reduce tasks")
	// Start to schedule reduce tasks
	for c.reduceLeft.Load() > 0 {
		// Pick a reduce task
		for rid := range c.reduces {
			r := &c.reduces[rid]
			r.Mu.Lock()
			if r.State != IDLE {
				r.Mu.Unlock()
				continue
			}
			t := &Task{REDUCE, rid, c.nReduce, r.Paths, false}
			r.Mu.Unlock()
			info("Schedule: Got %s", t)

			// Pick a worker
			w := c.GetWorker()
			w.Mu.Lock()
			info("Schedule: Got %s", w)
			w.Mu.Unlock()
			w.AssignNewTask(t)

			// Update task status
			r.AssignTo(w)
		}

		// Wait until all reduce tasks done
		debug("Schedule: Waiting for reduce tasks done")
		time.Sleep(WAIT_DURATION)
	}

	// Boardcast EXIT task
	for _, w := range c.Workers {
		w.AssignNewTask(&TASK_EXIT)
	}
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *Task) error {
	info("TaskDone: Receive %v", args)
	// Validate worker id
	if args.WorkerID < 0 || args.WorkerID >= len(c.Workers) {
		return fmt.Errorf("invalid worker id: %d", args.WorkerID)
	}
	w := c.Workers[args.WorkerID]
	if args.Type == MAP {
		// Validate map task id
		if args.TaskID < 0 || args.TaskID >= len(c.maps) {
			return fmt.Errorf("invalid map task id: %d", args.TaskID)
		}
		// Mark map task as done; discard duplicated done task
		if c.maps[args.TaskID].DoneBy(w) {
			c.mapLeft.Add(-1)
			for _, o := range args.Output {
				var mi, ri int
				fmt.Sscanf(o, "mr-%d-%d", &mi, &ri)
				r := &c.reduces[ri]
				r.Mu.Lock()
				r.Paths = append(r.Paths, o)
				r.State = IDLE
				r.Mu.Unlock()
			}
		}
	} else {
		// Validate reduce task id
		if args.TaskID < 0 || args.TaskID >= len(c.reduces) {
			return fmt.Errorf("invalid reduce task id: %d", args.TaskID)
		}
		// Mark reduce task as done; discard duplicated done task
		if c.reduces[args.TaskID].DoneBy(w) {
			c.reduceLeft.Add(-1)
		}
	}
	if w.HasNewTask(reply) {
		w.FreshState(IN_PROGRESS)
	} else {
		w.FreshState(IDLE)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.Schedule()
	// TODO dead worker detector
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	ma, ra := int32(len(c.maps)), int32(c.nReduce)
	ml, rl := c.mapLeft.Load(), c.reduceLeft.Load()
	done := (ml == 0) && (rl == 0)
	if done {
		info("ALL task done.")
	} else {
		info("Map task %d/%d Reduce task %d/%d", ml, ma, rl, ra)
	}
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.maps = make([]Input, len(files))
	c.reduces = make([]Input, nReduce)
	c.reduceLeft.Store(int32(nReduce))
	for i, f := range files {
		c.maps[i].Paths = []string{f}
		c.maps[i].State = IDLE
	}
	c.mapLeft.Store(int32(len(c.maps)))
	c.server()
	return &c
}
