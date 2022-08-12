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
	FAIL

	WAIT
	MAP
	REDUCE
	EXIT

	DEBUG = false
	INFO  = false

	WAIT_DURATION = time.Millisecond * 1000
	PING_INTERVAL = time.Second
)

var statename = []string{
	IDLE:        "IDLE",
	IN_PROGRESS: "IN_PROGRESS",
	COMPLETED:   "COMPLETED",
	FAIL:        "FAIL",
	EXIT:        "EXIT",
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

// Worker task
type WorkerTask struct {
	Type    int
	ID      int
	NReduce int
	Paths   []string
}

func (t WorkerTask) String() string {
	return fmt.Sprintf("Task{%s: %d-%v}", taskname[t.Type], t.ID, t.Paths)
}

func (t *WorkerTask) CopyTo(dst *WorkerTask) {
	dst.ID = t.ID
	dst.Type = t.Type
	dst.NReduce = t.NReduce
	tmp := make([]string, len(t.Paths))
	copy(tmp, t.Paths)
	dst.Paths = tmp
}

func (t WorkerTask) State() int {
	switch t.Type {
	case MAP:
		fallthrough
	case REDUCE:
		return IN_PROGRESS
	case WAIT:
		return IDLE
	case EXIT:
		return EXIT
	}
	return FAIL
}

var TASK_EXIT = &WorkerTask{EXIT, -1, 0, nil}
var TASK_WAIT = &WorkerTask{WAIT, -1, 0, nil}

// Worker record
type WorkerRecord struct {
	// Only init in construction, read don't need to hold lock
	Sock string

	// rw need hold the lock
	Client *rpc.Client
	State  int
	task   *WorkerTask
	input  *Input
	Mu     sync.Mutex
}

func (w *WorkerRecord) String() string {
	return fmt.Sprintf("Worker{%s: %s, input: %p}", statename[w.State], w.task, w.input)
}

// Make RPC call, must hold lock
func (w *WorkerRecord) dial() error {
	c, err := rpc.DialHTTP("unix", w.Sock)
	if err != nil {
		info("dial: dialing to %s: %v", w, err)
		w.State = FAIL
		i := w.input
		w.input = nil

		// Reset task state to IDLE if it's worker is fail
		if i != nil {
			i.Reset()
			info("dial: reset %s to IDLE", w.task)
		}
		return err
	}
	debug("Dial: dial up to %s", w.Sock)
	old := w.Client
	w.Client = c
	if old != nil {
		old.Close()
	}
	return nil
}

// Make RPC call, must hold lock
func (w *WorkerRecord) call(rpcName string, args, reply interface{}) bool {
	err := w.Client.Call(rpcName, args, reply)
	if w.State == EXIT {
		return true
	}
	if err != nil {
		info("call: calling to %s@%s: %v", w.Sock, rpcName, err)
		// If w.Dial() failed will set w.State to DOW and return error
		err := w.dial()
		if err != nil {
			return false
		} else {
			err = w.Client.Call(rpcName, args, reply)
			return err == nil
		}
	}
	return true
}

// Ping worker to get health status
func (w *WorkerRecord) Ping() {
	defer w.Mu.Unlock()
	w.Mu.Lock()
	args := PingArgs{}
	reply := PingReply{}
	// Need hold lock until RPC return
	// Or a ping before this call return will clear the worker's state
	success := w.call("WorkerInfo.Ping", &args, &reply)

	if success {
		w.State = reply.State
	}
	debug("Ping: %s %s on %s", w.Sock, statename[w.State], w.task)
}

func (w *WorkerRecord) IsFault() bool {
	defer w.Mu.Unlock()
	w.Mu.Lock()
	return w.State == FAIL
}

func (w *WorkerRecord) AssignNewTask(t *WorkerTask, i *Input) bool {
	// Set to task's state first to avoid repeat assigning MAP/REDUCE task
	defer w.Mu.Unlock()
	w.Mu.Lock()
	w.State = t.State()
	w.task = t
	w.input = i

	reply := PingReply{}
	// Need hold lock until RPC return
	// Or a ping before this call return will clear the worker's state
	if w.call("WorkerInfo.AssignNewTask", t, &reply) {
		// Set to actually worker's state
		w.State = reply.State
		debug("AssignNewTask: now the worker is %s", w)
		return true
	}
	return false
}

type Input struct {
	Paths  []string
	State  int
	Worker *WorkerRecord
	Start  time.Time
	Dura   time.Duration
	Mu     sync.Mutex
}

// Reset input to init state
func (i *Input) Reset() {
	i.Mu.Lock()
	i.State = IDLE
	i.Worker = nil
	i.Mu.Unlock()
}

// Assign input to worker
func (i *Input) AssignTo(w *WorkerRecord) {
	i.Mu.Lock()
	i.State = IN_PROGRESS
	i.Worker = w
	i.Start = time.Now()
	i.Mu.Unlock()
}

// This is done by a worker
func (i *Input) DoneBy(w *WorkerRecord) bool {
	defer i.Mu.Unlock()
	i.Mu.Lock()
	// Discard repeated done
	if i.State != COMPLETED {
		i.State = COMPLETED
		i.Worker = w
		i.Dura = time.Since(i.Start)
		return true
	}
	return false
}

// How long have the input been processing
func (i *Input) Last() time.Duration {
	defer i.Mu.Unlock()
	i.Mu.Lock()
	return time.Since(i.Start)
}

// Coordinator data struct
type Coordinator struct {
	// Your definitions here.
	workers  []*WorkerRecord
	workerMu sync.Mutex

	maps    []Input
	mapLeft atomic.Int32

	nReduce    int
	reduceLeft atomic.Int32
	reduces    []Input
}

// Your code here -- RPC handlers for the worker to call.

// This is a RPC call
// Register worker to coordinator
func (c *Coordinator) RegistryWorker(args *RegistryArgs, reply *RegistryReply) error {
	debug("RegistryWorker: New worker registry on sock %s", args.Sock)

	// Init worker and dial up
	w := WorkerRecord{Sock: args.Sock, State: IDLE, task: nil}
	defer w.Mu.Unlock()
	w.Mu.Lock()
	if err := w.dial(); err != nil {
		return err
	}

	// Assign Worker ID
	c.workerMu.Lock()
	id := len(c.workers)
	c.workers = append(c.workers, &w)
	c.workerMu.Unlock()

	reply.WorkerID, reply.Sockname = id, w.Sock
	info("RegistryWorker: assign worker id %d to %s", id, w.Sock)
	return nil
}

// This is a RPC call
// Worker report task output to coordinator
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	// Validate worker id
	c.workerMu.Lock()
	nWorkers := len(c.workers)
	c.workerMu.Unlock()
	if args.WorkerID < 0 || args.WorkerID >= nWorkers {
		return fmt.Errorf("invalid worker id: %d", args.WorkerID)
	}

	w := c.workers[args.WorkerID]
	w.Mu.Lock()
	w.State = IDLE
	i := w.input
	t := w.task
	w.input = nil
	w.task = nil
	reply.SyncState = w.State
	w.Mu.Unlock()

	if i.DoneBy(w) {
		left := 0
		switch args.Type {
		case MAP:
			c.mapLeft.Add(-1)
			left = int(c.mapLeft.Load())
			// Add output to reduce inputs
			for _, o := range args.Output {
				var mi, ri int
				fmt.Sscanf(o, "mr-%d-%d", &mi, &ri)
				r := &c.reduces[ri]
				r.Mu.Lock()
				r.Paths = append(r.Paths, o)
				r.State = IDLE
				r.Mu.Unlock()
			}
		case REDUCE:
			c.reduceLeft.Add(-1)
			left = int(c.reduceLeft.Load())
		}
		info("TaskDone: %s done in %s, %d task(s) left", t, i.Dura, left)
	}
	return nil
}

// Get an IDLE worker from worker list
func (c *Coordinator) GetWorker() *WorkerRecord {
	for {
		c.workerMu.Lock()
		for _, w := range c.workers {
			if w.Mu.TryLock() {
				if w.State == IDLE {
					w.Mu.Unlock()
					c.workerMu.Unlock()
					return w
				}
				w.Mu.Unlock()
			}
		}
		c.workerMu.Unlock()
		// Wait until worker avaliable
		debug("GetWorker: Waiting for avaliable worker")
		time.Sleep(WAIT_DURATION)
	}
}

// Schedule tasks to workers
func (c *Coordinator) Schedule() {
	// Do not schedule reduce task until all map tasks done
	info("Schedule: Start schedule map tasks")
	for c.mapLeft.Load() > 0 {
		for mid := range c.maps {
			m := &c.maps[mid]
			m.Mu.Lock()
			if m.State != IDLE {
				m.Mu.Unlock()
				continue
			}

			// Pick a map task
			t := &WorkerTask{MAP, mid, c.nReduce, m.Paths}
			m.Mu.Unlock()
			info("Schedule: Got %s", t)

			for {
				// Pick a worker
				w := c.GetWorker()
				w.Mu.Lock()
				info("Schedule: Got %s", w)
				w.Mu.Unlock()

				if w.AssignNewTask(t, m) {
					// Update task status
					m.AssignTo(w)
					break
				}
				// If assign failed, choose an new worker
			}
		}
		// Wait until all map tasks done
		debug("Schedule: Waiting for map tasks done")
		time.Sleep(WAIT_DURATION)
	}
	info("Schedule: Start schedule reduce tasks")
	// Start to schedule reduce tasks
	for c.reduceLeft.Load() > 0 {
		for rid := range c.reduces {
			r := &c.reduces[rid]
			r.Mu.Lock()
			if r.State != IDLE {
				r.Mu.Unlock()
				continue
			}

			// Pick a reduce task
			t := &WorkerTask{REDUCE, rid, c.nReduce, r.Paths}
			r.Mu.Unlock()
			info("Schedule: Got %s", t)

			for {
				// Pick a worker
				w := c.GetWorker()
				w.Mu.Lock()
				info("Schedule: Got %s", w)
				w.Mu.Unlock()

				if w.AssignNewTask(t, r) {
					// Update task status
					r.AssignTo(w)
					break
				}
				// If assign failed, choose an new worker
			}
		}

		// Wait until all reduce tasks done
		debug("Schedule: Waiting for reduce tasks done")
		time.Sleep(WAIT_DURATION)
	}

	// Boardcast EXIT task
	for _, w := range c.workers {
		w.AssignNewTask(TASK_EXIT, nil)
	}
}

// Periodically ping all workers for health status
func (c *Coordinator) pinger(pingDuration time.Duration) {
	for {
		var wg sync.WaitGroup
		c.workerMu.Lock()
		for _, w := range c.workers {
			wg.Add(1)
			go func(w *WorkerRecord) {
				defer wg.Done()
				if w.IsFault() {
					return
				}
				w.Ping()
			}(w)
		}
		c.workerMu.Unlock()
		wg.Wait()

		// Wait for next check iteration
		time.Sleep(pingDuration)
	}
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
	go c.pinger(PING_INTERVAL)
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
	for i, f := range files {
		c.maps[i].Paths = []string{f}
		c.maps[i].State = IDLE
	}
	c.reduceLeft.Store(int32(len(c.reduces)))
	c.mapLeft.Store(int32(len(c.maps)))
	c.server()
	return &c
}
