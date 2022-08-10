package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	IDLE int = iota
	IN_PROGRESS
	COMPLETED
	MAP
	REDUCE
)

type Piece struct {
	Path   string
	State  int
	Worker int
}

type Task struct {
	Type int
	ID   int
	Path string
}

type Coordinator struct {
	// Your definitions here.
	nReduce    int
	mapDone    int
	reduceDone int
	maps       []Piece
	reduces    []Piece
}

func (c *Coordinator) assignTask(worker int, reply *AcquireReply) error {
	reply.NReduce = c.nReduce
	for i := range c.maps {
		m := &c.maps[i]
		if m.State == IDLE {
			m.State = IN_PROGRESS
			m.Worker = worker
			reply.Task = Task{MAP, i, m.Path}
			return nil
		}
	}
	for i := range c.reduces {
		r := &c.reduces[i]
		if r.State == IDLE {
			r.State = IN_PROGRESS
			r.Worker = worker
			reply.Task = Task{REDUCE, i, r.Path}
			return nil
		}
	}
	return errors.New("no task left")
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AcquireTask(args *AcquireArgs, reply *AcquireReply) error {
	err := c.assignTask(args.WorkerID, reply)
	return err
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *AcquireReply) error {
	var t *Piece
	if args.Type == MAP {
		t = &c.maps[args.TaskID]
		c.mapDone++
		// c.reduces = append(c.reduces, Piece{args.Output, IDLE, -1})
	} else {
		t = &c.reduces[args.TaskID]
		c.reduceDone++
	}
	t.State = COMPLETED
	t.Worker = args.WorkerID
	err := c.assignTask(args.WorkerID, reply)
	return err
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	ma, ra := len(c.maps), len(c.reduces)
	ml, rl := ma-c.mapDone, ra-c.reduceDone
	// Your code here.
	done := (ml == 0) && (rl == 0)
	if done {
		log.Printf("ALL task done.\n")
	} else {
		log.Printf("Map task %d/%d Reduce task %d/%d\n", ml, ma, rl, ra)
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
	c.maps = make([]Piece, 0, len(files))
	c.reduces = make([]Piece, 0, nReduce)
	for _, f := range files {
		c.maps = append(c.maps, Piece{f, IDLE, -1})
	}

	c.server()
	return &c
}
