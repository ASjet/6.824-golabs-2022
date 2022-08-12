package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerInfo struct {
	// Do not need to hold lock here
	Sock string
	ID   int

	// Need to hold lock when r/w
	state  int
	task   *WorkerTask
	update bool

	mu sync.Mutex
}

// Registry this worker to coordinator
func (w *WorkerInfo) Registry() {
	args := RegistryArgs{w.Sock}
	reply := RegistryReply{}
	err := call("Coordinator.RegistryWorker", &args, &reply)
	if err != nil {
		log.Fatalf("[%s]register: %v", w.Sock, err)
	}
	w.ID = reply.WorkerID
	w.task = TASK_WAIT
}

// Run RPC server
func (w *WorkerInfo) server() {
	prefix := "worker_"
	w.Sock = prefix + strconv.Itoa(os.Getpid())
	rpc.Register(w)
	rpc.HandleHTTP()
	os.Remove(w.Sock)
	l, e := net.Listen("unix", w.Sock)
	if e != nil {
		log.Fatalf("[%s]listen error: %v", w.Sock, e)
	}
	go http.Serve(l, nil)
	w.Registry()
}

// This is a RPC call
// Coordinator ping worker to get health status
func (w *WorkerInfo) Ping(args *PingArgs, reply *PingReply) error {
	w.mu.Lock()
	reply.State = w.state
	reply.TaskType = w.task.Type
	reply.TaskID = w.task.ID
	w.mu.Unlock()
	return nil
}

// This is a RPC call
// Coordinator assign new task to worker
func (w *WorkerInfo) AssignNewTask(args *WorkerTask, reply *PingReply) error {
	debug("[%s]AssignNewTask: received %s", w.Sock, args)
	if args.Type == EXIT {
		info("[%s]AssignNewTask: received EXIT", w.Sock)
		os.Exit(0)
	}
	w.mu.Lock()
	w.task = args
	w.update = true
	w.state = args.State()
	w.mu.Unlock()
	reply.State = args.State()
	return nil
}

func (w *WorkerInfo) Updated() bool {
	defer w.mu.Unlock()
	w.mu.Lock()
	return w.update
}

func (w *WorkerInfo) ReadTask() *WorkerTask {
	defer w.mu.Unlock()
	w.mu.Lock()
	if w.update {
		task := w.task
		w.update = false
		return task
	}
	return TASK_WAIT
}

func (w *WorkerInfo) Done(t *WorkerTask, output []string) {
	// CANNOT Set worker to IDLE here, or will intruduce race that cannot be detected

	info("[%s]Done: %v", w.Sock, t)
	args := TaskDoneArgs{w.ID, t.Type, t.ID, output}
	reply := TaskDoneReply{}
	err := call("Coordinator.TaskDone", &args, &reply)
	if err != nil {
		log.Printf("[%s]RPC: TaskDone err: %v", w.Sock, err)
		w.Registry()
		w.Done(t, output)
	}
	w.mu.Lock()
	w.state = reply.SyncState
	debug("[%s]Done: Sync state to %s", w.Sock, statename[w.state])
	w.mu.Unlock()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your workera implementation here.
	worker := WorkerInfo{}
	// Start RPC server and register to coordinator
	worker.server()

	// Main loop
LoopStart:
	for {
		t := worker.ReadTask()
		switch t.Type {
		case MAP:
			info("[%s]Got %s", worker.Sock, t)
			intermediate := make([][]KeyValue, t.NReduce)
			outputFiles := make([]string, t.NReduce)

			for _, path := range t.Paths {
				ifile, err := os.Open(path)
				if err != nil {
					log.Printf("[%s]cannot open %v: %v", worker.Sock, path, err)
				}
				content, err := io.ReadAll(ifile)
				if err != nil {
					log.Printf("[%s]cannot read %v: %v", worker.Sock, path, err)
				}
				ifile.Close()

				if worker.Updated() {
					break LoopStart
				}

				// Call mapper
				kva := mapf(path, string(content))
				sort.Sort(ByKey(kva))

				for _, kv := range kva {
					if worker.Updated() {
						break LoopStart
					}
					// hash partition
					i := ihash(kv.Key) % t.NReduce
					intermediate[i] = append(intermediate[i], kv)
				}

				for i, kva := range intermediate {
					tmpname := fmt.Sprintf("tmp-intermediate-%d-%d", worker.ID, t.ID)
					ofile, err := os.Create(tmpname)
					if err != nil {
						log.Printf("[%s]cannot open %v: %v", worker.Sock, ofile, err)
					}
					enc := json.NewEncoder(ofile)
					for _, kv := range kva {
						enc.Encode(kv)
					}
					if worker.Updated() {
						ofile.Close()
						os.Remove(tmpname)
						break LoopStart
					}
					ofile.Close()
					oname := fmt.Sprintf("mr-%d-%d", t.ID, i)
					os.Rename(tmpname, oname)
					outputFiles[i] = oname
				}
				if worker.Updated() {
					break LoopStart
				}
			}
			worker.Done(t, outputFiles)
		case REDUCE:
			info("[%s]Got %v", worker.Sock, t)
			intermeidate := []KeyValue{}
			// Read all intermediate kv pairs
			for _, path := range t.Paths {
				ifile, err := os.Open(path)
				if err != nil {
					log.Printf("[%s]cannot open %v: %v", worker.Sock, path, err)
				}
				dec := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermeidate = append(intermeidate, kv)
				}
				ifile.Close()
				if worker.Updated() {
					break LoopStart
				}
			}
			sort.Sort(ByKey(intermeidate))
			tmpname := fmt.Sprintf("tmp-out-%d-%d", worker.ID, t.ID)
			ofile, _ := os.Create(tmpname)
			for i := 0; i < len(intermeidate); {
				j := i + 1
				for j < len(intermeidate) && intermeidate[j].Key == intermeidate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermeidate[k].Value)
				}
				output := reducef(intermeidate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermeidate[i].Key, output)

				i = j
				if worker.Updated() {
					ofile.Close()
					os.Remove(tmpname)
					break LoopStart
				}
			}
			ofile.Close()
			oname := fmt.Sprintf("mr-out-%d", t.ID)
			os.Rename(tmpname, oname)
			worker.Done(t, []string{oname})
		case EXIT:
			info("[%s]Received EXIT", worker.Sock)
			os.Exit(0)
		default:
			time.Sleep(WAIT_DURATION)
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("dialing: %v", err)
	}
	defer c.Close()
	return c.Call(rpcname, args, reply)
}
