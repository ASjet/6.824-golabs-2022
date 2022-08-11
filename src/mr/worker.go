package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

var WorkerTask Task = TASK_WAIT
var WorkerTaskMu sync.Mutex

type Info struct {
	ID    int
	Sock  string
	State int
	mu    sync.Mutex
}

func (i *Info) SetState(state int) {
	i.mu.Lock()
	i.State = state
	i.mu.Unlock()
}

var WorkerInfo Info

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

func registry() {
	WorkerInfo.ID = -1
	WorkerTaskMu.Lock()
	WorkerTask.Type = WAIT
	WorkerTaskMu.Unlock()
	reply := RegisterReply{}
	err := call("Coordinator.RegistryWorker", &WorkerInfo, &reply)
	if err != nil {
		log.Fatalf("[%d]register: %v", WorkerInfo.ID, err)
	}
	WorkerInfo.ID = reply.WorkerID
	WorkerInfo.SetState(IDLE)
	info("[%d]registry: Got worker id %d", WorkerInfo.ID, WorkerInfo.ID)
}

func ping() {
	for {
		t := Task{}
		WorkerInfo.mu.Lock()
		args := PingArgs{WorkerInfo.ID, WorkerInfo.State}
		WorkerInfo.mu.Unlock()
		err := call("Coordinator.Ping", &args, &t)
		if err != nil {
			log.Fatalf("[%d]ping: %v", WorkerInfo.ID, err)
		}
		if t.Type == EXIT {
			info("[%d]ping:Received EXIT", WorkerInfo.ID)
			os.Exit(0)
		}
		if !t.Read {
			WorkerTaskMu.Lock()
			WorkerTask = t
			debug("[%d]ping: New %s\n", WorkerInfo.ID, WorkerTask)
			WorkerInfo.SetState(WorkerTask.State())
			WorkerTaskMu.Unlock()
		}
		time.Sleep(WAIT_DURATION)
	}
}

func readTask(t *Task) {
	WorkerTaskMu.Lock()
	if !WorkerTask.Read {
		debug("[%d]readTask: raw %s", WorkerInfo.ID, WorkerTask)
		WorkerTask.CopyTo(t)
		WorkerTask.Read = true
	}
	WorkerTaskMu.Unlock()
}

func done(t *Task, output []string) {
	info("[%d]done: Task done: %v", WorkerInfo.ID, t)
	args := TaskDoneArgs{WorkerInfo.ID, t.Type, t.ID, output}
	reply := Task{}
	err := call("Coordinator.TaskDone", &args, &reply)
	WorkerTaskMu.Lock()
	WorkerTask = reply
	WorkerInfo.SetState(WorkerTask.State())
	WorkerTaskMu.Unlock()
	if err != nil {
		log.Printf("[%d]RPC: TaskDone err: %v", WorkerInfo.ID, err)
		registry()
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// Register to coordinator
	registry()
	// Start pinger
	go ping()

	// Main loop
LoopStart:
	for {
		t := Task{}
		readTask(&t)
		switch t.Type {
		case MAP:
			info("[%d]Got %s", WorkerInfo.ID, t)
			intermediate := make([][]KeyValue, t.NReduce)
			outputFiles := make([]string, t.NReduce)

			for _, path := range t.Paths {
				if !WorkerTask.Read {
					break LoopStart
				}
				file, err := os.Open(path)
				if err != nil {
					log.Printf("[%d]cannot open %v: %v", WorkerInfo.ID, path, err)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Printf("[%d]cannot read %v: %v", WorkerInfo.ID, path, err)
				}
				file.Close()

				// Call mapper
				kva := mapf(path, string(content))
				sort.Sort(ByKey(kva))

				for _, kv := range kva {
					if !WorkerTask.Read {
						break LoopStart
					}
					// hash partition
					i := ihash(kv.Key) % t.NReduce
					intermediate[i] = append(intermediate[i], kv)
				}

				for i, kva := range intermediate {
					if !WorkerTask.Read {
						break LoopStart
					}
					filename := fmt.Sprintf("mr-%d-%d", t.ID, i)
					file, err := os.Create(filename)
					if err != nil {
						log.Printf("[%d]cannot open %v: %v", WorkerInfo.ID, file, err)
					}
					enc := json.NewEncoder(file)
					for _, kv := range kva {
						if !WorkerTask.Read {
							break LoopStart
						}
						enc.Encode(kv)
					}
					outputFiles[i] = filename
				}
			}
			done(&t, outputFiles)
		case REDUCE:
			info("[%d]Got %v", WorkerInfo.ID, t)
			intermeidate := []KeyValue{}
			// Read all intermediate kv pairs
			for _, path := range t.Paths {
				if !WorkerTask.Read {
					break LoopStart
				}
				file, err := os.Open(path)
				if err != nil {
					log.Printf("[%d]cannot open %v: %v", WorkerInfo.ID, path, err)
				}
				dec := json.NewDecoder(file)
				for {
					if !WorkerTask.Read {
						break LoopStart
					}
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermeidate = append(intermeidate, kv)
				}
			}
			sort.Sort(ByKey(intermeidate))
			tmpname := fmt.Sprintf("tmp-out-%d-%d", WorkerInfo.ID, t.ID)
			ofile, _ := os.Create(tmpname)
			for i := 0; i < len(intermeidate); {
				if !WorkerTask.Read {
					break LoopStart
				}
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
			}
			ofile.Close()
			oname := fmt.Sprintf("mr-out-%d", t.ID)
			os.Rename(tmpname, oname)
			done(&t, []string{oname})
		case EXIT:
			info("[%d]Received EXIT", WorkerInfo.ID)
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
		log.Fatalf("[%d]dialing: %v", WorkerInfo.ID, err)
	}
	defer c.Close()
	return c.Call(rpcname, args, reply)
}
