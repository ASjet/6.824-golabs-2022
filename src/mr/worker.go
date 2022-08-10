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
	"time"
)

var nReduce int

var workerID = os.Getpid()

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// Get filename
	for t, e := acquireTask(); e == nil; {
		switch t.Type {
		case MAP:
			fmt.Printf("Got map task %v\n", t.Path)
			intermediate := make([][]KeyValue, nReduce)
			outputFiles := make([]string, nReduce)

			file, err := os.Open(t.Path)
			if err != nil {
				log.Printf("cannot open %v: %v", t.Path, err)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Printf("cannot read %v: %v", t.Path, err)
			}
			file.Close()

			kva := mapf(t.Path, string(content))
			sort.Sort(ByKey(kva))
			for _, kv := range kva {
				i := ihash(kv.Key) % nReduce
				intermediate[i] = append(intermediate[i], kv)
			}
			for i, kva := range intermediate {
				filename := fmt.Sprintf("mr-%d-%d", t.ID, i)
				file, err := os.Create(filename)
				if err != nil {
					log.Printf("cannot open %v: %v", file, err)
				}
				enc := json.NewEncoder(file)
				for _, kv := range kva {
					enc.Encode(kv)
				}
				outputFiles[i] = filename
			}
			t, e = done(t, outputFiles)
		case REDUCE:
			fmt.Printf("Got reduce task %v\n", t.Path)
		}
		time.Sleep(time.Second)
	}
}

func acquireTask() (Task, error) {
	args := AcquireArgs{workerID}
	reply := AcquireReply{}

	err := call("Coordinator.AcquireTask", &args, &reply)
	if err == nil {
		nReduce = reply.NReduce
		return reply.Task, nil
	} else {
		fmt.Printf("call failed!\n")
		return Task{}, err
	}
}

func done(t Task, output []string) (Task, error) {
	args := TaskDoneArgs{workerID, t.Type, t.ID, output}
	reply := AcquireReply{}
	err := call("Coordinator.TaskDone", &args, &reply)
	if err == nil {
		return reply.Task, nil
	} else {
		fmt.Printf("call failed!\n")
		return Task{}, err
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return nil
	}

	fmt.Println(err)
	return err
}
