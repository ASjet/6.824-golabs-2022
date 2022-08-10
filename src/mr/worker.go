package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	for t, e := acquireTask(); e == nil; t, e = done(t) {
		switch t.Type {
		case MAP:
			fmt.Printf("Got map task %v\n", t.Path)
		case REDUCE:
			fmt.Printf("Got reduce task %v\n", t.Path)
		}
		time.Sleep(time.Second)
	}
}

func acquireTask() (Task, error) {
	args := AcquireArgs{os.Getpid()}
	reply := AcquireReply{}

	err := call("Coordinator.AcquireTask", &args, &reply)
	if err == nil {
		return reply.Task, nil
	} else {
		fmt.Printf("call failed!\n")
		return Task{}, err
	}
}

func done(t Task) (Task, error) {
	args := TaskDoneArgs{os.Getpid(), t.Type, t.ID, t.Path}
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
