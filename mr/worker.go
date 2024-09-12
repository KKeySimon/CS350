package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
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

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	Lock    sync.Mutex
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
	}
	w.AssignTask()
}

func (w *WorkerSt) AssignTask() {
	args := EmptyArs{}
	reply := TypeTask{
		Task: "",
	}
	call("Coordinator.AssignTask", &args, &reply)

	if reply.Task == "map" {
		// fmt.Printf("map requested")
		w.RequestMapTask()

	} else {
		// fmt.Printf("reduce requested")
		w.RequestReduceTask()
	}
}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestMapTask() {
	args := EmptyArs{}
	reply := MapTask{}
	call("Coordinator.RequestMapTask", &args, &reply)
	if reply.Filename == "" {
		w.AssignTask()
		return
	}

	file, err := os.Open(reply.Filename)
	if err != nil {
		os.Exit(1)
		//log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		os.Exit(1)
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	kva := w.mapf(reply.Filename, string(content))

	// store kva in multiple files according to rules described in the README
	// ...
	buckets := make([][]KeyValue, reply.NumReduce)
	for _, kv := range kva {
		buckets[ihash(kv.Key)%reply.NumReduce] = append(buckets[ihash(kv.Key)%reply.NumReduce], kv)
	}

	for i, b := range buckets {
		fname := "mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(i) + ".json"
		intermediate, _ := os.Create(fname)
		enc := json.NewEncoder(intermediate)
		for _, kv := range b {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	//fmt.Println("Map task for", reply.Filename, "completed")
	// fmt.Println(kva)

	completeReply := TaskCompleted{
		Completed: false,
	}
	call("Coordinator.TaskCompleted", &reply, &completeReply)
	if completeReply.Completed {
		w.AssignTask()
		return
	}
	w.AssignTask()
}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestReduceTask() {
	for {
		args := EmptyArs{}
		reply := ReduceTask{}
		call("Coordinator.RequestReduceTask", &args, &reply)
		if reply.Index == -1 {
			return
		}

		intermediate := []KeyValue{}
		for i := 0; i < reply.FileLength; i++ {
			filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Index) + ".json"
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			kva := []KeyValue{}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}

			file.Close()
			intermediate = append(intermediate, kva...)
		}

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + strconv.Itoa(reply.Index)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := w.reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		completeReply := TaskCompleted{
			Completed: false,
		}
		//fmt.Println("Reduce task for", reply.Index, "completed")
		call("Coordinator.TaskReduceCompleted", &reply, &completeReply)
		//fmt.Println(completeReply.Completed)
		if completeReply.Completed {
			return
		}

	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(1)
		//log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
