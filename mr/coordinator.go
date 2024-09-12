package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce            int          // Number of reduce tasks
	Files                []string     // Files for map tasks, len(Files) is number of Map tasks
	MapTasks             chan MapTask // Channel for uncompleted map tasks
	ReduceTasks          chan ReduceTask
	CompletedTasks       map[string]bool // Map to check if task is completed
	Lock                 sync.Mutex      // Lock for contolling shared variables
	MapCompleted         bool
	IntermediateFiles    []string
	Index                int
	CompletedReduceTasks map[string]bool
	ReduceCompleted      bool
}

// Starting coordinator logic
func (c *Coordinator) Start() {
	// fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	// Prepare initial MapTasks and add them to the queue
	for _, file := range c.Files {
		mapTask := MapTask{
			Filename:  file,
			NumReduce: c.NumReduce,
			Index:     c.Index,
		}

		c.Index += 1

		// fmt.Println("MapTask", mapTask, "added to channel")

		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false
	}

	c.server()
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestMapTask(args *EmptyArs, reply *MapTask) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	select {
	case task, _ := <-c.MapTasks: // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

		// fmt.Println("Map task found,", task.Filename)

		*reply = task

		go c.WaitForWorker(task)

		return nil
	default:
		return nil
	}
}

func (c *Coordinator) RequestReduceTask(args *EmptyArs, reply *ReduceTask) error {
	// fmt.Println("Reduce task requested")
	c.Lock.Lock()
	defer c.Lock.Unlock()
	select {
	case task, _ := <-c.ReduceTasks: // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

		// fmt.Println("Reduce task found,", task)

		*reply = task

		go c.WaitForReduceWorker(task)

		return nil
	default:
		reply.Index = -1
		return nil
	}
}

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		// fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForReduceWorker(task ReduceTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedReduceTasks["reduce_"+strconv.Itoa(task.Index)] == false {
		// fmt.Println("Timer expired, task", task.Index, "is not finished. Putting back in queue.")
		c.ReduceTasks <- task
	}
	c.Lock.Unlock()
}

func (c *Coordinator) StartReduce() error {
	for i := 0; i < c.NumReduce; i++ {
		reduceTask := ReduceTask{
			Index:      i,
			NumReduce:  c.NumReduce,
			FileLength: len(c.Files),
		}
		// fmt.Println("ReduceTask", reduceTask, "added to channel")
		c.ReduceTasks <- reduceTask
		c.CompletedReduceTasks["reduce_"+strconv.Itoa(reduceTask.Index)] = false
	}
	return nil
}

// RPC for reporting a completion of a task
func (c *Coordinator) TaskCompleted(args *MapTask, reply *TaskCompleted) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["map_"+args.Filename] = true

	// fmt.Println("Task", args, "completed")

	// If all of map tasks are completed, go to reduce phase
	// ...
	completed := true
	for _, i := range c.CompletedTasks {
		if i == false {
			completed = false
		}
	}

	if completed {
		c.MapCompleted = true
		// fmt.Println("all maps completed!")
		reply.Completed = true
		c.StartReduce()
	}

	return nil
}

// RPC for reporting a completion of a task
func (c *Coordinator) TaskReduceCompleted(args *ReduceTask, reply *TaskCompleted) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedReduceTasks["reduce_"+strconv.Itoa(args.Index)] = true

	// fmt.Println("Task", args, "completed")

	// If all of map tasks are completed, go to reduce phase
	// ...
	completed := true
	for _, i := range c.CompletedReduceTasks {
		if i == false {
			completed = false
		}
	}

	if completed {
		c.ReduceCompleted = true
		// fmt.Println("all reduces completed!")

		reply.Completed = true
	}

	return nil
}

// RPC for reporting a completion of a task
func (c *Coordinator) AssignTask(args *EmptyArs, reply *TypeTask) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if c.MapCompleted == true {
		reply.Task = "reduce"
	} else {
		reply.Task = "map"
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	ret := false

	if c.ReduceCompleted {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:            nReduce,
		Files:                files,
		MapTasks:             make(chan MapTask, 100),
		ReduceTasks:          make(chan ReduceTask, 100),
		CompletedTasks:       make(map[string]bool),
		MapCompleted:         false,
		IntermediateFiles:    make([]string, nReduce),
		Index:                0,
		CompletedReduceTasks: make(map[string]bool),
		ReduceCompleted:      false,
	}

	// fmt.Println("Starting coordinator")

	c.Start()

	return &c
}
