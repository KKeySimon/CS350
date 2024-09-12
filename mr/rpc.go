package mr

// Submission
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type EmptyArs struct {
}

type EmptyReply struct {
}

type TypeTask struct {
	Task string
}

// Universal Task structure
type MapTask struct {
	Filename  string // Filename = key
	NumReduce int    // Number of reduce tasks, used to figure out number of buckets
	Index     int
}

type TaskCompleted struct {
	Completed bool
}

type ReduceTask struct {
	Index      int
	NumReduce  int
	FileLength int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
