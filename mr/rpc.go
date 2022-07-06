package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type GetArgs struct {
	WorkerId int
}

type GetReply struct {
	// 0 - map, 1 - reduce, 2 - not rn, 3 - done
	TaskType int

	// map - task num of map task, reduce - task num of reduce task
	TaskNum int

	// map - name of file for task
	Filename string

	// map - number of partitions
	Partitions int
}

type FinishArgs struct {
	// 0 - map, 1 - reduce
	TaskType int

	WorkerId int
	TaskNum  int
}

type FinishReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
