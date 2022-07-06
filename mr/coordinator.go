package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type mapTask struct {
	done     bool
	worker   int
	filename string
}

type reduceTask struct {
	done   bool
	worker int
}

type Coordinator struct {
	mapTasks    []mapTask
	reduceTasks []reduceTask

	availableMapTasks    map[int]int
	availableReduceTasks map[int]int

	mapDoneTasks    int
	reduceDoneTasks int

	mapLock    sync.Mutex
	reduceLock sync.Mutex
}

func waitCheck(c *Coordinator, taskType int, taskNum int) {
	time.Sleep(10 * time.Second)

	if taskType == 0 {
		c.mapLock.Lock()
		if !c.mapTasks[taskNum].done {
			defer fmt.Printf("Worker %v took too long to respond for map task %v\n",
				c.mapTasks[taskNum].worker, taskNum)
			c.mapTasks[taskNum].worker = -1
			c.availableMapTasks[taskNum] = taskNum
		}
		c.mapLock.Unlock()
	} else {
		c.reduceLock.Lock()
		if !c.reduceTasks[taskNum].done {
			defer fmt.Printf("Worker %v took too long to respond for reduce task %v\n",
				c.reduceTasks[taskNum].worker, taskNum)
			c.reduceTasks[taskNum].worker = -1
			c.availableReduceTasks[taskNum] = taskNum
		}
		c.reduceLock.Unlock()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *GetArgs, reply *GetReply) error {
	fmt.Printf("Worker %v requesting for task\n", args.WorkerId)
	// If map task available, send map task

	c.mapLock.Lock()
	for id, _ := range c.availableMapTasks {
		fmt.Printf("Map Task %v given to worker %v\n", id, args.WorkerId)

		// Populate reply
		reply.TaskType = 0
		reply.TaskNum = id
		reply.Filename = c.mapTasks[id].filename
		reply.Partitions = len(c.reduceTasks)

		// Fill in maptask details
		c.mapTasks[id].worker = args.WorkerId

		// Remove from available
		delete(c.availableMapTasks, id)

		// Run waiting thread
		go waitCheck(c, 0, id)

		c.mapLock.Unlock()

		return nil
	}
	c.mapLock.Unlock()

	// All map tasks not finished yet
	if c.mapDoneTasks != len(c.mapTasks) {
		fmt.Printf("No tasks available for worker %v\n", args.WorkerId)
		reply.TaskType = 2
		return nil
	}

	c.reduceLock.Lock()
	// If all map tasks over and reduce task available send reduce task
	for id, _ := range c.availableReduceTasks {
		fmt.Printf("Reduce Task %v given to worker %v\n", id, args.WorkerId)

		// Populate reply
		reply.TaskType = 1
		reply.TaskNum = id
		reply.Partitions = len(c.mapTasks)

		// Fill in reduce details
		c.reduceTasks[id].worker = args.WorkerId

		// Remove from available
		delete(c.availableReduceTasks, id)

		// Run waiting thread
		go waitCheck(c, 1, id)

		c.reduceLock.Unlock()

		return nil
	}
	c.reduceLock.Unlock()

	if c.reduceDoneTasks != len(c.reduceTasks) {
		// No task available right now
		fmt.Printf("No tasks available for worker %v\n", args.WorkerId)
		reply.TaskType = 2
		return nil
	} else {
		// No task available right now
		fmt.Printf("All tasks completed, quiting worker %v\n", args.WorkerId)
		reply.TaskType = 3
		return nil
	}
}

func (c *Coordinator) FinishTask(args *FinishArgs, reply *FinishReply) error {
	if args.TaskType == 0 {
		// Map task finished
		c.mapLock.Lock()
		if c.mapTasks[args.TaskNum].worker == args.WorkerId {
			fmt.Printf("Worker %v finished map task %v\n",
				args.WorkerId, args.TaskNum)
			c.mapTasks[args.TaskNum].done = true
			c.mapTasks[args.TaskNum].worker = -1
			c.mapDoneTasks++
		} else {
			fmt.Printf("Worker %v finished map task %v too late\n",
				args.WorkerId, args.TaskNum)
		}
		c.mapLock.Unlock()
		return nil
	} else {
		// Reduce task finished
		c.reduceLock.Lock()
		if c.reduceTasks[args.TaskNum].worker == args.WorkerId {
			fmt.Printf("Worker %v finished reduce task %v\n",
				args.WorkerId, args.TaskNum)
			c.reduceTasks[args.TaskNum].done = true
			c.reduceTasks[args.TaskNum].worker = -1
			c.reduceDoneTasks++
		} else {
			fmt.Printf("Worker %v finished reduce task %v too late\n",
				args.WorkerId, args.TaskNum)
		}
		c.reduceLock.Unlock()
		return nil
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mapLock.Lock()
	c.reduceLock.Lock()
	ret := c.mapDoneTasks == len(c.mapTasks) &&
		c.reduceDoneTasks == len(c.reduceTasks)
	c.reduceLock.Unlock()
	c.mapLock.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Fill map tasks
	c.mapTasks = make([]mapTask, len(files))
	c.availableMapTasks = make(map[int]int)
	c.mapDoneTasks = 0

	for i, _ := range c.mapTasks {
		c.mapTasks[i] = mapTask{false, -1, files[i]}
		c.availableMapTasks[i] = i
	}

	// Fill reduce tasks
	c.reduceTasks = make([]reduceTask, nReduce)
	c.availableReduceTasks = make(map[int]int)
	c.reduceDoneTasks = 0

	for i, _ := range c.reduceTasks {
		c.reduceTasks[i] = reduceTask{false, -1}
		c.availableReduceTasks[i] = i
	}

	c.server()
	return &c
}
