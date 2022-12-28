package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	tasks                  chan *Task
	NMapTasks              int
	NReduceTasks           int
	FinishedMapTaskBits    *FinishedBitsWithLock
	FinishedReduceTaskBits *FinishedBitsWithLock
}

type FinishedBitsWithLock struct {
	bits int
	sync.RWMutex
}

type Task struct {
	TaskType
	FileName   string
	TaskNumber int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ApplyForWork(args *MRArgs, reply *MRReply) error {
	select {
	case t := <-c.tasks:
		if t.TaskType == MapTask {
			// 任务已经被完成了
			c.FinishedMapTaskBits.RLock()
			defer c.FinishedMapTaskBits.RUnlock()

			if (c.FinishedMapTaskBits.bits & (1 << t.TaskNumber)) != 0 {
				reply.TaskType = InvalidTask
			} else { // 任务还未被完成
				reply.TaskType = MapTask
				reply.FileName = t.FileName
				reply.MapTaskNumber = t.TaskNumber
				reply.NReduceTasks = c.NReduceTasks

				go func() {
					time.Sleep(10 * time.Second)
					// 10s后还未完成认为其超时重新发送
					c.FinishedMapTaskBits.RLock()
					defer c.FinishedMapTaskBits.RUnlock()

					if (c.FinishedMapTaskBits.bits & (1 << t.TaskNumber)) == 0 {
						c.tasks <- t
					}
				}()
			}
		} else if t.TaskType == ReduceTask {
			c.FinishedReduceTaskBits.RLock()
			defer c.FinishedReduceTaskBits.RUnlock()

			if (c.FinishedReduceTaskBits.bits & (1 << t.TaskNumber)) != 0 {
				reply.TaskType = InvalidTask
			} else {
				reply.TaskType = ReduceTask
				reply.NMapTasks = c.NMapTasks
				reply.ReduceTaskNumber = t.TaskNumber

				go func() {
					time.Sleep(10 * time.Second)
					// 10s后还未完成认为其超时重新发送
					c.FinishedReduceTaskBits.RLock()
					defer c.FinishedReduceTaskBits.RUnlock()

					if (c.FinishedReduceTaskBits.bits & (1 << t.TaskNumber)) == 0 {
						c.tasks <- t
					}
				}()
			}
		} else if t.TaskType == FinishTask {
			reply.TaskType = FinishTask
		}
	}
	return nil
}

func (c *Coordinator) FinishedMapWork(args *MRArgs, reply *MRReply) error {
	c.FinishedMapTaskBits.Lock()
	c.FinishedMapTaskBits.bits |= 1 << args.FinishedMapTaskNumber
	c.FinishedMapTaskBits.Unlock()

	if c.isMapAllFinished() {
		for i := 0; i < c.NReduceTasks; i++ {
			t := &Task{
				TaskType:   ReduceTask,
				TaskNumber: i,
			}

			c.tasks <- t
		}
	}

	return nil
}

func (c *Coordinator) FinishedReduceWork(args *MRArgs, reply *MRReply) error {
	c.FinishedReduceTaskBits.Lock()
	c.FinishedReduceTaskBits.bits |= 1 << args.FinishedReduceTaskNumber
	c.FinishedReduceTaskBits.Unlock()

	if c.isReduceAllFinished() {
		t := &Task{
			TaskType: FinishTask,
		}

		c.tasks <- t
	}

	return nil
}

func (c *Coordinator) isMapAllFinished() bool {
	c.FinishedMapTaskBits.RLock()
	defer c.FinishedMapTaskBits.RUnlock()

	if c.FinishedMapTaskBits.bits == (1<<c.NMapTasks - 1) {
		return true
	}
	return false
}

func (c *Coordinator) isReduceAllFinished() bool {
	c.FinishedReduceTaskBits.RLock()
	defer c.FinishedReduceTaskBits.RUnlock()

	if c.FinishedReduceTaskBits.bits == (1<<c.NReduceTasks - 1) {
		return true
	}
	return false
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := false

	// Your code here.
	if c.isReduceAllFinished() {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	taskChan := make(chan *Task, nReduce)
	c.tasks = taskChan
	c.NMapTasks = len(files)
	c.NReduceTasks = nReduce
	c.FinishedMapTaskBits = &FinishedBitsWithLock{}
	c.FinishedReduceTaskBits = &FinishedBitsWithLock{}

	for i, file := range files {
		t := &Task{
			TaskType:   MapTask,
			FileName:   file,
			TaskNumber: i,
		}

		c.tasks <- t
	}

	c.server()
	return &c
}
