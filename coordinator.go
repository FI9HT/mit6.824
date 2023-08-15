package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskNotProcessed     = 0
	TaskBeingProcess     = 1
	TaskCompletedProcess = 2
)

type Task struct {
	filename []string // Map任务
	kva      []KV     // Reduce任务
	status   int      // 0：待处理 1：正在处理 2：已完成
}

type TaskManager struct {
	tasks             []Task     // 任务列表
	mutex             sync.Mutex // lock tasks
	notProcessedNum   int        // 没有分配的任务数量
	beingProcessedNum int        // 已分配，正在处理中的任务数量
	completedNum      int        // 已完成的任务数量
	totalTaskNum      int        // 任务总数
	kind              int        // 0: map task, 1: reduce task
}

/*
返回任务状态信息
-2: 全部任务完成
-1：有任务未完成，但是已经分配到其他worker
0: 有待分配的任务
*/
func (t *TaskManager) getTasksStatus() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.completedNum == t.totalTaskNum {
		return -2
	}
	if t.notProcessedNum != 0 {
		return 0
	}
	return -1
}

/*
返回一个待分配的任务的序号和内容
return
>= 0, success
< 0, fail
*/
func (t *TaskManager) getATask() (int, []string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for i := range t.tasks {
		task := &t.tasks[i]
		// 有等待分配的任务
		if task.status == TaskNotProcessed {
			task.status = TaskBeingProcess
			t.notProcessedNum--
			t.beingProcessedNum++
			retFilename := t.tasks[i].filename
			// 开启计时器，超时重置
			go t.processCountdown(i)
			//fmt.Printf("分配任务[%d] 开始计时\n", i)
			return i, retFilename
		}
	}
	//没有待分配的任务
	return -1, nil
}

func (t *TaskManager) completeTask(taskSeq int, taskKind int) bool {
	if taskSeq < 0 || taskSeq >= len(t.tasks) {
		return false
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.tasks[taskSeq].status == TaskBeingProcess {
		t.tasks[taskSeq].status = TaskCompletedProcess
		if taskKind == 0 {
			//fmt.Println("Map 任务 [", taskSeq, "] 完成")
		} else {
			//fmt.Println("Reduce 任务 [", taskSeq, "] 完成")
		}
		t.beingProcessedNum--
		t.completedNum++
		return true
	}
	return false
}

func (t *TaskManager) processCountdown(taskSeq int) {
	i := 0
	for i < 10 {
		t.mutex.Lock()
		if t.tasks[taskSeq].status == TaskCompletedProcess {
			t.mutex.Unlock()
			return
		}
		t.mutex.Unlock()
		time.Sleep(1 * time.Second)
		i++
	}
	t.mutex.Lock()
	t.tasks[taskSeq].status = TaskNotProcessed
	t.notProcessedNum++
	t.beingProcessedNum--
	t.mutex.Unlock()
	//fmt.Printf("任务 [%d] 超时未完成，重置\n", taskSeq)
}

func (t *TaskManager) appendReduceTaskFile(taskSeq int, file string) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if taskSeq >= len(t.tasks) {
		return false
	}
	t.tasks[taskSeq].filename = append(t.tasks[taskSeq].filename, file)
	return true
}

type Coordinator struct {
	// Your definitions here.
	mapTasksManager    TaskManager
	reduceTasksManager TaskManager
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	//fmt.Printf("Coordinary [SubmitTask] was called.\n")
	taskKind := args.TaskKind
	taskSeq := args.TaskSeq
	if args.TaskKind == 0 {
		// 处理map任务的提交
		taskManager := &c.mapTasksManager
		taskManager.completeTask(taskSeq, taskKind)
		// modify reduce task
		for _, file := range args.Filename {
			strArr := strings.Split(file, "-")
			if len(strArr) < 3 {
				fmt.Println("split error, len < 3")
				continue
			}
			// 获取文件名对应的reduce任务
			reduceTaskSeq, _ := strconv.Atoi(strArr[2])
			c.reduceTasksManager.appendReduceTaskFile(reduceTaskSeq, file)
			//fmt.Printf("把文件[%s]放入reduceTask[%d]\n", file, reduceTaskSeq)
		}

	} else {
		taskManager := &c.reduceTasksManager
		taskManager.completeTask(taskSeq, taskKind)
	}

	return nil
}

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	//fmt.Printf("Coordinary [FetchMapTask] was called.\n")
	// 处理map任务的分配
	status := c.mapTasksManager.getTasksStatus()
	if status == 0 {
		// 分配map任务
		seq, files := c.mapTasksManager.getATask()
		if seq >= 0 {
			reply.Kind = 0
			reply.Result = seq
			reply.Filename = files
			return nil
		} else {
			// 没有要分配的任务
			reply.Result = -1
			return nil
		}
	} else if status == -1 {
		// 直接返回让worker继续等待
		reply.Result = -1
		return nil
	}
	// 处理reduce任务的分配
	status = c.reduceTasksManager.getTasksStatus()
	if status == 0 {
		seq, files := c.reduceTasksManager.getATask()
		if seq >= 0 {
			// 分配reduce任务
			reply.Kind = 1
			reply.Result = seq
			reply.Filename = files
			return nil
		} else {
			// 没有待分配的任务
			reply.Result = -1
			return nil
		}
	} else if status == -1 {
		reply.Result = -1
		return nil
	}
	// 全部任务完成，让worker退出
	reply.Result = -2
	return nil
}

// Example an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	//fmt.Println("coordinary func [Example] was called")
	reply.Y = args.X + 1
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
	fmt.Println("http start...", sockname)
	//fmt.Println("coordinary server start...")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if i := c.reduceTasksManager.getTasksStatus(); i == -2 {
		return true
	}

	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	// 初始化Map任务
	for _, j := range files {
		task := Task{
			filename: []string{j},
			kva:      nil,
			status:   TaskNotProcessed,
		}
		c.mapTasksManager.tasks = append(c.mapTasksManager.tasks, task)
		c.mapTasksManager.notProcessedNum++
		//fmt.Println("初始化 map task ", i)
	}
	c.mapTasksManager.totalTaskNum = len(c.mapTasksManager.tasks)
	c.mapTasksManager.notProcessedNum = c.mapTasksManager.totalTaskNum

	// 初始化reduce任务
	for i := 0; i < nReduce; i++ {
		task := Task{
			filename: nil,
			kva:      nil,
			status:   TaskNotProcessed,
		}
		c.reduceTasksManager.tasks = append(c.reduceTasksManager.tasks, task)
		//fmt.Println("初始化 reduce task ", i)
	}
	c.reduceTasksManager.totalTaskNum = len(c.reduceTasksManager.tasks)
	c.reduceTasksManager.notProcessedNum = c.reduceTasksManager.totalTaskNum

	//fmt.Println("Map 任务生成完毕，有 [", c.mapTasksManager.notProcessedNum, "] 个任务")
	//fmt.Println("Reduce 任务生成完毕，有 [", c.reduceTasksManager.notProcessedNum, "] 个任务, ordinary server...")
	c.server()
	return &c
}
