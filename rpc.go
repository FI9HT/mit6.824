package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type FetchTaskArgs struct {
}

type FetchTaskReply struct {
	Kind     int      // 0: map 1:reduce
	Result   int      // >=0：处理任务 -1：等待 -2：退出
	Filename []string // map任务的长度为1，reduce任务>=1
}

type KV struct {
	Key   string
	Value string
}

type SubmitTaskArgs struct {
	TaskSeq  int
	TaskKind int      // 0 map 1 reduce
	Filename []string // only used by map worker
}

type SubmitTaskReply struct {
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
