package kvraft

import (
	"6.824/labrpc"
	"strconv"
	"sync"
)
import "crypto/rand"
import "math/big"

var clerkCount = 0
var clerkCountMutex sync.Mutex

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mostRecentLeader int
	mu               sync.Mutex
	me               int
	cmdSeq           int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.mostRecentLeader = -1 // find leader
	ck.cmdSeq = 0

	clerkCountMutex.Lock()
	ck.me = clerkCount
	clerkCount++
	clerkCountMutex.Unlock()

	DPrintf("clerk %v start\n", ck.me)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()

	leaderIndex := ck.getLeaderIndex()
	args := GetArgs{
		Key:      key,
		CmdId:    ck.generateCmdIdAndPlusSeq(),
		ClientId: ck.me,
	}

	ck.mu.Unlock()

	returnValue := ""

	for {
		reply := GetReply{}

		ok := ck.servers[leaderIndex].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.mu.Lock()
				ck.mostRecentLeader = leaderIndex
				ck.mu.Unlock()
				returnValue = reply.Value

				DPrintf("ck %v send GET %+v to kvs %v success and result %v\n", ck.me, args, leaderIndex, reply.Value)

				break

			} else if reply.Err == ErrWrongLeader {
				leaderIndex = (leaderIndex + 1) % (len(ck.servers))
				//DPrintf("retry LeaderIndex %v\n", leaderIndex)

			} else {
				break
			}

		} else {
			DPrintf("ck %v send GTE timeout and retry", ck.me)
			leaderIndex = (leaderIndex + 1) % (len(ck.servers))
		}

	}
	return returnValue
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()

	leaderIndex := ck.getLeaderIndex()

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		CmdId:    ck.generateCmdIdAndPlusSeq(),
		ClientId: ck.me,
	}

	ck.mu.Unlock()

	for {
		//DPrintf("ck %v send PutAppend %+v to kvs %v before \n", ck.me, args, leaderIndex)
		reply := PutAppendReply{}

		ok := ck.servers[leaderIndex].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == OK {
				// success
				ck.mu.Lock()
				ck.mostRecentLeader = leaderIndex
				ck.mu.Unlock()

				DPrintf("ck %v send PutAppend %+v to kvs %v success \n", ck.me, args, leaderIndex)

				break
			} else if reply.Err == ErrWrongLeader {

				leaderIndex = (leaderIndex + 1) % len(ck.servers)

			} else {
				DPrintf("ck %v PutAppend %+v failed\n", ck.me, args)
				break
			}
		} else {
			//DPrintf("ck %v send PutAppend timeout", ck.me)
			leaderIndex = (leaderIndex + 1) % (len(ck.servers))
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// note, it's already with lock
func (ck *Clerk) generateCmdIdAndPlusSeq() string {
	cmdId := strconv.Itoa(ck.me) + ":" + strconv.Itoa(ck.cmdSeq)
	ck.cmdSeq++
	return cmdId
}

// note, it's already with lock
func (ck *Clerk) getLeaderIndex() int {
	if ck.mostRecentLeader >= 0 {
		return ck.mostRecentLeader
	}
	return int(nrand() % int64(len(ck.servers)))
}
