package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string // "Put" or "Append"
	CmdId     string
}

type SnapshotData struct {
	Data   map[string]string
	CmdIds []string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data           map[string]string
	cmdIdsFromRaft []string
	commitIndex    int
	cond           *sync.Cond
	leaderTerm     int
	persister      *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.findCmdId(args.CmdId, &kv.cmdIdsFromRaft) {
		// cmd has been applied
		value, ok := kv.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		DPrintf("kv %v PA arg %+v has been add, return\n", kv.me, args)
		return
	}

	op := Op{
		Key:       args.Key,
		Value:     "",
		Operation: "Get",
		CmdId:     args.CmdId,
	}

	// try to get consistence
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	DPrintf("kvs %v GET-WAIT op %+v in index %v", kv.me, op, index)

	if term > kv.leaderTerm {
		kv.leaderTerm = term
		kv.cond.Broadcast()
	}

	// wait cmd to be committed
	for {
		kv.cond.Wait()

		if kv.leaderTerm > term || kv.leaderTerm == 0 {
			DPrintf("kvs %v GET-ERR-LEADER op %+v in index %v", kv.me, op, index)
			reply.Err = ErrWrongLeader
			return
		}
		if kv.commitIndex >= index {
			break
		}
	}

	// if index has been committed, but cmdId is not found, indicate that leader has changed
	if !kv.findCmdId(args.CmdId, &kv.cmdIdsFromRaft) {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		DPrintf("kv %v GET-ERR-LEADER op %v has been replace", kv.me, op)
		return
	}

	key := args.Key
	value, ok := kv.data[key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}

	DPrintf("kvs %v GET-SUCCESS %v success, op %+v back to %v", kv.me, key, op, args.ClientId)

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//DPrintf("kvs %v REV PA args %+v\n", kv.me, args)

	if kv.findCmdId(args.CmdId, &kv.cmdIdsFromRaft) {
		DPrintf("kv %v PA arg %+v has been add, return\n", kv.me, args)
		reply.Err = OK
		return
	}

	op := Op{
		args.Key,
		args.Value,
		args.Op,
		args.CmdId,
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		if term > kv.leaderTerm {
			kv.leaderTerm = term
			kv.cond.Broadcast()
		}
	}

	DPrintf("kvs %v PA-WAIT op %+v in index %v", kv.me, op, index)

	// wait cmd to be committed
	for {
		kv.cond.Wait()

		if kv.leaderTerm > term || kv.leaderTerm == 0 {
			reply.Err = ErrWrongLeader
			DPrintf("kvs %v PA-ERR-LEADER op %+v in index %v", kv.me, op, index)
			return
		}
		if kv.commitIndex >= index {
			break
		}
	}

	if !kv.findCmdId(args.CmdId, &kv.cmdIdsFromRaft) {
		reply.Err = ErrWrongLeader
		DPrintf("kvs %v PA-SUCCESS op %v in index %v has been replace", kv.me, op, index)
		return
	}

	reply.Err = OK

	DPrintf("kvs %v PA success, op %+v index %v back to %v", kv.me, op, index, args.ClientId)

	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	//close(kv.applyCh)
	DPrintf("kvS %v has been killed\n", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.commitIndex = 0
	kv.leaderTerm = 0
	kv.cond = sync.NewCond(&kv.mu)
	kv.cmdIdsFromRaft = make([]string, 20)
	kv.persister = persister

	if kv.persister.RaftStateSize() != 0 {
		DPrintf("kvs %v raft state size is not 0, read to recover", kv.me)

		snapshotData := SnapshotData{}
		err := json.Unmarshal(kv.persister.ReadSnapshot(), &snapshotData)

		//DPrintf("kvs %v len(snapshotData)=%v", kv.me, len(snapshotData))

		if err != nil {
			DPrintf("JSON Unmarshal error")
		} else {
			kv.eraseAndCopyData(snapshotData.Data, snapshotData.CmdIds)
		}

		for i, j := range kv.data {
			DPrintf("kvs %v recover data key %v value %v", kv.me, i, j)
		}
	}

	go func() {
		kv.readApplyChLoop()
	}()

	//go func() {
	//	for !kv.killed() {
	//		kv.mu.Lock()
	//		DPrintf("==============kvs %v commit index %v================", kv.me, kv.commitIndex)
	//		for i, j := range kv.data {
	//			DPrintf("kvs %v CHECK key %v value %v", kv.me, i, j)
	//		}
	//		kv.mu.Unlock()
	//		time.Sleep(time.Second * time.Duration(1))
	//	}
	//}()

	DPrintf("KVS %v start", kv.me)

	return kv
}

// read from applyCh
func (kv *KVServer) readApplyChLoop() {

	DPrintf("kvs %v readapplyloop start...", kv.me)

	for !kv.killed() {
		msg := <-kv.applyCh
		DPrintf("KVS %v recv index %v", kv.me, msg.CommandIndex)

		kv.mu.Lock()
		if msg.CommandValid {
			//DPrintf("kvs %v get index %v from applyCh op %v, kvCommitIndex %v",
			//	kv.me, msg.CommandIndex, msg.Command.(Op), kv.commitIndex)

			if msg.CommandIndex < kv.commitIndex {
				DPrintf("ERROR! msg commitIndex < kv's commitIndex")
				kv.mu.Unlock()
				continue
			}
			// finish update commitIndex
			op := msg.Command.(Op)
			kv.commitIndex = msg.CommandIndex
			// apply, should avoid replicated op
			if !kv.findCmdId(op.CmdId, &kv.cmdIdsFromRaft) {
				kv.applyCmd(op)
				kv.checkWhetherNeedSnapshot(msg.CommandIndex)
			} else {
				DPrintf("cmdID %v already exist", op.CmdId)
			}
			// broadcast to notify every RPC
			kv.cond.Broadcast()
		} else {
			//DPrintf("WARING! msg commitIndex is invalid index %v", msg.CommandIndex)
			if msg.SnapshotIndex != 0 {
				snapshotData := SnapshotData{}
				err := json.Unmarshal(kv.persister.ReadSnapshot(), &snapshotData)
				kv.commitIndex = msg.SnapshotIndex
				if err != nil {
					DPrintf("JSON Unmarshal error")
				} else {
					kv.eraseAndCopyData(snapshotData.Data, snapshotData.CmdIds)
				}
			} else {
				kv.leaderTerm = 0 // indicate that leader change, and the RPC which is waiting for reply could return ERR LEADER right now
				kv.cond.Broadcast()
			}
		}

		kv.mu.Unlock()
	}
}

// note it's already locked
func (kv *KVServer) applyCmd(op Op) {
	key := op.Key
	value := op.Value

	if op.Operation == "Put" {
		kv.data[key] = value
	} else if op.Operation == "Append" {
		v, ok := kv.data[key]
		if ok {
			kv.data[key] = v + value
		} else {
			kv.data[key] = value
		}
		// debug
		value = kv.data[key]
	}
	kv.cmdIdsFromRaft = append(kv.cmdIdsFromRaft, op.CmdId)
	//if op.Operation != "Get" {
	DPrintf("kvs %v apply key %v  [op %v]", kv.me, key, op)
	//}
}

// note it's already locked
func (kv *KVServer) findCmdId(cmd string, cmdIds *[]string) bool {
	for _, j := range *cmdIds {
		if j == cmd {
			return true
		}
	}
	return false
}

// note it's already locked
func (kv *KVServer) checkWhetherNeedSnapshot(index int) {

	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize()+200 > kv.maxraftstate {

		snapshotData := SnapshotData{
			Data:   kv.data,
			CmdIds: kv.cmdIdsFromRaft,
		}
		snapshot, err := json.Marshal(snapshotData)
		if err != nil {
			DPrintf("json Marshal error")
			return
		}

		DPrintf("------------kvs %v snapshot(%v), raft state size %v max %v index %v---------------",
			kv.me, len(snapshot), kv.persister.RaftStateSize(), kv.maxraftstate, index)

		kv.rf.Snapshot(index, snapshot)

	}
}

func (kv *KVServer) eraseAndCopyData(data map[string]string, cmdIds []string) {

	for k, v := range kv.data {
		DPrintf("eraseAndCopyData1 kvs %v k %v, v %v", kv.me, k, v)
	}

	kv.cmdIdsFromRaft = kv.cmdIdsFromRaft[:0]
	for _, j := range cmdIds {
		kv.cmdIdsFromRaft = append(kv.cmdIdsFromRaft, j)
	}

	for key := range kv.data {
		delete(kv.data, key)
	}
	for i, j := range data {
		kv.data[i] = j
	}

	for k, v := range kv.data {
		DPrintf("eraseAndCopyData2 kvs %v k %v, v %v", kv.me, k, v)
	}
}
