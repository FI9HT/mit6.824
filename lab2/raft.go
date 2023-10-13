package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	logger *logrus.Logger
	once   sync.Once
)

// const config variable
const LeaderHeartbeatInterval = 150
const LeaderElectionTimeoutBase = 300
const LeaderElectionTimeoutRange = 300
const Follower = "Follower"
const Candidate = "Candidate"
const Leader = "Leader"

func makeLogger() *logrus.Logger {
	once.Do(func() {
		logger = logrus.New()
		logger.SetLevel(logrus.DebugLevel)
		logger.SetOutput(os.Stdout)
		logger.SetReportCaller(true)
		logrus.SetFormatter(&logrus.JSONFormatter{})
		logger.Formatter = &logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "15:04:05.000",
			CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
				fileName := path.Base(frame.File)
				return fmt.Sprintf("[%v]", frame.Line), fmt.Sprintf(" %s", fileName)
			},
		}
	})

	return logger
}

// new struct
type LogEntry struct {
	Command interface{}
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm               int
	votedFor                  int
	log                       map[int]LogEntry
	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int

	// volatile state
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// my code
	state        string // Follower, Candidate, Leader
	rpcTimestamp int64
	cond         *sync.Cond

	// channel
	applych chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == Leader
	return rf.currentTerm, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// term
	err := e.Encode(rf.currentTerm)
	if err != nil {
		logger.Errorf("encode error!\n")
		return
	}
	// votedFor
	err = e.Encode(rf.votedFor)
	if err != nil {
		logger.Errorf("encode error!\n")
		return
	}
	// log
	err = e.Encode(rf.log)
	if err != nil {
		logger.Errorf("encode error!\n")
		return
	}
	// snapshotLastIncludedIndex
	err = e.Encode(rf.snapshotLastIncludedIndex)
	if err != nil {
		logger.Errorf("encode error!\n")
		return
	}
	// snapshotLastIncludedTerm
	err = e.Encode(rf.snapshotLastIncludedTerm)
	if err != nil {
		logger.Errorf("encode error!\n")
		return
	}

	data := w.Bytes()

	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log map[int]LogEntry
	var snapshotLastIncludedIndex int
	var snapshotLastIncludedTerm int

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&snapshotLastIncludedIndex) != nil || d.Decode(&snapshotLastIncludedTerm) != nil {
		logger.Errorf("Decode error !!!\n")

	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotLastIncludedIndex = snapshotLastIncludedIndex
		rf.snapshotLastIncludedTerm = snapshotLastIncludedTerm
		rf.mu.Unlock()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.snapshotLastIncludedIndex || index > rf.getLastLogIndex() {
		logger.Warnf("Snapshot error range index %v rf.snapshotIndex %v lastIndex %v\n",
			index, rf.snapshotLastIncludedIndex, rf.getLastLogIndex())
		return
	}

	entry, _ := rf.log[index]

	for i := rf.snapshotLastIncludedIndex + 1; i <= index; i++ {
		delete(rf.log, i)
	}
	logger.Debugf("S %v delete [%v~%v]\n", rf.me, rf.snapshotLastIncludedIndex+1, index)

	rf.snapshotLastIncludedIndex = index
	rf.snapshotLastIncludedTerm = entry.Term

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

	if rf.state == Leader {
		for i, j := range rf.nextIndex {
			if i == rf.me {
				continue
			}
			if j <= rf.snapshotLastIncludedIndex {
				go func(index int) {
					rf.setAndSendInstallSnapshot(index)
				}(i)
			}
		}
	}

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntries RPC Struct Define
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
	LastLogIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// check whether server LogEntry is newer than or equal to leader's entry
func candidateLogIsUpToDate(serverIndex int, serverTerm int, candidateIndex int, candidateTerm int) bool {
	if candidateTerm > serverTerm {
		return true
	} else if candidateTerm == serverTerm {
		if candidateIndex >= serverIndex {
			return true
		}
	}
	return false
}

// note, all functions about log has been locked
func (rf *Raft) getLastLogIndex() int {
	return rf.snapshotLastIncludedIndex + len(rf.log)
}

func (rf *Raft) appendLog(entry LogEntry) {
	index := rf.getLastLogIndex() + 1
	_, ok := rf.log[index]
	if ok {
		logger.Errorf("rf.log[%v] already exist! =[%+v] return", index, rf.log[index])
		return
	}
	rf.log[index] = entry
}

// erase log after index, including index
func (rf *Raft) eraseLogAfterIndex(index int) {
	for i, _ := range rf.log {
		if i >= index {
			delete(rf.log, i)
		}
	}
}

// erase log before index, including index
func (rf *Raft) eraseLogBeforeIndex(index int) {
	for i, _ := range rf.log {
		if i <= index {
			delete(rf.log, i)
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		logger.Debugf("server [%d] not vote for [%d]\n\n", rf.me, args.CandidateId)

	} else {

		if args.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = args.Term // important here
			rf.votedFor = -1
		}

		lastLogIndex := rf.getLastLogIndex()
		if rf.votedFor == args.CandidateId || rf.votedFor == -1 {

			selfTerm := 0

			if lastLogIndex == rf.snapshotLastIncludedIndex {
				selfTerm = rf.snapshotLastIncludedTerm
			} else {
				selfTerm = rf.log[lastLogIndex].Term
			}

			if candidateLogIsUpToDate(lastLogIndex, selfTerm, args.LastLogIndex, args.LastLogTerm) {
				// vote
				rf.votedFor = args.CandidateId
				rf.state = Follower
				rf.rpcTimestamp = time.Now().UnixMicro()
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				logger.Debugf("server [%d] vote for [%d]\n\n", rf.me, args.CandidateId)
			}
		}
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//logger.Debugf("server [%d][%d] commitID [%v] lastLog[%v] => recv AE RPC from server[%v] lastLogIndex[%v] %+v \n\n",
	//	rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex, args.LeaderId, args.LastLogIndex, args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		// no need to do fast rollback, because peer will exit leader
	} else {

		rf.rpcTimestamp = time.Now().UnixMicro()

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
		}

		lastLogIndex := rf.getLastLogIndex()
		reply.Term = rf.currentTerm
		reply.Success = false

		if args.PreLogIndex > lastLogIndex {
			reply.XIndex = 0
			reply.XTerm = 0
			reply.XLen = rf.getLastLogIndex() + 1

		} else if args.PreLogIndex > rf.snapshotLastIncludedIndex && rf.log[args.PreLogIndex].Term != args.PreLogTerm {
			// PreLogIndex or PreLogTerm not match
			// fast roll back
			reply.XTerm = rf.log[args.PreLogIndex].Term
			firstIndex := args.PreLogIndex
			for index := args.PreLogIndex; index > rf.snapshotLastIncludedIndex; index-- {
				if rf.log[index].Term == rf.log[args.PreLogIndex].Term {
					firstIndex = index
				} else {
					break
				}
			}
			reply.XIndex = firstIndex
			reply.XLen = rf.getLastLogIndex() + 1

		} else {
			// preLogIndex and preLogTerm match, do log replication
			if args.Entries != nil {
				for argsIndex, j := range args.Entries {
					index := argsIndex + args.PreLogIndex + 1
					if index > rf.getLastLogIndex() {
						rf.appendLog(LogEntry{j.Command, j.Term})
					} else if index > rf.snapshotLastIncludedIndex && rf.log[index].Term != args.Entries[argsIndex].Term {
						// has conflict entry, delete it and all after it. important
						rf.eraseLogAfterIndex(index)
						rf.appendLog(LogEntry{j.Command, j.Term})
					}

				}
				//logger.Debugf("server[%v][%v] (( Append LogEntris )) from leader [%v] now lastLogIndex %v\n\n",
				//	rf.me, rf.currentTerm, args.LeaderId, rf.getLastLogIndex())
			}

			validCommitIndex := args.PreLogIndex + len(args.Entries)
			if validCommitIndex > args.LeaderCommit {
				validCommitIndex = args.LeaderCommit
			}

			if validCommitIndex > rf.commitIndex {
				if validCommitIndex > rf.getLastLogIndex() {
					rf.commitIndex = rf.getLastLogIndex()
				} else {
					rf.commitIndex = validCommitIndex
				}
				// update applyId
				rf.lastApplied = rf.commitIndex
				logger.Infof("server[%v][%v] in AE update commitIndex = %v", rf.me, rf.currentTerm, rf.commitIndex)
				rf.cond.Signal()
			}
			reply.Success = true
		}
	}
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logger.Debugf("S %v InstallSnapshot lastIndex %v lastTerm %v\n", rf.me, args.LastIncludedIndex, args.LastIncludedTerm)

	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {

		if args.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}

		if args.LastIncludedIndex > rf.snapshotLastIncludedIndex {

			rf.snapshotLastIncludedIndex = args.LastIncludedIndex
			rf.snapshotLastIncludedTerm = args.LastIncludedTerm

			// discard log
			rf.eraseLogBeforeIndex(args.LastIncludedIndex)
			rf.persist()
			rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)

			if rf.commitIndex < args.LastIncludedIndex {
				rf.commitIndex = args.LastIncludedIndex
				rf.lastApplied = rf.commitIndex
				rf.cond.Signal()
			}
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term := 0
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader && !rf.killed() {
		term = rf.currentTerm

		//rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		rf.appendLog(LogEntry{command, rf.currentTerm})
		index = rf.getLastLogIndex()
		rf.matchIndex[rf.me] = index

		logger.Debugf("====================== "+
			"RET COMMAND[%v] server[%v][%v][%v] isLeader[%v] logIndex[%d] "+
			"=====================\n\n",
			command, rf.me, rf.currentTerm, rf.commitIndex, rf.state == Leader, index)

		go func() {
			rf.handleClientRequest(index)
		}()

	} else {
		term = rf.currentTerm
		isLeader = false
	}
	rf.persist()

	return index, term, isLeader
}

// note, with lock
// find peer nextIndex by fast rollback
func (rf *Raft) fastRollBack(preLogIndex int, reply *AppendEntriesReply) int {

	firstIndex := 0
	if reply.XIndex == 0 {
		firstIndex = reply.XLen
		logger.Debugf("(1) leader[%d][%d] find firstIndex[%d]\n\n", rf.me, rf.currentTerm, firstIndex)
	} else {
		// try to find Xterm
		hasXterm := false
		for index := preLogIndex; index > rf.snapshotLastIncludedIndex; index-- {
			if rf.log[index].Term == reply.XTerm {
				hasXterm = true
				firstIndex = index
				logger.Debugf("(2) leader[%d][%d] find firstIndex[%d]\n\n", rf.me, rf.currentTerm, firstIndex)
				break
			}
		}
		if !hasXterm {
			firstIndex = reply.XIndex
			logger.Debugf("(3) leader[%d][%d] find firstIndex[%d]\n\n", rf.me, rf.currentTerm, firstIndex)
		}
	}

	return firstIndex
}

func (rf *Raft) setAndSendInstallSnapshot(serverIndex int) bool {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.snapshotLastIncludedIndex,
		rf.snapshotLastIncludedTerm,
		0,
		rf.persister.ReadSnapshot(),
		true,
	}
	reply := InstallSnapshotReply{}
	logger.Debugf("S %v send RPC-IS index %v\n", rf.me, serverIndex)
	rf.mu.Unlock()

	success := rf.sendInstallSnapshot(serverIndex, &args, &reply)

	rf.mu.Lock()
	if success {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		} else {
			if rf.nextIndex[serverIndex] <= args.LastIncludedIndex {
				rf.nextIndex[serverIndex] = args.LastIncludedIndex + 1
			}
		}
	}
	rf.mu.Unlock()
	return success
}

// sync Leader's log[rf.nextIndex[serverIndex]] ~ log[lastLogIndex] to serverIndex
// return true if log sync success
// return false if is not leader or timeout. (retry is decided by invoker)
func (rf *Raft) syncLogToSinglePeer(serverIndex int, lastLogIndex int) bool {

	for !rf.killed() {

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}

		if rf.nextIndex[serverIndex] <= rf.snapshotLastIncludedIndex {
			// todo Install Snapshot
			logger.Debugf("nextLogIndex %v <= rf.snapshotLastIncludedIndex %v\n", rf.nextIndex[serverIndex], rf.snapshotLastIncludedIndex)
			rf.mu.Unlock()

			rf.setAndSendInstallSnapshot(serverIndex)
			continue // may be invalid here again
		}

		// maybe other newer replication has accomplished
		// invalid nextLogIndex is impossible > lastLogIndex
		if lastLogIndex <= rf.matchIndex[serverIndex] {
			rf.mu.Unlock()
			return true
		}

		var entries []LogEntry
		nextLogIndex := rf.nextIndex[serverIndex]

		for i := nextLogIndex; i <= lastLogIndex; i++ {
			entry, ok := rf.log[i]
			if !ok {
				logger.Errorf("log entry error nextLogIndex %v lastLogIndex %v \n", nextLogIndex, lastLogIndex)
			}
			entries = append(entries, entry)
		}

		preLogIndex := nextLogIndex - 1
		preLogTerm := 0

		if preLogIndex == rf.snapshotLastIncludedIndex {
			preLogTerm = rf.snapshotLastIncludedTerm
		} else {
			preLogTerm = rf.log[preLogIndex].Term
		}

		args := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			preLogIndex,
			preLogTerm,
			entries,
			rf.commitIndex,
			lastLogIndex,
		}

		logger.Debugf("leader %v send RPC-AE index %v to %v\n\n",
			rf.me, nextLogIndex, serverIndex)

		rf.mu.Unlock()

		reply := AppendEntriesReply{}

		sendSuccess := rf.sendAppendEntries(serverIndex, &args, &reply)

		rf.mu.Lock()

		if rf.state != Leader || rf.currentTerm != args.Term || !sendSuccess {
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()

		if !reply.Success {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				logger.Debugf("leader[%v][%v] after AE-RPC(LOG) to server[%v] recv higher term[%v] from server[%v] and exit leader\n\n",
					rf.me, rf.currentTerm, serverIndex, reply.Term, serverIndex)

				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return false
			} else {
				// send success, but log not match, do fast roll back to find nextIndex
				if nextLogIndex == rf.nextIndex[serverIndex] {
					rf.nextIndex[serverIndex] = rf.fastRollBack(args.PreLogIndex, &reply)
					logger.Debugf("leader[%v][%v] after AE-RPC(LOG) to server[%v] fastRollback nextLogIndex[%v] preSnapshotIncluded[%v]\n\n",
						rf.me, rf.currentTerm, serverIndex, rf.nextIndex[serverIndex], rf.snapshotLastIncludedIndex)
				}
				rf.mu.Unlock()
			}
		} else {
			// success and update next and match index
			rf.mu.Lock()

			nextIndex := args.PreLogIndex + len(args.Entries) + 1

			if nextLogIndex >= rf.nextIndex[serverIndex] {
				rf.nextIndex[serverIndex] = nextIndex
			}

			matchIndex := nextIndex - 1

			matchIndexUpdate := false
			if matchIndex > rf.matchIndex[serverIndex] {
				rf.matchIndex[serverIndex] = matchIndex
				matchIndexUpdate = true
			}

			logger.Debugf("leader[%d][%d] after AE-RPC(LOG)[%v ~ %v] to server[%v] success. "+
				"nextIndex[%v] matchIndex[%v]\n\n",
				rf.me, rf.currentTerm, args.PreLogIndex+1, lastLogIndex, serverIndex, rf.nextIndex[serverIndex], rf.matchIndex[serverIndex])

			rf.mu.Unlock()

			if matchIndexUpdate {
				rf.checkMatchIndexToUpdateCommitIndex()
			}
			return true
		}
	}

	return false
}

func (rf *Raft) syncLogToAllPeers(lastLogIndex int) bool {
	rf.mu.Lock()
	logger.Debugf("leader[%d][%v][%v] start syncAllLog. lastLogIndex[%d]\n\n",
		rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex)
	rf.mu.Unlock()

	syncResultChan := make(chan bool, len(rf.peers))
	wg := sync.WaitGroup{}

	for serverIndex, _ := range rf.peers {
		if serverIndex == rf.me {
			continue
		}
		wg.Add(1)
		serverIndex := serverIndex
		go func() {
			defer wg.Done()
			result := false
			result = rf.syncLogToSinglePeer(serverIndex, lastLogIndex)
			syncResultChan <- result
		}()
	}
	go func() {
		wg.Wait()
		close(syncResultChan)
	}()

	syncSuccessNum := 1
	syncFailNum := 0
	majority := len(rf.peers) / 2

	for i := 0; i < len(rf.peers)-1; i++ {
		result := <-syncResultChan
		if result {
			syncSuccessNum++

			//rf.mu.Lock()
			//logger.Debugf("leader[%v][%v] syncResult successNum++ [%d]\n\n", rf.me, rf.currentTerm, syncSuccessNum)
			//rf.mu.Unlock()

			if syncSuccessNum > majority {
				return true
			}
		} else {
			syncFailNum++

			//rf.mu.Lock()
			//logger.Debugf("leader[%v][%v] syncResult failNum++ [%d]\n\n", rf.me, rf.currentTerm, syncFailNum)
			//rf.mu.Unlock()

			if syncFailNum > majority {
				return false
			}
		}
	}

	return false
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) election(electionTerm int) bool {

	rf.mu.Lock()

	logger.Infof("Server[%d][%v][commitIndex %v] start election. lastLogIndex[%d]\n\n",
		rf.me, rf.currentTerm, rf.commitIndex, rf.getLastLogIndex())

	rf.mu.Unlock()

	voteCh := make(chan bool, 1000)
	wg := sync.WaitGroup{}

	for serverIndex, _ := range rf.peers {
		if serverIndex == rf.me {
			continue
		}
		wg.Add(1)
		index := serverIndex
		go func() {
			defer wg.Done()
			rf.mu.Lock()

			lastLogIndex := rf.getLastLogIndex()
			lastLogTerm := 0

			if lastLogIndex == rf.snapshotLastIncludedIndex {
				lastLogTerm = rf.snapshotLastIncludedTerm
			} else if lastLogIndex > rf.snapshotLastIncludedIndex {
				lastLogTerm = rf.log[lastLogIndex].Term
			} else {
				logger.Errorf("lastLogIndex < rf.snapshotLastIncludedIndex, error\n")
				return
			}

			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				lastLogIndex,
				lastLogTerm,
			}
			reply := RequestVoteReply{}
			logger.Debugf("S %v send RPC-RV\n",
				rf.me)

			rf.mu.Unlock()

			sendSuccess := rf.sendRequestVote(index, &args, &reply)

			rf.mu.Lock()
			if rf.state == Candidate && rf.currentTerm == electionTerm && sendSuccess && reply.VoteGranted {
				voteCh <- true
			} else {
				voteCh <- false
			}
			rf.mu.Unlock()
		}()
	}

	majority := len(rf.peers) / 2
	voteGrantedNum := 1 // initial 1, because of self vote
	voteFailNum := 0

	go func() {
		wg.Wait()
		close(voteCh)
	}()

	for index := 0; index < len(rf.peers)-1; index++ {
		voteResult := <-voteCh
		if voteResult {
			voteGrantedNum++

			if voteGrantedNum > majority {
				rf.mu.Lock()

				if electionTerm == rf.currentTerm && rf.state == Candidate {
					rf.state = Leader
					// the rf.term should be equal to the args.term when get the majority vote. if not, fail
					// because maybe recv term > this.term at the moment after obtaining the majority of votes
					for i, _ := range rf.nextIndex {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}

					logger.Infof("====================  "+
						"server[%v][%v][%v] election success.  lastLogIndex[%v]"+
						"====================\n\n",
						rf.me, rf.currentTerm, rf.commitIndex, rf.getLastLogIndex())

				} else {
					logger.Warnf("leader[%v][%v] get majority vote, but term is not valid.", rf.me, rf.currentTerm)
				}

				rf.mu.Unlock()
				break
			}
		} else {
			voteFailNum++
			if voteFailNum > majority {
				rf.mu.Lock()
				if electionTerm == rf.currentTerm && rf.state == Candidate {
					rf.state = Follower
				}
				logger.Infof("server[%v][%v][%v] election failed. lastLogIndex[%v]\n",
					rf.me, rf.currentTerm, rf.commitIndex, rf.getLastLogIndex())
				rf.mu.Unlock()
				break
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state == Leader
}

func (rf *Raft) leaderLoop(term int) {

	go func() {
		rf.heartbeatLoop(term)
	}()

}

func (rf *Raft) handleClientRequest(lastLogIndex int) {

	rf.mu.Lock()
	//logger.Debugf("leader[%v][%v][%v] handleClientRequest index[%v]\n\n",
	//	rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex)
	term := rf.currentTerm
	rf.mu.Unlock()

	syncResult := rf.syncLogToAllPeers(lastLogIndex)

	rf.mu.Lock()
	if syncResult && term == rf.currentTerm && rf.state == Leader {

		if rf.commitIndex < lastLogIndex {
			rf.commitIndex = lastLogIndex
			rf.lastApplied = rf.commitIndex
			rf.cond.Signal()
			logger.Debugf("leader[%v][%v] syncAllLog [%v] success, update commitIndex[%v]",
				rf.me, rf.currentTerm, lastLogIndex, rf.commitIndex)
		}
	}

	rf.mu.Unlock()

}

func (rf *Raft) heartbeatLoop(term int) {

	for !rf.killed() {

		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for serverIndex, _ := range rf.peers {
			if serverIndex == rf.me {
				continue
			}

			serverIndex := serverIndex
			go func() {

				rf.mu.Lock()
				preLogIndex := rf.getLastLogIndex()
				preLogTerm := 0
				if preLogIndex == rf.snapshotLastIncludedIndex {
					preLogTerm = rf.snapshotLastIncludedTerm
				} else {
					preLogTerm = rf.log[preLogIndex].Term
				}

				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					preLogIndex,
					preLogTerm,
					nil,
					rf.commitIndex,
					0,
				}
				reply := AppendEntriesReply{}
				term := rf.currentTerm

				rf.mu.Unlock()
				logger.Debugf("leader %v send RPC-HB\n",
					rf.me)

				sendSuccess := rf.sendAppendEntries(serverIndex, &args, &reply)

				rf.mu.Lock()
				if rf.state != Leader || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				rf.mu.Lock()
				if sendSuccess {
					//logger.Debugf(" leader[%d][%d][commitIndex %v] after AE-RPC(HB) to server[%d] success reply[%+v]\n\n",
					//	rf.me, rf.currentTerm, rf.commitIndex, serverIndex, reply)
					if !reply.Success {
						if reply.Term > args.Term {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							logger.Debugf(" leader[%d][%d] after AE-RPC(HB) to server[%d][%v], which has "+
								"higher term, leader give up. \n\n",
								rf.me, rf.currentTerm, serverIndex, reply.Term)
							rf.persist()
							rf.mu.Unlock()
							return
						} else {
							// preLogIndex not match, sync log to peer
							go func() {
								rf.mu.Lock()
								lastLogIndex := rf.getLastLogIndex()
								rf.mu.Unlock()

								rf.syncLogToSinglePeer(serverIndex, lastLogIndex)
							}()
						}
					} else {
						// success
						logger.Debugf(" leader[%d][%d] after HB-RPC to s [%d][%v] success, set nextIndex[%v]",
							rf.me, rf.currentTerm, serverIndex, reply.Term, preLogIndex+1)
						rf.nextIndex[serverIndex] = preLogIndex + 1
						rf.matchIndex[serverIndex] = preLogIndex
						rf.mu.Unlock()
						rf.checkMatchIndexToUpdateCommitIndex()
						rf.mu.Lock()
					}

				}
				rf.mu.Unlock()
			}()
		}

		time.Sleep(time.Millisecond * time.Duration(LeaderHeartbeatInterval))
	}
}

func (rf *Raft) sendCommittedLogToApplyChLoop() {

	lastCommittedLogIndex := 1

	for !rf.killed() {

		rf.mu.Lock()
		rf.cond.Wait()

		commitIndex := rf.commitIndex

		var msgs []ApplyMsg

		for index := lastCommittedLogIndex; index <= commitIndex; {

			msg := ApplyMsg{}

			if index > rf.snapshotLastIncludedIndex {
				msg.CommandIndex = index
				msg.CommandValid = true
				msg.Command = rf.log[index].Command
				index++
			} else {
				msg.CommandValid = false
				msg.SnapshotValid = true
				msg.SnapshotIndex = rf.snapshotLastIncludedIndex
				msg.SnapshotTerm = rf.snapshotLastIncludedTerm
				msg.Snapshot = rf.persister.ReadSnapshot()
				index = rf.snapshotLastIncludedIndex + 1
			}

			msgs = append(msgs, msg)
		}
		lastCommittedLogIndex = commitIndex + 1

		rf.mu.Unlock()

		for _, j := range msgs {
			rf.applych <- j
		}

	}
}

func (rf *Raft) checkMatchIndexToUpdateCommitIndex() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	majority := len(rf.peers) / 2
	matchIndex := -1

	for index := rf.getLastLogIndex(); index > rf.snapshotLastIncludedIndex && rf.log[index].Term == rf.currentTerm && rf.state == Leader; index-- {
		peersMatchNum := 0

		for _, matchLogIndex := range rf.matchIndex {
			if matchLogIndex >= index {
				peersMatchNum++
				if peersMatchNum > majority {
					break
				}
			}
		}
		if peersMatchNum > majority && index > rf.commitIndex {
			logger.Debugf("leader[%d][%d] update commitIndex to [%v] in checkMatchIndexToUpdateCommitIndex\n\n", rf.me, rf.currentTerm, index)
			matchIndex = index
			break
		}
	}

	if matchIndex != -1 && rf.state == Leader {
		rf.commitIndex = matchIndex
		rf.lastApplied = rf.commitIndex
		rf.cond.Signal()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rand.NewSource(time.Now().UnixNano())
		electionTimeout := rand.Intn(LeaderElectionTimeoutRange) + LeaderElectionTimeoutBase

		oldTime := time.Now().UnixMicro()
		time.Sleep(time.Millisecond * time.Duration(electionTimeout))

		rf.mu.Lock()
		if oldTime > rf.rpcTimestamp && rf.state != Leader {
			// need to start election
			logger.Debugf("S%v start new elction", rf.me)
			rf.currentTerm++
			term := rf.currentTerm
			rf.votedFor = rf.me
			rf.state = Candidate
			rf.persist()
			rf.mu.Unlock()

			go func() {
				electionSuccess := rf.election(term)
				if electionSuccess {
					rf.leaderLoop(term)
				}
			}()
		} else {
			rf.mu.Unlock()
		}

	}

	logger.Debugf("server[%d] has been killed!\n\n", rf.me)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	//mm := make(map[int]LogEntry)
	//fmt.Printf("[%v]\n", mm[1].Term)
	//fmt.Printf("[%v]\n", mm[2].Command)
	//
	//time.Sleep(time.Duration(100) * time.Second)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	makeLogger()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cond = sync.NewCond(&rf.mu)

	atomic.StoreInt32(&rf.dead, 0)
	rf.log = make(map[int]LogEntry)
	rf.log[0] = LogEntry{nil, 0}

	rf.snapshotLastIncludedIndex = -1
	rf.snapshotLastIncludedTerm = -1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.applych = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	go func() {
		rf.sendCommittedLogToApplyChLoop()
	}()

	//go func() {
	//	for !rf.killed() {
	//		rf.mu.Lock()
	//		var result strings.Builder
	//		for index, entry := range rf.log {
	//			result.WriteString(fmt.Sprintf("[%v,%v]", index, entry.Term))
	//		}
	//		logger.Debugf("server [%v][%v][%v] lastLogIndex[%v]-> %v\n\n", rf.me, rf.currentTerm, rf.commitIndex, rf.getLastLogIndex(), result.String())
	//		rf.mu.Unlock()
	//		time.Sleep(time.Millisecond * time.Duration(1000))
	//	}
	//}()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
