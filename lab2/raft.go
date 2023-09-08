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
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var (
	logger *logrus.Logger
	once   sync.Once
)

func makeLogger() *logrus.Logger {
	once.Do(func() {
		logger = logrus.New()
		logger.SetLevel(logrus.ErrorLevel)
		logger.SetOutput(os.Stdout)
		logger.SetReportCaller(true)
		logger.Formatter = &logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05.000",
			CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
				fileName := path.Base(frame.File)
				return fmt.Sprintf("[%d]", frame.Line), fmt.Sprintf(" %s", fileName)
			},
		}

	})
	return logger
}

// new struct
type LogEntry struct {
	Command string
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
	YcDebug       int
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
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile state
	commitIndex int
	lastApplied int
	// for leader state
	nextIndex  []int
	matchIndex []int

	// my code
	isLeader             bool
	hasReceivedHeartbeat bool
	heartbeatTime        int // heartbeat : every 100ms
	electionTimeout      int // election timeout: 400-600ms
	electionTimeoutRange int
	applych              chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.isLeader
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

// AppendEntries define
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// todo
	// add log replication and safety check

	logger.Debugf("server [%d][%d] receive RequestVote from [%d][%d]\n",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)

	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = true
		logger.Debugf("server [%d] vote to [%d]\n", rf.me, args.CandidateId)
	} else {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		logger.Debugf("server [%d] not vote to [%d]\n", rf.me, args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	logger.Debugf("server[%d][%d] recv heartbeat from [%d][%d]\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	rf.mu.Lock()
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.hasReceivedHeartbeat = true
		if rf.isLeader {
			rf.isLeader = false
		}
		rf.mu.Unlock()

		if args.Entries != nil {
			// todo
			// log replication
		}

		reply.Term = rf.currentTerm
		reply.Success = true

	} else {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	//logger.Debugf("server[%d] has been killed\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendRequestVoteTimeOut(serverIndex int, args RequestVoteArgs) bool {
	var requestVoteReply RequestVoteReply
	requestVoteReply.VoteGranted = false
	sendSuccess := make(chan bool)

	go func() {
		//logger.Infof("server[%d][%d] sendRequestVote to [%d]\n", rf.me, rf.currentTerm, serverIndex)
		if !rf.sendRequestVote(serverIndex, &args, &requestVoteReply) {
			sendSuccess <- false
		} else {
			sendSuccess <- true
		}
	}()

	select {
	case success := <-sendSuccess:
		if success {
			// success
			logger.Infof("server[%d][%d] sendRequestVote to [%d] success. reply[%t]\n",
				rf.me, rf.currentTerm, serverIndex, requestVoteReply.VoteGranted)
			if requestVoteReply.VoteGranted {
				return true
			}
		} else {
			// fail
			logger.Infof("server[%d][%d] sendRequestVote to [%d] fail\n", rf.me, rf.currentTerm, serverIndex)
		}
	case <-time.After(time.Millisecond * time.Duration(100)):
		// timeout
		logger.Infof("server[%d][%d] sendRequestVote to [%d] timeout\n", rf.me, rf.currentTerm, serverIndex)
	}

	return false
}

func (rf *Raft) sendAppendEntriesTimeOut(serverIndex int, appendEntriesArgs AppendEntriesArgs) bool {
	var appendEntriesReply AppendEntriesReply
	sendSuccess := make(chan bool)

	go func() {
		//logger.Debugf("server[%d] sendAppendEntries id [%d]\n", rf.me, serverIndex)

		if !rf.sendAppendEntries(serverIndex, &appendEntriesArgs, &appendEntriesReply) {
			sendSuccess <- false
		} else {
			sendSuccess <- true
		}
	}()

	select {
	case success := <-sendSuccess:
		if success {
			// success
			logger.Infof("server[%d] sendAppendEntries to [%d] success. reply[%t]\n",
				rf.me, serverIndex, appendEntriesReply.Success)
			if appendEntriesReply.Success {
				return true
			}
		} else {
			// fail
			logger.Infof("server[%d][%d] sendAppendEntries to [%d] fail\n", rf.me, rf.currentTerm, serverIndex)
		}
	case <-time.After(time.Millisecond * time.Duration(100)):
		// timeout
		logger.Infof("server[%d][%d] sendAppendEntries to [%d] timeout\n", rf.me, rf.currentTerm, serverIndex)
	}

	return false
}

func (rf *Raft) election() {

	rf.mu.Lock()
	rf.currentTerm++
	logger.Infof("server[%d] start election. term[%d]\n", rf.me, rf.currentTerm)
	var lastTerm int
	var lastIndex int
	if len(rf.log) == 0 {
		lastTerm = -1
		lastIndex = -1
	} else {
		lastIndex = len(rf.log) - 1
		lastTerm = rf.log[lastIndex].Term
	}

	requestVoteArgs := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastIndex,
		lastTerm,
	}
	majority := len(rf.peers) / 2
	rf.hasReceivedHeartbeat = false
	rf.mu.Unlock()

	voteGrantedNum := 1 // initial 1, because of self vote
	election := false

	for serverIndex, _ := range rf.peers {
		if rf.killed() {
			return
		}
		if serverIndex == rf.me {
			continue
		}
		// when recv heartbeat which has higher term, stop election
		rf.mu.Lock()
		if rf.hasReceivedHeartbeat {
			rf.mu.Unlock()
			logger.Infof("leader[%d] hasReceivedHeartbeat in election.give up and return\n", rf.me)
			return
		}
		rf.mu.Unlock()

		voteResult := rf.sendRequestVoteTimeOut(serverIndex, requestVoteArgs)

		// question 得到大多数的投票后还要不要继续发送VoteRequest？我认为不用
		if voteResult {
			voteGrantedNum++
			if voteGrantedNum > majority {
				election = true
				rf.mu.Lock()
				rf.isLeader = true
				rf.mu.Unlock()
				break
			}
		}
	}

	if !election {
		logger.Infof("server[%d] election failed\n", rf.me)
		return
	} else {
		logger.Infof("=============server[%d] election success=============\n", rf.me)
		rf.keepSendHeartbeat()
	}
}

func (rf *Raft) keepSendHeartbeat() {
	for !rf.killed() {
		//successFlag := rf.sendAllHeartbeat()

		rf.mu.Lock()
		appendEntriesArgs := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			-1,
			-1,
			nil,
			-1,
		}
		rf.mu.Unlock()

		majority := len(rf.peers) / 2
		heartbeatSuccessNum := 1 // already has self

		// send heartbeat to all server
		for i, _ := range rf.peers {
			if rf.killed() {
				return
			}
			if i == rf.me {
				continue
			}
			// if recv higher term heartbeat, stop.
			rf.mu.Lock()
			if rf.hasReceivedHeartbeat {
				rf.mu.Unlock()
				logger.Infof("leader[%d][sendAppendEntries], recv hasReceivedHeartbeat, give up leader\n", rf.me)
				return
			}
			rf.mu.Unlock()

			sendResult := rf.sendAppendEntriesTimeOut(i, appendEntriesArgs)
			if sendResult {
				heartbeatSuccessNum++
			}
		}
		var successFlag bool
		if heartbeatSuccessNum > majority {
			successFlag = true
		} else {
			successFlag = false
			logger.Infof("server[%d] didn't recv majority heartbeat ack. heartbeatSuccessNum[%d] majority[%d]\n",
				rf.me, heartbeatSuccessNum, majority)
		}

		if !successFlag {
			rf.mu.Lock()
			rf.isLeader = false
			rf.mu.Unlock()
			return
		}
		logger.Debugf("leader[%d] sleep 100ms\n", rf.me)
		time.Sleep(time.Millisecond * time.Duration(rf.heartbeatTime))
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
		electionTimeout := rand.Intn(200) + 200
		logger.Debugf("[ticker] server[%d] sleep[%d]ms\n", rf.me, electionTimeout)
		time.Sleep(time.Millisecond * time.Duration(electionTimeout))

		rf.mu.Lock()
		if !rf.hasReceivedHeartbeat {
			// call election
			rf.mu.Unlock()
			rf.election()
			rf.mu.Lock()
		}
		// reset, wait heartbeat again.
		rf.hasReceivedHeartbeat = false
		rf.mu.Unlock()
	}
	logger.Debugf("server[%d] has been killed!\n", rf.me)
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
	rf.isLeader = false
	// ms
	rf.heartbeatTime = 100
	rf.electionTimeout = 400
	rf.electionTimeoutRange = 200
	atomic.StoreInt32(&rf.dead, 0)
	rf.applych = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	logger.Debugf("server [%d][%d] running...raft[%p]\n", rf.me, rf.currentTerm, peers[rf.me])

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

type LogWriter struct{}

func (lw *LogWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}
