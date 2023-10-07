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
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strings"
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

// config variable
const leaderHeartbeatInterval = 150
const leaderElectionTimeoutBase = 300
const leaderElectionTimeoutRange = 300
const leaderStopKeepSyncLog = 0
const leaderStopKeepHeartbeat = 2
const leaderRPCTimeout = 100

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
	isLeader         bool
	isElectingLeader bool
	//hasReceivedHeartbeat bool
	syncLogCh           chan int
	applych             chan ApplyMsg
	resetElectionTimeCh chan bool
	exitLeaderCh        chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	var log []LogEntry

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		logger.Errorf("Decode error !!!\n\n")
	} else {
		//logger.Debugf("Decode => term %v votedFor %v loglen %v \n\n", term, votedFor, len(log))
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
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
	LastLogIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIndex := len(rf.log) - 1
	//logger.Debugf("server [%d][%d][%v] receive RequestVote from [%d][%d] "+
	//	"LastLog Index[%d] Term[%d]. Arg LastLog Index[%d] Term[%d]\n\n",
	//	rf.me, rf.currentTerm, len(rf.log), args.CandidateId, args.Term, lastIndex, rf.log[lastIndex].Term, args.LastLogIndex, args.LastLogTerm)

	if args.Term >= rf.currentTerm {

		if args.Term > rf.currentTerm {
			if rf.isLeader {
				rf.isLeader = false // important also
				//rf.leaderExitClear()
				rf.exitLeaderCh <- true
			}
			rf.currentTerm = args.Term // important here
			rf.votedFor = -1
		}
		if rf.votedFor == -1 && candidateLogIsUpToDate(lastIndex, rf.log[lastIndex].Term, args.LastLogIndex, args.LastLogTerm) {
			// vote
			rf.votedFor = args.CandidateId
			rf.isLeader = false
			rf.resetElectionTimeCh <- true
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			logger.Debugf("server [%d] vote for [%d]\n\n", rf.me, args.CandidateId)
			return
		}
	}
	// not vote
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.persist()
	logger.Debugf("server [%d] not vote for [%d]\n\n", rf.me, args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex := len(rf.log) - 1

	//logger.Debugf("server [%d][%d] commitID [%v] lastLog[%v] => recv AE RPC from server[%v] lastLogIndex[%v] %+v \n\n",
	//	rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex, args.LeaderId, args.LastLogIndex, args)

	if args.Term >= rf.currentTerm {
		if rf.isLeader {
			rf.isLeader = false
			//rf.leaderExitClear()
			rf.exitLeaderCh <- true
		} else {
			rf.resetElectionTimeCh <- true
		}

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		reply.Term = rf.currentTerm
		reply.Success = true

		if args.PreLogIndex > lastLogIndex || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
			// PreLogIndex or PreLogTerm not match
			reply.Success = false
			// fast roll back
			if args.PreLogIndex <= lastLogIndex {
				reply.XTerm = rf.log[args.PreLogIndex].Term
				firstIndex := args.PreLogIndex
				for index := args.PreLogIndex; index > 0; index-- {
					if rf.log[index].Term == rf.log[args.PreLogIndex].Term {
						firstIndex = index
					} else {
						break
					}
				}
				reply.XIndex = firstIndex
			}

		} else {
			// PreLogIndex and PreLogTerm match, do Log Replicate
			if args.Entries != nil {
				for i, j := range args.Entries {
					index := i + args.PreLogIndex + 1
					if index >= len(rf.log) {
						rf.log = append(rf.log, LogEntry{j.Command, j.Term})
					} else {
						if rf.log[index].Term != args.Entries[i].Term {
							// has conflict entry, delete it and all after it. important
							rf.log = rf.log[:index]
							rf.log = append(rf.log, LogEntry{j.Command, j.Term})
						}
					}
				}
				logger.Debugf("server[%v][%v] (( Append LogEntris )) from leader [%v] now logLen %v\n\n",
					rf.me, rf.currentTerm, args.LeaderId, len(rf.log))
			}

			// normal heartbeat, sync commitId & cut log
			//rf.log = rf.log[:args.PreLogIndex+1]	// maybe error, recv logreplicate and immediately recv heartbeat, be cover cut
			if len(rf.log)-1 >= rf.commitIndex && args.LeaderCommit > rf.commitIndex {
				oldCommitIndex := rf.commitIndex
				if args.LeaderCommit > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				logger.Debugf("server[%v][%v] in AE update commitIndex = %v", rf.me, rf.currentTerm, rf.commitIndex)
				// update applyId
				rf.lastApplied = rf.commitIndex

				for commitIndex := oldCommitIndex + 1; commitIndex <= rf.commitIndex; commitIndex++ {
					msg := ApplyMsg{}
					msg.CommandIndex = commitIndex
					msg.CommandValid = true
					msg.Command = rf.log[commitIndex].Command
					rf.applych <- msg
				}
			}
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	reply.XLen = len(rf.log)
	rf.persist()
	logger.Debugf("server [%d][%d] commitID [%v] lastLog[%v] => reply AE RPC to server[%v]  %+v \n\n",
		rf.me, rf.currentTerm, rf.commitIndex, len(rf.log)-1, args.LeaderId, reply)
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

	if rf.isLeader && !rf.killed() {
		term = rf.currentTerm
		isLeader = true

		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		rf.matchIndex[rf.me] = len(rf.log) - 1
		index = len(rf.log) - 1

		logger.Debugf("====================== "+
			"RET COMMAND[%v] server[%v][%v][%v] isLeader[%t] logIndex[%d] "+
			"=====================\n\n",
			command, rf.me, rf.currentTerm, rf.commitIndex, isLeader, index)

		rf.syncLogCh <- index

	} else {
		term = rf.currentTerm
		isLeader = false
	}
	rf.persist()

	return index, term, isLeader
}

// sync Leader's log[rf.nextIndex[serverIndex]] ~ log[lastLogIndex] to serverIndex
// return true if LogReplication success
// return false if is not leader or wrong preLogIndex
func (rf *Raft) syncLogToSinglePeer(serverIndex int, lastLogIndex int) bool {

	for !rf.killed() {

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			//logger.Debugf("leader[%d][%d] is not leader, exit synceLog loop[%d]\n\n", rf.me, rf.currentTerm, serverIndex)
			break
		}

		nextLogIndex := rf.nextIndex[serverIndex]

		// maybe other newer LogReplication has accomplished
		if nextLogIndex >= lastLogIndex+1 {
			logger.Warnf("leader[%d][%d] syncLog[%v ~ %v] to server[%v] but return \n\n",
				rf.me, rf.currentTerm, nextLogIndex, lastLogIndex, serverIndex)
			rf.mu.Unlock()
			return true
		}

		//logger.Debugf("leader[%d][%d] start AE-RPC(LOG)[%v ~ %v] to server[%v]\n\n",
		//	rf.me, rf.currentTerm, nextLogIndex, lastLogIndex, serverIndex)

		args := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			nextLogIndex - 1,
			rf.log[nextLogIndex-1].Term,
			rf.log[nextLogIndex : lastLogIndex+1],
			rf.commitIndex,
			lastLogIndex,
		}

		rf.mu.Unlock()

		reply := AppendEntriesReply{}

		sendSuccess := rf.sendAppendEntries(serverIndex, &args, &reply)

		rf.mu.Lock()
		if !rf.isLeader || rf.currentTerm != args.Term {
			//logger.Debugf("leader[%v][%v] give up leader in synclog\n\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()

		if !sendSuccess {
			return false
		}

		rf.mu.Lock()
		logger.Debugf("leader[%d][%d][commitIndex %v] after AE-RPC(LOG)[%v ~ %v] to server[%v], success reply[%+v]\n\n",
			rf.me, rf.currentTerm, rf.commitIndex, nextLogIndex, lastLogIndex, serverIndex, reply)
		rf.mu.Unlock()

		if !reply.Success {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				// give up leader
				logger.Debugf("leader[%v][%v] after AE-RPC(LOG) to server[%v] recv higher term[%v] from server[%v] and give up leader\n\n",
					rf.me, rf.currentTerm, serverIndex, reply.Term, serverIndex)
				rf.isLeader = false
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				//rf.leaderExitClear()
				rf.exitLeaderCh <- true
				rf.mu.Unlock()
				return false
			} else {
				// fast roll back
				if nextLogIndex == rf.nextIndex[serverIndex] {
					firstIndex := serverIndex
					if reply.XIndex == 0 {
						firstIndex = reply.XLen
						logger.Debugf("(1) leader[%d][%d] find firstIndex[%d]\n\n", rf.me, rf.currentTerm, firstIndex)
					} else {
						// try to find Xterm
						hasXterm := false
						for index := len(rf.log) - 1; index > 0; index-- {
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
					rf.nextIndex[serverIndex] = firstIndex
					logger.Debugf("leader[%v][%v] after AE-RPC(LOG) to server[%v] update nextLogIndex[%v]\n\n",
						rf.me, rf.currentTerm, serverIndex, rf.nextIndex[serverIndex])
				}
			}
			rf.mu.Unlock()
		} else {
			// success and update next and match info
			rf.mu.Lock()
			nextIndex := args.PreLogIndex + len(args.Entries) + 1
			matchIndex := nextIndex - 1

			if rf.nextIndex[serverIndex] == nextLogIndex {
				rf.nextIndex[serverIndex] = nextIndex
			}

			if matchIndex > rf.matchIndex[serverIndex] {
				rf.matchIndex[serverIndex] = matchIndex
			}

			logger.Debugf("leader[%d][%d] after AE-RPC(LOG)[%v ~ %v] to server[%v] success. "+
				"nextIndex[%v] matchIndex[%v]\n\n",
				rf.me, rf.currentTerm, args.PreLogIndex+1, lastLogIndex, serverIndex, rf.nextIndex[serverIndex], rf.matchIndex[serverIndex])

			rf.mu.Unlock()
			return true
		}
	}

	return false
}

func (rf *Raft) syncLogToAllPeers(lastLogIndex int) bool {
	logger.Debugf("leader[%d][%v][%v] start syncAllLog. lastLogIndex[%d]\n\n",
		rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex)

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
			result := rf.syncLogToSinglePeer(serverIndex, lastLogIndex)
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
			if syncSuccessNum > majority {
				rf.mu.Lock()
				logger.Debugf("leader[%v][%v] syncMajority success.lastLogIndex[%d]\n\n", rf.me, rf.currentTerm, lastLogIndex)
				rf.mu.Unlock()
				return true
			}
		} else {
			syncFailNum++
			if syncFailNum > majority {
				rf.mu.Lock()
				logger.Debugf("leader[%v][%v] syncMajority fail.lastLogIndex[%d]\n\n", rf.me, rf.currentTerm, lastLogIndex)
				rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader {
		rf.exitLeaderCh <- true
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// sendRequestVote timeout interface
// if timeout return true, else return false
func (rf *Raft) sendRequestVoteTimeOut(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	sendSuccessCh := make(chan bool, 1000)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		ret := rf.sendRequestVote(serverIndex, args, reply)
		sendSuccessCh <- ret
	}()

	go func() {
		wg.Wait()
		close(sendSuccessCh)
	}()

	select {
	case sendResult := <-sendSuccessCh:
		return !sendResult
	case <-time.After(time.Millisecond * time.Duration(leaderRPCTimeout)):
	}

	return true
}

// sendAppendEntries timeOut interface
// if timeout return true, else return false
func (rf *Raft) sendAppendEntriesTimeOut(serverIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	sendSuccess := make(chan bool, 1000)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		if !rf.sendAppendEntries(serverIndex, args, reply) {
			sendSuccess <- false
		} else {
			sendSuccess <- true
		}
	}()

	go func() {
		wg.Wait()
		close(sendSuccess)
	}()

	select {
	case sendResult := <-sendSuccess:
		return !sendResult
	case <-time.After(time.Millisecond * time.Duration(leaderRPCTimeout)):
	}

	return true
}

func (rf *Raft) election() bool {

	rf.mu.Lock()
	rf.isElectingLeader = true
	rf.currentTerm++
	targetTerm := rf.currentTerm
	rf.votedFor = rf.me
	lastLogIndex := len(rf.log) - 1
	logger.Infof("Server[%d][%v][commitIndex %v] start election. lastLogIndex[%d]\n\n",
		rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex)

	rf.persist()
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

			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				len(rf.log) - 1,
				rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			var sendSuccess bool
			sendSuccess = rf.sendRequestVote(index, &args, &reply)

			//rf.mu.Lock()
			//logger.Debugf("server [%v][%v][commitIndex %v] after RV-RPC to server [%v] success? [%v] reply [%+v]\n\n",
			//	rf.me, rf.currentTerm, rf.commitIndex, index, sendSuccess, reply)
			//rf.mu.Unlock()

			if sendSuccess && reply.VoteGranted {
				voteCh <- true
			} else {
				voteCh <- false
			}
		}()
	}

	majority := len(rf.peers) / 2
	voteGrantedNum := 1 // initial 1, because of self vote
	voteFailNum := 0
	wg.Add(1)
	voteFinalResultCh := make(chan bool, 1000)

	go func() {
		wg.Wait()
		close(voteCh)
		close(voteFinalResultCh)
	}()

	for index := 0; index < len(rf.peers)-1; index++ {
		voteResult := <-voteCh
		if voteResult {
			voteGrantedNum++
			if voteGrantedNum > majority {
				rf.mu.Lock()

				if targetTerm == rf.currentTerm {
					rf.isLeader = true
					// the rf.term should be equal to the args.term when get the majority vote. if not, fail
					// because maybe recv term > this.term at the moment after obtaining the majority of votes
					for i, _ := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					logger.Infof("====================  "+
						"server[%d][%v] election success. commitIndex[%v] lastLogIndex[%v]"+
						"====================\n\n",
						rf.me, rf.currentTerm, rf.commitIndex, len(rf.log)-1)

					hasClear := false
					for !hasClear {
						select {
						case <-rf.exitLeaderCh:
						default:
							hasClear = true
						}
					}
				} else {
					logger.Warnf("leader[%v][%v] get majority vote, but term is not valid.", rf.me, rf.currentTerm)
				}

				rf.mu.Unlock()
				break
			}
		} else {
			voteFailNum++
			if voteFailNum >= majority {
				return false
			}
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.isLeader
}

func (rf *Raft) leaderLoop() {

	go func() {
		rf.handleClientRequestLoop()
	}()

	go func() {
		rf.checkMatchAndUpdateCommitIndexLoop()
	}()

	go func() {
		rf.heartbeatLoop()
	}()

	go func() {
		rf.checkWhetherNeedLogSyncLoop()
	}()

	select {
	case <-rf.exitLeaderCh:
		rf.mu.Lock()

		rf.isLeader = false
		logger.Debugf("leader[%v][%v] exit, clear client msg channel\n", rf.me, rf.currentTerm)
		//rf.leaderExitClear()

		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderExitClear() {
	// clear client request in channel
	hasClear := false
	for !hasClear {
		select {
		case index := <-rf.syncLogCh:
			msg := ApplyMsg{}
			msg.CommandIndex = index
			msg.CommandValid = false
			msg.Command = rf.log[index].Command
			rf.applych <- msg
		default:
			// empty, no data to read
			hasClear = true
		}
	}
}

func (rf *Raft) handleClientRequestLoop() {

	for !rf.killed() {

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		index, ok := <-rf.syncLogCh
		if !ok {
			rf.mu.Lock()
			logger.Debugf("leader[%v][%v] exit handleClientRequestLoop\n\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		// read all logIndex
		readAllIndex := true
		for readAllIndex {
			select {
			case index, ok = <-rf.syncLogCh:
				if !ok {
					rf.mu.Lock()
					logger.Debugf("leader[%v][%v] exit handleClientRequestLoop\n\n", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}
			default:
				readAllIndex = false
			}
		}

		rf.mu.Lock()
		logger.Debugf("leader[%v][%v][commitIndex %v] handleClientRequestLoop start sync index[%v]\n\n",
			rf.me, rf.currentTerm, rf.commitIndex, index)
		termBeforeSendRPC := rf.currentTerm
		rf.mu.Unlock()

		syncResult := rf.syncLogToAllPeers(index)

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		if syncResult && termBeforeSendRPC == rf.currentTerm {
			for commitIndex := rf.commitIndex + 1; commitIndex <= index; commitIndex++ {
				msg := ApplyMsg{}
				msg.CommandIndex = commitIndex
				msg.CommandValid = true
				msg.Command = rf.log[commitIndex].Command
				rf.applych <- msg
			}

			if rf.commitIndex < index {
				rf.commitIndex = index
				rf.lastApplied = rf.commitIndex
			}

			logger.Debugf("leader[%v][%v] syncAllLog [%v] success, update commitIndex[%v]",
				rf.me, rf.currentTerm, index, rf.commitIndex)

		} else {
			msg := ApplyMsg{}
			msg.CommandIndex = index
			msg.CommandValid = false
			msg.Command = rf.log[index].Command
			rf.applych <- msg
			logger.Debugf("leader[%v][%v] syncAllLog [%v] failed",
				rf.me, rf.currentTerm, index)
		}
		rf.mu.Unlock()

	}

}

func (rf *Raft) checkWhetherNeedLogSyncLoop() {
	for !rf.killed() {

		for serverIndex, _ := range rf.peers {
			if serverIndex == rf.me {
				continue
			}

			rf.mu.Lock()

			serverIndex := serverIndex
			nextIndex := rf.nextIndex[serverIndex]

			if nextIndex < len(rf.log) {
				logLen := len(rf.log)
				go func() {
					rf.syncLogToSinglePeer(serverIndex, logLen-1)
				}()
			}
			rf.mu.Unlock()
		}

		time.Sleep(time.Millisecond * time.Duration(100))
	}
}

func (rf *Raft) heartbeatLoop() {

	for !rf.killed() {

		rf.mu.Lock()
		if !rf.isLeader {
			//logger.Debugf("leader[%d][%d] is not leaders, exit keep sending heartbeat\n\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		heartbeatSuccessCh := make(chan bool, 100)
		wg := sync.WaitGroup{}
		wg.Add(len(rf.peers) - 1)

		rf.mu.Unlock()

		for serverIndex, _ := range rf.peers {
			if serverIndex == rf.me {
				continue
			}

			serverIndex := serverIndex
			go func() {
				defer wg.Done()

				rf.mu.Lock()
				leaderLastLogIndex := len(rf.log) - 1

				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					leaderLastLogIndex,
					rf.log[leaderLastLogIndex].Term,
					nil,
					rf.commitIndex,
					0,
				}
				reply := AppendEntriesReply{}
				termBeforeRPC := rf.currentTerm

				rf.mu.Unlock()
				//logger.Debugf(" leader[%d][%d][%v] start AE-RPC(HB) to server[%d] \n\n",
				//	rf.me, rf.currentTerm, rf.commitIndex, serverIndex)

				sendSuccess := rf.sendAppendEntries(serverIndex, &args, &reply)

				rf.mu.Lock()
				if !rf.isLeader || rf.currentTerm != termBeforeRPC {
					//logger.Debugf("leader[%d][%d] is not leaders, exit keep sending heartbeat\n\n", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				rf.mu.Lock()
				if sendSuccess {
					logger.Debugf(" leader[%d][%d][commitIndex %v] after AE-RPC(HB) to server[%d] success? [%v] reply[%+v]\n\n",
						rf.me, rf.currentTerm, rf.commitIndex, serverIndex, sendSuccess, reply)
					if !reply.Success {
						if reply.Term > args.Term {
							// follower has higher term, leader give up.
							rf.isLeader = false
							rf.currentTerm = reply.Term // debug yc
							rf.votedFor = -1
							logger.Debugf(" leader[%d][%d] after AE-RPC(HB) to server[%d][%v], which has "+
								"higher term, leader give up. \n\n",
								rf.me, rf.currentTerm, serverIndex, reply.Term)
							rf.persist()
							//rf.leaderExitClear()
							rf.exitLeaderCh <- true
						}
					} else {
						// success
						logger.Debugf(" leader[%d][%d] logLen[%v] after AE-RPC(HB) to server[%d][%v] success",
							rf.me, rf.currentTerm, len(rf.log), serverIndex, reply.Term)
					}
					heartbeatSuccessCh <- true // important

				} else {
					heartbeatSuccessCh <- false
				}
				rf.mu.Unlock()
			}()
		}

		go func() {
			wg.Wait()
			close(heartbeatSuccessCh)
		}()

		go func() {

			heartbeatSuccessNum := 1
			heartbeatFailedNum := 0
			majority := len(rf.peers) / 2

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				select {
				case success := <-heartbeatSuccessCh:
					if success {
						heartbeatSuccessNum++
						if heartbeatSuccessNum > majority {
							return
						}
					} else {
						heartbeatFailedNum++
						if heartbeatFailedNum > majority {
							rf.mu.Lock()
							if rf.isLeader {
								rf.isLeader = false
								logger.Debugf(" leader[%d][%d][commitIndex %v] give up leader because of heartbeat failed",
									rf.me, rf.currentTerm, rf.commitIndex)
								rf.exitLeaderCh <- true
							}
							rf.mu.Unlock()
							return
						}
					}
				}
			}
		}()

		select {
		case <-time.After(time.Millisecond * time.Duration(leaderHeartbeatInterval)):

		}
	}
}

func (rf *Raft) checkMatchAndUpdateCommitIndexLoop() {

	majority := len(rf.peers) / 2
	// check matchIndex and update commitIndex
	for !rf.killed() {

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			break
		}
		matchIndex := -1

		//logger.Debugf("leader[%d][%d] check matchIndex\n\n", rf.me, rf.currentTerm)

		for index := len(rf.log) - 1; index > 0 && rf.log[index].Term == rf.currentTerm && rf.isLeader; index-- {
			peersMatchNum := 0
			// include self peer

			for _, matchLogIndex := range rf.matchIndex {
				if !rf.isLeader {
					break
				}
				//logger.Debugf("leader[%d][%d] server[%v] target[%v] matchIndex[%v]\n\n", rf.me, rf.currentTerm, i, index, matchLogIndex)
				if matchLogIndex >= index {
					peersMatchNum++
				}
			}
			if peersMatchNum > majority && matchIndex > rf.commitIndex {
				logger.Debugf("leader[%d][%d] update commitIndex to [%v] in heartbeat\n\n", rf.me, rf.currentTerm, index)
				matchIndex = index
				break
			}
		}

		if matchIndex != -1 && rf.isLeader {
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = matchIndex
			rf.lastApplied = rf.commitIndex
			for index := oldCommitIndex + 1; index < rf.commitIndex; index++ {
				msg := ApplyMsg{}
				msg.CommandIndex = index
				msg.CommandValid = true
				msg.Command = rf.log[index].Command
				rf.applych <- msg
			}
			//logger.Debugf("leader[%v][%v] ycommitIndex[%v] \n\n", rf.me, rf.currentTerm, rf.commitIndex)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(100))
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
		electionTimeout := rand.Intn(leaderElectionTimeoutRange) + leaderElectionTimeoutBase
		//logger.Debugf("[ticker] server[%d] sleep[%d]ms\n\n", rf.me, electionTimeout)

		select {
		case <-rf.resetElectionTimeCh:
		case <-time.After(time.Millisecond * time.Duration(electionTimeout)):
			electionSuccess := rf.election()
			if electionSuccess {
				rf.leaderLoop()
			}
		}
	}

	rf.mu.Lock()
	close(rf.syncLogCh)
	//close(rf.exitLeaderCh)
	//close(rf.resetElectionTimeCh)
	rf.mu.Unlock()

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

	atomic.StoreInt32(&rf.dead, 0)
	rf.log = append(rf.log, LogEntry{nil, 0})

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.applych = applyCh
	rf.syncLogCh = make(chan int, 1000) // WARNING！！！！must bigger than total client req a time, or will be dead lock
	rf.resetElectionTimeCh = make(chan bool, 100)
	rf.exitLeaderCh = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			var result strings.Builder
			for index, entry := range rf.log {
				result.WriteString(fmt.Sprintf("[%v,%v,%v]", index, entry.Term, entry.Command))
			}
			logger.Debugf("server [%v][%v][%v] logLen[%v]-> %v\n\n", rf.me, rf.currentTerm, rf.commitIndex, len(rf.log), result.String())
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(1000))
		}
	}()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
