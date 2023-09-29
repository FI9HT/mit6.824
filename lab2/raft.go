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
const leaderElectionTimeoutBase = 200
const leaderElectionTimeoutRange = 200
const leaderStopKeepSyncLog = 2
const leaderStopKeepHeartbeat = 3

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
	isLeader             bool
	hasReceivedHeartbeat bool
	syncLogCh            chan int
	applych              chan ApplyMsg
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
		logger.Errorf("Decode error\n\n")
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
	// todo add log replication and safety check
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIndex := len(rf.log) - 1
	logger.Debugf("server [%d][%d][%v] receive RequestVote from [%d][%d] "+
		"LastLog Index[%d] Term[%d]. Arg LastLog Index[%d] Term[%d]\n\n",
		rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term, lastIndex, rf.log[lastIndex].Term, args.LastLogIndex, args.LastLogTerm)

	if args.Term > rf.currentTerm {
		// todo
		//logger.Debugf("server [%d] xxxvote for [%d] rf.votedFor[%v]\n\n", rf.me, args.CandidateId, rf.votedFor)
		rf.currentTerm = args.Term // important here
		if candidateLogIsUpToDate(lastIndex, rf.log[lastIndex].Term, args.LastLogIndex, args.LastLogTerm) {
			// vote
			rf.votedFor = args.CandidateId
			rf.isLeader = false
			rf.hasReceivedHeartbeat = true // if a server vote for a candidate, there is an available leader
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.persist()
			logger.Debugf("server [%d] vote for [%d]\n\n", rf.me, args.CandidateId)
			return
		}
	}
	// not vote
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	logger.Debugf("server [%d] not vote for [%d]\n\n", rf.me, args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex := len(rf.log) - 1

	logger.Debugf("server [%d][%d] commitID [%v] lastLog[%v] => recv AppendEntries from server[%v] lastLogIndex[%v] %+v \n\n",
		rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex, args.LeaderId, args.LastLogIndex, args)

	reply.XLen = len(rf.log)

	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		rf.hasReceivedHeartbeat = true
		rf.isLeader = false

		reply.Term = rf.currentTerm
		reply.Success = true

		if args.PreLogIndex > lastLogIndex || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
			// PreLogIndex or PreLogTerm not match
			reply.Success = false
			// todo fast roll back
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

		} else if args.Entries != nil {
			// PreLogIndex and PreLogTerm match, do Log Replicate
			logger.Debugf("server[%v][%v] (( Append LogEntris )) from leader [%v] lastLogIndex[%d]\n\n",
				rf.me, rf.currentTerm, args.LeaderId, args.LastLogIndex)

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

		} else {
			// normal heartbeat, sync commitId & cut log
			rf.log = rf.log[:args.PreLogIndex+1]

			// todo need args.LeaderCommit > rf.commitIndex?
			if len(rf.log)-1 >= rf.commitIndex && args.LeaderCommit > rf.commitIndex {
				oldCommitIndex := rf.commitIndex
				if args.LeaderCommit > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				logger.Debugf("server[%v][%v] commitIndex=%v", rf.me, rf.currentTerm, rf.commitIndex)
				// update applyId
				rf.lastApplied = rf.commitIndex

				for commitIndex := oldCommitIndex + 1; commitIndex <= rf.commitIndex; commitIndex++ {
					msg := ApplyMsg{}
					msg.CommandIndex = commitIndex
					msg.CommandValid = true
					msg.Command = rf.log[commitIndex].Command
					rf.applych <- msg
					logger.Debugf("server[%v][%v] commidId[%v, %v] \n\n", rf.me, rf.currentTerm, commitIndex, msg.Command)
				}
				//logger.Debugf("server[%v][%v] commitIndex", rf.me, rf.currentTerm)

				//logger.Debugf("server[%v][%v] commidId[%v ~ %v] \n\n",
				//	rf.me, rf.currentTerm, oldCommitIndex+1, rf.commitIndex)
			}
		}

	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	rf.persist()
	logger.Debugf("AE RPC reply %+v\n\n", reply)
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
	if rf.isLeader && !rf.killed() {
		term = rf.currentTerm
		isLeader = true
		//  todo maybe client command has been appended, now is no need
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		rf.matchIndex[rf.me] = len(rf.log) - 1
		index = len(rf.log) - 1

		logger.Debugf("====================== "+
			"RET COMMAND[%v] server[%v][%v][%v] isLeader[%t] logIndex[%d] "+
			"=====================\n\n",
			command, rf.me, rf.currentTerm, rf.commitIndex, isLeader, index)

		go func() {
			rf.syncLogCh <- index
		}()

	} else {
		// only can close in this function and within LOCK, I want to close in ticker().but will cause problems
		if rf.killed() {
			close(rf.syncLogCh)
		}
		term = rf.currentTerm
		isLeader = false
	}
	rf.persist()
	rf.mu.Unlock()

	return index, term, isLeader
}

// sync Leader's log[rf.nextIndex[serverIndex]] ~ log[lastLogIndex] to serverIndex
// return true if LogReplication success
// return false if peer timeout, no leader, wrong preLogIndex
func (rf *Raft) syncSingleLogStatusToPeer(serverIndex int, lastLogIndex int) bool {
	rf.mu.Lock()
	nextLogIndex := rf.nextIndex[serverIndex]

	if nextLogIndex > lastLogIndex+1 {
		// maybe other newer LogReplication has accomplished
		logger.Warnf("leader[%d][%d] syncLog[%v ~ %v] to server[%v] but return \n\n",
			rf.me, rf.currentTerm, nextLogIndex, lastLogIndex, serverIndex)
		rf.mu.Unlock()
		return true
	}

	logger.Debugf("leader[%d][%d] syncLog[%v ~ %v] to server[%v]\n\n",
		rf.me, rf.currentTerm, nextLogIndex, lastLogIndex, serverIndex)

	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		nextLogIndex - 1,
		rf.log[nextLogIndex-1].Term,
		// if nextLogIndex == lastLogIndex+1, Entries is nil and this RPC is for heartbeat
		rf.log[nextLogIndex : lastLogIndex+1],
		rf.commitIndex,
		lastLogIndex,
	}
	rf.mu.Unlock()

	// Keep LogReplication, until peer has the same log or network issue.
	for {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			logger.Debugf("leader[%d][%d] is not leader, exit synceLog [%d]\n\n", rf.me, rf.currentTerm, serverIndex)
			break
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}

		sendTimeOut := rf.sendAppendEntriesTimeOut(serverIndex, &args, &reply)
		logger.Debugf("== leader[%d][%d] sendAppendEntries to follower[%d] timeout? [%v] reply[%+v]\n\n",
			rf.me, rf.currentTerm, serverIndex, sendTimeOut, reply)

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			logger.Debugf("leader[%v][%v] give up leader\n\n", rf.me, rf.currentTerm)
			return false
		}
		rf.mu.Unlock()

		if !sendTimeOut {
			if !reply.Success {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					// give up leader
					logger.Debugf("leader[%v][%v] recv higher term[%v] from server[%v] and give up leader\n\n",
						rf.me, rf.currentTerm, reply.Term, serverIndex)
					rf.isLeader = false
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
					break
				} else {
					// need to do LogReplication
					// another case, send AE RPC, recv and vote VR RPC, recv AE RPC reply, but is no harmful
					logger.Debugf("leader[%d][%d] preIndex[%d] preTerm[%d] syncLog to server[%d] continue to LogReplication.\n\n",
						rf.me, rf.currentTerm, args.PreLogIndex, args.PreLogTerm, serverIndex)
					//args.PreLogIndex--
					//if args.PreLogIndex < 0 {
					//	logger.Warnf("leader[%d][%d] syncLog [%d] error. PreLogIndex < 0\n\n",
					//		rf.me, rf.currentTerm, serverIndex)
					//	rf.mu.Unlock()
					//	break
					//}
					//args.PreLogTerm = rf.log[args.PreLogIndex].Term
					//args.Entries = rf.log[args.PreLogIndex+1 : lastLogIndex+1]
					//args.LeaderCommit = rf.commitIndex
					//rf.mu.Unlock()

					// todo fast roll back
					firstIndex := serverIndex
					if reply.XIndex == 0 {
						firstIndex = reply.XLen
						logger.Debugf("(1) leader[%d][%d] find firstIndex[%d]\n\n", rf.me, rf.currentTerm, firstIndex)
					} else {
						// try to find Xterm
						hasXterm := false
						for index := len(rf.log) - 1; index > 0; index-- {
							//logger.Debugf("leader[%d][%d] search firstIndex[%d], term[%d]\n\n", rf.me, rf.currentTerm, index, rf.log[index].Term)
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
					args.PreLogIndex = firstIndex - 1
					args.PreLogTerm = rf.log[firstIndex-1].Term
					args.Entries = rf.log[firstIndex : lastLogIndex+1]
				}
				rf.mu.Unlock()
			} else {
				// success and update next and match info
				rf.mu.Lock()
				nextIndex := args.PreLogIndex + len(args.Entries) + 1
				matchIndex := nextIndex - 1

				rf.nextIndex[serverIndex] = nextIndex

				if matchIndex > rf.matchIndex[serverIndex] {
					rf.matchIndex[serverIndex] = matchIndex
				}

				logger.Debugf("leader[%d][%d] SyncLog[%v ~ %v] to server[%v] success. \n\n",
					rf.me, rf.currentTerm, args.PreLogIndex+1, lastLogIndex, serverIndex)
				rf.mu.Unlock()
				return true
			}
		} else {
			// timeout
			//logger.Debugf("leader[%d] syncLog[%v ~ %v] to server[%d] timeout\n\n",
			//	rf.me, args.PreLogIndex+1, lastLogIndex, serverIndex)
			break
		}
	}
	return false
}

func (rf *Raft) syncAllLogStatusToPeers(lastLogIndex int) bool {
	logger.Debugf("leader[%d] start syncAllLog. lastLogIndex[%d]\n\n", rf.me, lastLogIndex)

	syncResultChan := make(chan bool)
	wg := sync.WaitGroup{}

	for serverIndex, _ := range rf.peers {
		if serverIndex == rf.me {
			continue
		}
		wg.Add(1)
		serverIndex := serverIndex
		go func() {
			defer wg.Done()
			result := rf.syncSingleLogStatusToPeer(serverIndex, lastLogIndex)
			syncResultChan <- result
		}()
	}

	go func() {
		wg.Wait()
		close(syncResultChan)
	}()

	syncLogStatusNum := 1
	majority := len(rf.peers) / 2

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return false
		}

		rf.mu.Unlock()
		select {
		case result := <-syncResultChan:
			if result {
				syncLogStatusNum++
				if syncLogStatusNum > majority {
					rf.mu.Lock()
					//logger.Debugf("leader[%v][%v] syncMajority success.lastLogIndex[%d]\n\n", rf.me, rf.currentTerm, lastLogIndex)
					rf.mu.Unlock()
					return true
				}
			}
		case <-time.After(time.Duration(500) * time.Millisecond):

		}
	}
	logger.Warnf("leader[%d] syncMajority fail\n\n", rf.me)
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

// sendRequestVote timeout interface
// if timeout return true, else return false
func (rf *Raft) sendRequestVoteTimeOut(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	sendSuccessCh := make(chan bool)
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
		//rf.mu.Lock()
		//logger.Debugf("server [%v][%v] sendRequestVoteTimeOut to server [%v] and sendResult [%v][%v] reply term[%v] granted[%v]\n\n",
		//	rf.me, rf.currentTerm, serverIndex, sendResult, sendResult, reply.Term, reply.VoteGranted)
		//rf.mu.Unlock()
		return !sendResult
	case <-time.After(time.Millisecond * time.Duration(500)):
		//rf.mu.Lock()
		//logger.Debugf("server [%v][%v][%v] sendRequestVoteTimeOut to server [%v], but timeout\n\n",
		//	rf.me, rf.currentTerm, rf.votedFor, serverIndex)
		//rf.mu.Unlock()
	}

	return true
}

// sendAppendEntries timeOut interface
// if timeout return true, else return false
func (rf *Raft) sendAppendEntriesTimeOut(serverIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	sendSuccess := make(chan bool)
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
		rf.mu.Lock()
		//logger.Debugf("server[%v][%v] sendAppendEntries to server[%v] ret[%v] reply[%+v] \n\n",
		//	rf.me, rf.currentTerm, serverIndex, sendResult, reply)
		rf.mu.Unlock()
		return !sendResult
	case <-time.After(time.Millisecond * time.Duration(500)):
		//logger.Debugf("server[%v][%v] sendAppendEntries to server[%v] timeout\n\n",
		//	rf.me, rf.currentTerm, serverIndex)
	}

	return true
}

func (rf *Raft) election() {

	rf.mu.Lock()
	rf.currentTerm++
	targetTerm := rf.currentTerm
	rf.votedFor = rf.me
	lastLogIndex := len(rf.log) - 1
	logger.Infof("Server[%d] start election. term[%d] lastLogIndex[%d]\n\n", rf.me, rf.currentTerm, lastLogIndex)

	majority := len(rf.peers) / 2
	rf.hasReceivedHeartbeat = false
	rf.persist()
	rf.mu.Unlock()

	voteCh := make(chan bool)
	wg := sync.WaitGroup{}

	for serverIndex, _ := range rf.peers {
		if rf.killed() {
			return
		}
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

			voteResultTimeOut := rf.sendRequestVoteTimeOut(index, &args, &reply)

			rf.mu.Lock()
			logger.Debugf("server [%v][%v][%v] sendRequestVoteTimeOut to server [%v] timeout? [%v] reply [%+v]\n\n",
				rf.me, rf.currentTerm, rf.votedFor, index, voteResultTimeOut, reply)
			rf.mu.Unlock()
			if !voteResultTimeOut && reply.VoteGranted {
				voteCh <- true
			} else {
				voteCh <- false
			}
		}()
	}

	go func() {
		wg.Wait()
		close(voteCh)
	}()

	voteGrantedNum := 1 // initial 1, because of self vote
	//election := false
	// todo timeout by the other way
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		select {
		case voteResult := <-voteCh:
			logger.Debugf("server [%v][%v] get vote[%v]\n\n", rf.me, rf.currentTerm, voteResult)
			if voteResult {
				voteGrantedNum++
				if voteGrantedNum > majority {
					rf.mu.Lock()
					if targetTerm == rf.currentTerm {
						// the rf.term should be equal to the args.term when get the majority vote. if not, fail
						// because maybe recv term > this.term at the moment after obtaining the majority of votes
						//election = true
						rf.isLeader = true
						logger.Infof("====================  "+
							"server[%d][%v] election success. commitIndex[%v] lastLogIndex[%v]"+
							"====================\n\n",
							rf.me, rf.currentTerm, rf.commitIndex, len(rf.log)-1)

						for i, _ := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}

					} else {
						logger.Warnf("leader[%v][%v] get majority vote, but term is not valid.", rf.me, rf.currentTerm)
					}
					rf.mu.Unlock()
					break
				}
			}
		case <-time.After(time.Duration(500) * time.Millisecond):
		}

		rf.mu.Lock()
		if rf.isLeader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}

	//if !election {
	//	logger.Infof("server[%d] election failed\n\n", rf.me)
	//	return
	//} else {

	go func() {
		rf.keepSyncLog()
	}()

	rf.keepSendHeartbeat()
	//}
}

func (rf *Raft) keepSyncLog() {

	for {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		index, ok := <-rf.syncLogCh
		if !ok {
			logger.Debugf("leader[%v][%v] exit keepSyncLog\n\n", rf.me, rf.currentTerm)
			return
		}
		logger.Debugf("leader[%v][%v] keepSyncLog start sync index[%v]\n\n", rf.me, rf.currentTerm, index)

		syncFailedTimes := 0
		for {
			syncResult := rf.syncAllLogStatusToPeers(index)

			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if syncResult {
				rf.mu.Lock()
				for commitIndex := rf.commitIndex + 1; commitIndex <= index; commitIndex++ {
					//logger.Debugf("leader[%v][%v] preCommitIndex[%v] lenLog[%v] \n\n",
					//	rf.me, rf.currentTerm, commitIndex, len(rf.log))
					msg := ApplyMsg{}
					msg.CommandIndex = commitIndex
					msg.CommandValid = true
					msg.Command = rf.log[commitIndex].Command
					rf.applych <- msg
				}
				// debug yc
				//for commitIndex := 1; commitIndex <= index; commitIndex++ {
				//	logger.Debugf("[%v, %v] ", commitIndex, rf.log[commitIndex].Command)
				//}
				if rf.commitIndex < index {
					rf.commitIndex = index
					rf.lastApplied = rf.commitIndex
				}
				logger.Debugf("leader[%v][%v] commitIndex[1 ~ %v]", rf.me, rf.currentTerm, rf.commitIndex)
				rf.mu.Unlock()
				break
			} else {
				syncFailedTimes++
				if syncFailedTimes > leaderStopKeepSyncLog {
					// failed to sync log to majority peers, give up leader
					rf.mu.Lock()
					rf.isLeader = false
					rf.mu.Unlock()
					logger.Debugf("leader[%v] give up leader, exit keepSyncLog\n\n", rf.me)
					return
				}
				logger.Debugf("leader[%v][%v] SyncLog[%v] failed, times[%v]\n\n", rf.me, rf.currentTerm, index, syncFailedTimes)
				// sleep 30ms and retry
				time.Sleep(time.Millisecond * time.Duration(30))
			}
		}

	}
	logger.Debugf("end keepSyncLog\n\n")
}

func (rf *Raft) keepSendHeartbeat() {

	// todo other heartbeat more detailed action
	heartbeatFailedTimes := 0

	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader {
			logger.Debugf("leader[%d][%d] is not leaders, exit heartbeat\n\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		appendEntriesCh := make(chan bool)
		wg := sync.WaitGroup{}
		// send heartbeat to all server
		for serverIndex, _ := range rf.peers {

			if serverIndex == rf.me {
				continue
			}
			wg.Add(1)

			serverIndex := serverIndex
			go func() {
				defer wg.Done()
				rf.mu.Lock()
				nextIndex := rf.nextIndex[serverIndex]

				if nextIndex < len(rf.log) {
					// when peer's nextLogIndex < leader.len(log), call SyncLog()
					len := len(rf.log)
					rf.mu.Unlock()
					go func() {
						rf.syncSingleLogStatusToPeer(serverIndex, len-1)
					}()
					rf.mu.Lock()
				}

				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					nextIndex - 1,
					rf.log[nextIndex-1].Term,
					nil,
					rf.commitIndex,
					0,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()

				logger.Debugf(" leader[%d][%d] start AE-RPC to follower[%d] \n\n",
					rf.me, rf.currentTerm, serverIndex)
				sendTimeOut := rf.sendAppendEntriesTimeOut(serverIndex, &args, &reply)
				logger.Debugf(" leader[%d][%d] after AE-RPC to follower[%d] timeout? [%v] reply[%+v]\n\n",
					rf.me, rf.currentTerm, serverIndex, sendTimeOut, reply)

				rf.mu.Lock()
				if !rf.isLeader {
					logger.Debugf("leader[%d][%d] is not leaders, exit heartbeat\n\n", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if !sendTimeOut {
					if !reply.Success {
						rf.mu.Lock()
						if reply.Term > args.Term {
							// follower has higher term, leader give up.
							rf.isLeader = false
							rf.currentTerm = reply.Term // debug yc
							rf.votedFor = -1
							logger.Debugf("[sendAppendEntries] leader[%d][%d]. follower[%d][%d] "+
								"has higher term, leader give up.\n\n",
								rf.me, rf.currentTerm, serverIndex, reply.Term)
							rf.persist()
							rf.mu.Unlock()
							appendEntriesCh <- false
							return
						} else {
							// peer's preLogIndex and preLogTerm not Match
							logger.Warnf("leader[%d][%d] start syncSingleLog to[%d] in keepHeartbeat. [%+v]\n\n",
								rf.me, rf.currentTerm, serverIndex, reply)

							//rf.mu.Unlock()
							// todo fast roll back
							firstIndex := serverIndex
							if reply.XIndex == 0 {
								firstIndex = reply.XLen
								logger.Debugf("(1) leader[%d][%d] find firstIndex[%d]\n\n", rf.me, rf.currentTerm, firstIndex)
							} else {
								// try to find Xterm
								hasXterm := false
								for index := len(rf.log) - 1; index > 0; index-- {
									//logger.Debugf("leader[%d][%d] search firstIndex[%d], term[%d]\n\n", rf.me, rf.currentTerm, index, rf.log[index].Term)
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
							rf.mu.Unlock()

							//go func() {
							//	rf.syncSingleLogStatusToPeer(serverIndex, logLen-1)
							//}()
							//debug
							appendEntriesCh <- true
						}
					} else {
						// success
						appendEntriesCh <- true
					}
				} else {
					appendEntriesCh <- false
				}
			}()
		}

		go func() {
			wg.Wait()
			close(appendEntriesCh)
		}()

		majority := len(rf.peers) / 2

		go func() {

			heartbeatSuccessNum := 1 // already has self heartbeat
			successFlag := false

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				select {
				case sendResult := <-appendEntriesCh:
					if sendResult {
						heartbeatSuccessNum++
						if heartbeatSuccessNum > majority {
							successFlag = true
							break
						}
					}
				case <-time.After(time.Duration(500) * time.Millisecond):
				}
				if successFlag {
					return
				}
			}

			if !successFlag {
				heartbeatFailedTimes++
				logger.Debugf("leader[%d][%d] heartbeat failed. times[%d]\n\n", rf.me, rf.currentTerm, heartbeatFailedTimes)
				if heartbeatFailedTimes >= leaderStopKeepHeartbeat {
					rf.mu.Lock()
					rf.isLeader = false
					rf.mu.Unlock()
					logger.Debugf("leader[%d][%d] give up leadership, exit heartbeat\n\n", rf.me, rf.currentTerm)
					return
				}
			}
		}()

		// check matchIndex and update commitIndex
		rf.mu.Lock()
		matchIndex := -1

		logger.Debugf("leader[%d][%d] check matchIndex and update commitIndex\n\n", rf.me, rf.currentTerm)

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
				logger.Debugf("leader[%d][%d] find matchIndex and commitIndex [%v] in heartbeat\n\n", rf.me, rf.currentTerm, index)
				matchIndex = index
				break
			}
		}

		if matchIndex != -1 && rf.isLeader {
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = matchIndex
			for index := oldCommitIndex + 1; index < rf.commitIndex; index++ {
				msg := ApplyMsg{}
				msg.CommandIndex = index
				msg.CommandValid = true
				msg.Command = rf.log[index].Command
				rf.applych <- msg
			}
			logger.Debugf("leader[%v][%v] commidId[%v] \n\n", rf.me, rf.currentTerm, rf.commitIndex)
		}
		rf.mu.Unlock()

		logger.Debugf("leader[%d] sleep %vms\n\n", rf.me, leaderHeartbeatInterval)
		time.Sleep(time.Millisecond * time.Duration(leaderHeartbeatInterval))
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
		electionTimeout := rand.Intn(leaderElectionTimeoutBase) + leaderElectionTimeoutRange
		logger.Debugf("[ticker] server[%d] sleep[%d]ms\n\n", rf.me, electionTimeout)
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
		// clear channel
		clearNum := 0
		hasClear := false
		for !hasClear {
			select {
			case <-rf.syncLogCh:
				clearNum++
			default:
				// 通道为空，没有数据可读
				hasClear = true
			}
		}
		rf.mu.Unlock()
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
	rf.applych = applyCh
	rf.log = append(rf.log, LogEntry{nil, 0})
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.syncLogCh = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	logger.Debugf("server %v after readPersist term %v votedFor %v lenLog %v running...\n\n",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.log))

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
