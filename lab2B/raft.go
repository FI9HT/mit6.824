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
	LastLogIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

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
	// todo
	// add log replication and safety check

	rf.mu.Lock()
	defer rf.mu.Unlock()

	logger.Debugf("server [%d][%d] receive RequestVote from [%d][%d] "+
		"this.lastLogIndex[%d] this.lastLogTerm[%d] args.LastLogIndex[%d] args.LastLogTerm[%d]\n",
		rf.me, rf.currentTerm, args.CandidateId, args.Term,
		len(rf.log)-1, rf.log[len(rf.log)-1].Term, args.LastLogIndex, args.LastLogTerm)

	//rf.hasReceivedHeartbeat = true
	if args.Term > rf.currentTerm {
		lastIndex := len(rf.log) - 1
		rf.currentTerm = args.Term // important
		if candidateLogIsUpToDate(lastIndex, rf.log[lastIndex].Term, args.LastLogIndex, args.LastLogTerm) {
			// vote
			rf.votedFor = args.CandidateId
			rf.isLeader = false
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			logger.Debugf("server [%d] vote to [%d]\n", rf.me, args.CandidateId)
			return
		}
	}
	// not vote
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	logger.Debugf("server [%d] not vote to [%d]\n", rf.me, args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logger.Debugf("server [%d][%d] => recv AppendEntries from "+
		//"[%d][%d] self[%+v] args [ PreLogIndex[%+v] leaderCommitId [%+v] lastLogIndex[%+v] ] \n",
		"this.lastLogIndex[%v] args[%+v] \n",
		rf.me, rf.currentTerm, len(rf.log)-1, args)

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.hasReceivedHeartbeat = true
		rf.isLeader = false

		reply.Term = rf.currentTerm
		reply.Success = true

		if args.PreLogIndex > len(rf.log)-1 || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
			// PreLogIndex not match
			logger.Debugf("server[%d] recv AppendEntries this.logIndex[%v]= PreLogIndex[%v] not match\n",
				rf.me, len(rf.log)-1, args.PreLogIndex)
			reply.Success = false

		} else if args.Entries != nil {
			// PreLogIndex match, do log replicate
			logger.Debugf("server[%d] append log entris from leader, lastLogIndex[%d]\n",
				rf.me, args.LastLogIndex)

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
			// normal heartbeat, sync commitId
			if rf.commitIndex != args.LeaderCommit {
				oldCommitIndex := rf.commitIndex
				if args.LeaderCommit > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				rf.lastApplied = rf.commitIndex

				for commitIndex := oldCommitIndex + 1; commitIndex <= rf.commitIndex; commitIndex++ {
					msg := ApplyMsg{}
					msg.CommandIndex = commitIndex
					msg.CommandValid = true
					msg.Command = rf.log[commitIndex].Command
					rf.applych <- msg
					logger.Debugf("server[%d] rf.applych <- msg, commidId[%d] \n", rf.me, commitIndex)
				}
			}
		}

	} else {
		reply.Term = rf.currentTerm
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

	if rf.isLeader {
		term = rf.currentTerm
		isLeader = true
		// maybe command has been append, now is no need todo
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		index = len(rf.log) - 1

		rf.mu.Unlock()

	} else {
		term = rf.currentTerm
		isLeader = false
		rf.mu.Unlock()
	}
	logger.Debugf("====================== RET COMMAND[%v] server[%d] isLeader[%t] logIndex[%d] term[%d] "+
		"=====================\n", command, rf.me, isLeader, index, term)
	return index, term, isLeader
}

func (rf *Raft) syncSingleLogStatusToPeer(serverIndex int, lastLogIndex int) bool {
	rf.mu.Lock()
	logger.Debugf("leader[%d][%d] syncSingleLog rf.nextIndex[%v]=[%v] lastLogIndex[%v]\n",
		rf.me, rf.currentTerm, serverIndex, rf.nextIndex[serverIndex], lastLogIndex)

	if rf.nextIndex[serverIndex] > lastLogIndex+1 {
		logger.Debugf("leader[%v][%v] rf.nextIndex[%v]=[%v] > lastLogIndex[%v], no need to sync\n",
			rf.me, rf.currentTerm, serverIndex, rf.nextIndex[serverIndex], lastLogIndex)
		return true
	}
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		rf.nextIndex[serverIndex] - 1,
		rf.log[rf.nextIndex[serverIndex]-1].Term,
		rf.log[rf.nextIndex[serverIndex] : lastLogIndex+1],
		rf.commitIndex,
		lastLogIndex,
	}
	rf.mu.Unlock()

	// Keep send Log Replication AppendEntries, until peer has the same log status or network problem.
	for {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			logger.Debugf("leader[%d][%d] is not leaders, exit syncSingleLog [%d]\n", rf.me, rf.currentTerm, serverIndex)
			break
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}

		sendTimeOut := rf.sendAppendEntriesTimeOut(serverIndex, &args, &reply)

		if !sendTimeOut {
			if !reply.Success {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					// give up leader
					logger.Debugf("leader[%v][%v] recv higher term[%v] from server[%v], give up leader\n",
						rf.me, rf.currentTerm, reply.Term, serverIndex)
					rf.isLeader = false
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					break
				} else {
					// false and resend
					logger.Debugf("leader[%d][%d] preIndex[%d] preTerm[%d] syncSingle [%d] failed and resend.\n",
						rf.me, rf.currentTerm, args.PreLogIndex, args.PreLogTerm, serverIndex)
					args.PreLogIndex--
					if args.PreLogIndex < 0 {
						rf.mu.Unlock()
						break
					}
					args.PreLogTerm = rf.log[args.PreLogIndex].Term
					args.Entries = rf.log[args.PreLogIndex+1 : lastLogIndex+1]
					args.LeaderCommit = rf.commitIndex
					rf.mu.Unlock()
				}
			} else {
				// success
				rf.mu.Lock()
				nextIndex := args.PreLogIndex + len(args.Entries) + 1
				matchIndex := nextIndex - 1
				if nextIndex > rf.nextIndex[serverIndex] {
					rf.nextIndex[serverIndex] = nextIndex
				}
				if matchIndex > rf.matchIndex[serverIndex] {
					rf.matchIndex[serverIndex] = matchIndex
				}
				logger.Debugf("------------------------------------ "+
					"leader[%d][%d] SyncSingle success[%d]. LogIndex[%v] rf.nextIndex[%v]=[%v]"+
					"------------------------------------ \n",
					rf.me, rf.currentTerm, serverIndex, lastLogIndex, serverIndex, rf.nextIndex[serverIndex])
				rf.mu.Unlock()
				return true
			}
		} else {
			// timeout
			logger.Debugf("leader[%d] syncSingleLogStatusToPeer[%d] timeout\n", rf.me, serverIndex)
			break
		}
	}
	return false
}

func (rf *Raft) syncAllLogStatusToPeers(lastLogIndex int) bool {
	logger.Debugf("leader[%d] syncAllLogStatusToPeers. lastLogIndex[%d]\n", rf.me, lastLogIndex)

	syncResultChan := make(chan bool)
	wg := sync.WaitGroup{}

	for serverIndex, _ := range rf.peers {
		if serverIndex == rf.me {
			continue
		}
		wg.Add(1)
		serverIndex := serverIndex
		go func() {
			// todo how to limit go routine in case of permanently running?
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
		select {
		case result := <-syncResultChan:
			if result {
				syncLogStatusNum++
				if syncLogStatusNum > majority {
					logger.Debugf("--------------------------------------------- "+
						"leader[%d] syncAll <Majority> success.lastLogIndex[%d] "+
						"--------------------------------------------- \n", rf.me, lastLogIndex)
					return true
				}
			}
		case <-time.After(time.Duration(80) * time.Millisecond):

		}
	}
	logger.Debugf("leader[%d] syncLogStatusToPeers majority fail\n", rf.me)
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

// if timeout return true, else return false
func (rf *Raft) sendRequestVoteTimeOut(serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	sendSuccessCh := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		//logger.Infof("server[%d][%d] sendRequestVote to [%d]\n", rf.me, rf.currentTerm, serverIndex)
		if !rf.sendRequestVote(serverIndex, args, reply) {
			sendSuccessCh <- false
		} else {
			sendSuccessCh <- true
		}
	}()

	go func() {
		wg.Wait()
		close(sendSuccessCh)
	}()

	select {
	case success := <-sendSuccessCh:
		if success {
			// success
			//logger.Infof("server[%d][%d] sendRequestVote to [%d] success. reply[%t]\n",
			//	rf.me, rf.currentTerm, serverIndex, reply.VoteGranted)
			//if reply.VoteGranted {
			//	return true
			//}
		} else {
			// fail
			//logger.Infof("server[%d][%d] sendRequestVote to [%d] fail\n", rf.me, rf.currentTerm, serverIndex)
		}
		return false
	case <-time.After(time.Millisecond * time.Duration(80)):
		// timeout
		//logger.Infof("server[%d][%d] sendRequestVote to [%d] timeout\n", rf.me, rf.currentTerm, serverIndex)
	}

	return true
}

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
	case success := <-sendSuccess:
		if success {
			//logger.Infof("server[%d] sendAppendEntries to [%d] success. reply[%t]\n",
			//	rf.me, serverIndex, reply.Success)
		} else {
			//logger.Infof("server[%d][%d] sendAppendEntries to [%d] fail\n", rf.me, rf.currentTerm, serverIndex)
		}
		return false
	case <-time.After(time.Millisecond * time.Duration(80)):
		//logger.Infof("server[%d][%d] sendAppendEntries to [%d] timeout\n", rf.me, rf.currentTerm, serverIndex)
	}

	return true
}

func (rf *Raft) election() {

	rf.mu.Lock()
	rf.currentTerm++
	logger.Infof("server[%d] start election. term[%d] lastLogLen[%d]\n", rf.me, rf.currentTerm, len(rf.log)-1)

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		len(rf.log) - 1,
		rf.log[len(rf.log)-1].Term,
	}
	majority := len(rf.peers) / 2
	rf.hasReceivedHeartbeat = false
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
			// todo how to in case of pamenently running
			defer wg.Done()
			reply := RequestVoteReply{}
			voteResultTimeOut := rf.sendRequestVoteTimeOut(index, &args, &reply)
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
	election := false
	// todo timeout by the other way
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		select {
		case voteResult := <-voteCh:
			if voteResult {
				voteGrantedNum++
				if voteGrantedNum > majority {
					rf.mu.Lock()
					if args.Term == rf.currentTerm {
						// the rf.term should be equal to the args.term when get the majority vote. if not, fail
						election = true
						rf.isLeader = true
					} else {
						logger.Debugf("leader[%v][%v] get majority vote, but term is not valid. rf.term[%v]!=args.term[%v]",
							rf.me, rf.currentTerm, rf.currentTerm, args.Term)
					}
					rf.mu.Unlock()
					break
				}
			}
		case <-time.After(time.Duration(80) * time.Millisecond):
		}
	}

	if !election {
		logger.Infof("server[%d] election failed\n", rf.me)
		return
	} else {
		// initial nextIndex and matchIndex
		rf.mu.Lock()
		logger.Infof("================  server[%d][%v] election success ================\n", rf.me, rf.currentTerm)
		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
			if i == rf.me {
				rf.matchIndex[i] = 0
			} else {
				rf.matchIndex[i] = len(rf.log) - 1
			}
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		lastLogIndex := len(rf.log) - 1
		rf.mu.Unlock()
		syncResult := rf.syncAllLogStatusToPeers(lastLogIndex)
		if syncResult {
			rf.mu.Lock()
			oldCommitIndex := rf.commitIndex + 1
			rf.commitIndex = lastLogIndex
			rf.lastApplied = rf.commitIndex
			for begin := oldCommitIndex; begin <= rf.commitIndex; begin++ {
				msg := ApplyMsg{}
				msg.CommandIndex = begin
				msg.CommandValid = true
				msg.Command = rf.log[begin].Command
				rf.applych <- msg
			}

			logger.Debugf("leader[%d][%d] commitId[%d] in election\n", rf.me, rf.currentTerm, rf.commitIndex)
			rf.mu.Unlock()
		}

		go func() {
			rf.keepSyncLog()
		}()

		rf.keepSendHeartbeat()
	}
}

func (rf *Raft) keepSyncLog() {
	for {
		rf.mu.Lock()
		logLen := len(rf.log)
		index := rf.commitIndex + 1
		rf.mu.Unlock()

		syncFailedTimes := 0

		for i := index; i < logLen; i++ {

			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				logger.Debugf("leader[%v] exit keepSyncLog\n", rf.me)
				break
			}
			rf.mu.Unlock()

			syncResult := rf.syncAllLogStatusToPeers(i)

			if syncResult {
				syncFailedTimes = 0
				rf.mu.Lock()
				msg := ApplyMsg{}
				msg.CommandIndex = i
				msg.CommandValid = true
				msg.Command = rf.log[i].Command

				rf.commitIndex = i
				rf.lastApplied = rf.commitIndex
				rf.applych <- msg
				logger.Debugf("leader[%v][%v] SyncLog[%v] success. rf.commitIndex[%v]\n", rf.me, rf.currentTerm, i, rf.commitIndex)
				rf.mu.Unlock()
			} else {
				syncFailedTimes++
				if syncFailedTimes > 2 {
					rf.mu.Lock()
					rf.isLeader = false
					rf.mu.Unlock()
					break
				}
				logger.Debugf("leader[%v][%v] SyncLog[%v] failed, times[%v]\n", rf.me, rf.currentTerm, i, syncFailedTimes)
				// sleep 30ms and retry, if fail 3 times, quit leader
				time.Sleep(time.Millisecond * time.Duration(30))
				i--
				continue
			}
		}

		rf.mu.Lock()
		if !rf.isLeader {
			logger.Debugf("leader[%v] exit keepSyncLog\n", rf.me)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * time.Duration(50))
	}
}

func (rf *Raft) keepSendHeartbeat() {

	// todo other heartbeat more detailed action
	heartbeatFailedTimes := 0
	reachFailedTimesToExit := 3
	var serverHeartbeatFailedTimes []int
	serverHeartbeatFailedTimesMu := sync.Mutex{}
	for _, _ = range rf.peers {
		serverHeartbeatFailedTimes = append(serverHeartbeatFailedTimes, 0)
	}

	for !rf.killed() {

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			logger.Debugf("leader[%d][%d] is not leaders, return to ticker\n", rf.me, rf.currentTerm)
			return
		}
		args := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			len(rf.log) - 1,
			rf.log[len(rf.log)-1].Term,
			nil,
			rf.commitIndex,
			0,
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
				reply := AppendEntriesReply{}
				sendTimeOut := rf.sendAppendEntriesTimeOut(serverIndex, &args, &reply)
				// two condition: false or timeout. here does not distinguish them

				if !sendTimeOut {
					if !reply.Success {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							// follower has higher term, leader give up.
							rf.isLeader = false
							rf.currentTerm = reply.Term // debug yc
							logger.Debugf("[sendAppendEntries] leader[%d][%d]. follower[%d][%d] "+
								"has hisher term, leader give up.\n",
								rf.me, rf.currentTerm, serverIndex, reply.Term)
							rf.mu.Unlock()
							appendEntriesCh <- false
						} else {
							// log replication error
							rf.mu.Unlock()
							serverHeartbeatFailedTimesMu.Lock()
							if serverHeartbeatFailedTimes[serverIndex] > 1 {
								// first failed, maybe because of client request. And is replicating new log.
								rf.mu.Lock()
								logger.Debugf("leader[%d][%d] start syncSingleLog to[%d] in keepHeartbeat\n",
									rf.me, rf.currentTerm, serverIndex)
								rf.mu.Unlock()
								go func() {
									rf.mu.Lock()
									logLen := len(rf.log)
									rf.mu.Unlock()
									rf.syncSingleLogStatusToPeer(serverIndex, logLen-1)
								}()
							}
							serverHeartbeatFailedTimes[serverIndex]++
							serverHeartbeatFailedTimesMu.Unlock()
							appendEntriesCh <- false
						}
					} else {
						// success
						serverHeartbeatFailedTimesMu.Lock()
						serverHeartbeatFailedTimes[serverIndex] = 0
						serverHeartbeatFailedTimesMu.Unlock()
						appendEntriesCh <- true
					}
				} else {
					rf.mu.Lock()
					//logger.Debugf("[sendAppendEntries] leader[%d][%d] sendAppendEntries timeout[%d]\n",
					//	rf.me, rf.currentTerm, serverIndex)
					rf.mu.Unlock()
					appendEntriesCh <- false
				}
			}()
		}

		go func() {
			wg.Wait()
			close(appendEntriesCh)
		}()

		majority := len(rf.peers) / 2
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
			case <-time.After(time.Duration(80) * time.Millisecond):
			}
			if successFlag {
				break
			}
		}

		if !successFlag {
			heartbeatFailedTimes++
			logger.Debugf("leader[%d][%d] heartbeat failed. times[%d]\n", rf.me, rf.currentTerm, heartbeatFailedTimes)
			if heartbeatFailedTimes >= reachFailedTimesToExit {
				rf.mu.Lock()
				rf.isLeader = false
				rf.mu.Unlock()
				logger.Debugf("leader[%d][%d] give up leadership, exit heartbeat\n", rf.me, rf.currentTerm)
			}
		}

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			logger.Debugf("leader[%d][%d] is not leaders, exit heartbeat\n", rf.me, rf.currentTerm)
			break
		}
		rf.mu.Unlock()

		logger.Debugf("leader[%d] sleep 150ms\n", rf.me)
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
	rf.heartbeatTime = 150
	rf.electionTimeout = 400
	rf.electionTimeoutRange = 200
	atomic.StoreInt32(&rf.dead, 0)
	rf.applych = applyCh
	rf.log = append(rf.log, LogEntry{nil, 0})
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	logger.Debugf("server [%d][%d] running...raft[%p]\n", rf.me, rf.currentTerm, peers[rf.me])

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
