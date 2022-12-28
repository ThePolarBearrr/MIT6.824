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
	"math"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// leader选举相关
	currentTerm    int    // 当前任期
	votedFor       int    // 当前任期投票给了哪个candidate
	logs           []*Log // 收到的来自leader的log
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// 同步日志相关
	commitIndex int // 当前节点已知的所有节点最大提交索引
	lastApplied int // 当前节点已提交了的索引
	applyCh     chan ApplyMsg
	cond        *sync.Cond

	// leader维护各节点日志提交信息
	nextIndex  []int // 预计各个节点下一次要从哪个索引的日志同步
	matchIndex []int // 各个节点日志已经同步了的索引
}

type State uint

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

func stateToString(state State) string {
	if state == FOLLOWER {
		return "follower"
	} else if state == CANDIDATE {
		return "candidate"
	} else if state == LEADER {
		return "leader"
	}
	return "unknown"
}

type Log struct {
	Term int         // 该条log是哪个任期收到的
	Cmd  interface{} // log执行的具体写操作
}

const (
	TIMEOUT_HIGH       float64 = 1000
	TIMEOUT_LOW        float64 = 500
	HEARTBEAT_INTERVAL float64 = 150
	COMMIT_PERIOD      float64 = 10
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 当前任期
	CandidateId  int // 当前节点id
	LastLogIndex int // 最后一条log的index
	LastLogTerm  int // 最后一条log的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// 当满足以下两种情况，同意收到的选举请求
// 1.未同意过其它candidate的选举
// 2.candidate的日志不落后于当前节点
type RequestVoteReply struct {
	// Your data here (2A).
	Granted bool // 是否同意选举
	Term    int  // 当前节点任期
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//defer DPrintf("{Node %v %s term %v} received vote: voteArgs %v voteReply %v",
	//	rf.me, stateToString(rf.state), rf.currentTerm, *args, *reply)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 判断candidate任期是否小于当前节点任期
	if args.Term < rf.currentTerm {
		reply.Granted = false
		reply.Term = rf.currentTerm
		return
	}

	// 收到更高任期的请求
	if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 判断是否有给其它candidate投过票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Granted = false
		reply.Term = rf.currentTerm
		return
	}

	// 判断candidate的日志是否落后于当前节点
	lastIndex := len(rf.logs) - 1
	if rf.logs[lastIndex].Term > args.LastLogTerm {
		reply.Granted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.logs[lastIndex].Term == args.LastLogTerm && lastIndex > args.LastLogIndex {
		reply.Granted = false
		return
	}

	// 投票并重置选举超时时间
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(rf.randomElectionTime())
	reply.Granted = true
	reply.Term = rf.currentTerm
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// DPrintf("Node %v vote request to Node %v is over, ok: %v", rf.me, server, ok)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int // 要从哪个索引开始同步日志
	PrevLogTerm  int // PrevLogIndex索引上日志的term
	Logs         []*Log
	LeaderCommit int // leader commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// DPrintf("Node %v send heartbeat to Node %v is over, ok: %v", rf.me, server, ok)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 检查待复制日志
	curIdx := args.PrevLogIndex + 1
	for i := 0; i < len(args.Logs); i++ {
		if curIdx >= len(rf.logs) {
			break
		}
		if rf.logs[curIdx].Term != args.Logs[i].Term {
			rf.logs = rf.logs[:curIdx]
			break
		}
		curIdx++
	}
	// 复制日志
	if curIdx >= len(rf.logs) {
		rf.logs = append(rf.logs, args.Logs[curIdx-args.PrevLogIndex-1:]...)
	}

	// 更新commit
	DPrintf("node %v %s term %v update commit, leader commit %v, last index %v, len logs is %v",
		rf.me, stateToString(rf.state), rf.currentTerm, args.LeaderCommit, args.PrevLogIndex+len(args.Logs), len(rf.logs))
	if rf.commitIndex < args.LeaderCommit {
		originIdx := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(args.PrevLogIndex+len(args.Logs))))
		if rf.commitIndex > originIdx {
			rf.cond.Broadcast()
		}
	}

	// DPrintf("Node %v receive heartbeat from %v, reset election timer", rf.me, args.LeaderID)
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(rf.randomElectionTime())
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER || rf.killed() {
		return index, term, false
	}

	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, &Log{Term: rf.currentTerm, Cmd: command})
	DPrintf("Node %v %s term %v append log %v", rf.me, stateToString(rf.state), rf.currentTerm, command)

	rf.sendHeartbeat()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.changeState(CANDIDATE)
			if rf.state == CANDIDATE {
				rf.currentTerm++
				rf.sendElection()
			}
			rf.electionTimer.Reset(rf.randomElectionTime())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.sendHeartbeat()
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.logs = make([]*Log, 0)
	rf.logs = append(rf.logs, &Log{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.electionTimer = time.NewTimer(rf.randomElectionTime())
	rf.heartbeatTimer = time.NewTimer(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.logs)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyCommitted()

	return rf
}

func (rf *Raft) randomElectionTime() time.Duration {
	r := rand.New(rand.NewSource(int64(rf.me)))
	return time.Duration(r.Float64()*(TIMEOUT_HIGH-TIMEOUT_LOW)+TIMEOUT_LOW) * time.Millisecond
}

func (rf *Raft) sendElection() {
	voteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	DPrintf("{Node %v %s} starts election with vote request %v", rf.me, stateToString(rf.state), *voteArgs)
	rf.votedFor = rf.me
	grantedCounts := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		// 发送election请求,新启动协程来做，不阻塞ticker
		go func(peer int) {
			voteReply := &RequestVoteReply{}
			DPrintf("Node %v request to Node %v get vote", rf.me, peer)
			if rf.sendRequestVote(peer, voteArgs, voteReply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v %s term %v} receives vote reply from {Node %v}: voteArgs %v voteReply %v",
					rf.me, stateToString(rf.state), rf.currentTerm, peer, *voteArgs, *voteReply)
				// 如果发起选举过程中，任期没有变化且一直是candidate状态才进行处理
				if rf.currentTerm == voteArgs.Term && rf.state == CANDIDATE {
					if voteReply.Granted {
						grantedCounts++
						if grantedCounts > len(rf.peers)/2 {
							rf.changeState(LEADER)
							go rf.commitLog()
							DPrintf("Node %v become to leader!!!", rf.me)
						}
					} else if voteReply.Term > rf.currentTerm { // 如果发现更高任期的节点，直接切换成follower
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, voteReply.Term, rf.currentTerm)
						rf.changeState(FOLLOWER)
						rf.votedFor = -1
						rf.currentTerm = voteReply.Term
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) changeState(expectState State) {
	if expectState == FOLLOWER {
		rf.state = expectState
	} else if expectState == CANDIDATE {
		if rf.state == FOLLOWER {
			rf.state = expectState
		}
	} else if expectState == LEADER {
		if rf.state == CANDIDATE {
			rf.state = expectState
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		appendArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[peer] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[peer]-1].Term,
		}
		appendReply := &AppendEntriesReply{}

		if len(rf.logs) > rf.nextIndex[peer] { // 同步日志
			nextIdx := rf.nextIndex[peer]
			appendArgs.Logs = rf.logs[nextIdx:]

			go func(peer int) {
				DPrintf("Node %v sync log to Node %v appendArgs %v", rf.me, peer, *appendArgs)
				if rf.sendAppendEntries(peer, appendArgs, appendReply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("{Node %v %s term %v} sync log to {Node %v} appendReply %v",
						rf.me, stateToString(rf.state), rf.currentTerm, peer, *appendReply)
					if rf.currentTerm != appendArgs.Term {
						return
					}

					if appendReply.Term > rf.currentTerm {
						rf.changeState(FOLLOWER)
						rf.currentTerm = appendReply.Term
						rf.votedFor = -1
					}

					if appendReply.Success {
						rf.nextIndex[peer] = nextIdx + len(appendArgs.Logs)
						rf.matchIndex[peer] = appendArgs.PrevLogIndex + len(appendArgs.Logs)
					} else {
						rf.nextIndex[peer] = int(math.Max(1.0, float64(rf.nextIndex[peer]-1)))
					}
				}
			}(peer)
		} else { // 发送心跳
			go func(peer int) {
				DPrintf("Node %v send heartbeat to Node %v", rf.me, peer)
				if rf.sendAppendEntries(peer, appendArgs, appendReply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("{Node %v %s term %v} send heart beat to {Node %v} appendArgs %v appendReply %v",
						rf.me, stateToString(rf.state), rf.currentTerm, peer, *appendArgs, *appendReply)
					if rf.currentTerm != appendArgs.Term {
						return
					}

					if appendReply.Term > rf.currentTerm {
						rf.changeState(FOLLOWER)
						rf.currentTerm = appendReply.Term
						rf.votedFor = -1
					}
				}
			}(peer)
		}
	}

	rf.heartbeatTimer.Reset(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
}

func (rf *Raft) commitLog() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		if rf.commitIndex < len(rf.logs)-1 {
			for curIdx := rf.commitIndex + 1; curIdx < len(rf.logs); curIdx++ {
				consensus := 1
				if rf.logs[curIdx].Term == rf.currentTerm {
					for i := range rf.peers {
						if i == rf.me {
							continue
						}

						if rf.matchIndex[i] >= curIdx {
							consensus++
						}
					}

					if consensus*2 > len(rf.peers) {
						rf.commitIndex = curIdx
						DPrintf("Node %v %s term %v commit index %v broadcast cond", rf.me, stateToString(rf.state), rf.currentTerm, rf.commitIndex)
						rf.cond.Broadcast()
					}
				}
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(COMMIT_PERIOD))
	}
}

func (rf *Raft) applyCommitted() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}
		rf.lastApplied++

		cmdIdx := rf.lastApplied
		cmd := rf.logs[cmdIdx].Cmd
		rf.mu.Unlock()

		msg := ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: cmdIdx,
		}
		rf.applyCh <- msg
		DPrintf("[Node %v %s term %v] apply log %d to the service",
			rf.me, stateToString(rf.state), rf.currentTerm, rf.lastApplied)
	}
}
