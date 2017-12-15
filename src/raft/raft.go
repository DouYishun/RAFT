package raft

import "sync"
import (
	"labrpc"
	"log"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	FOLLOWER = "follower"
	CANDIDATE = "candidate"
	LEADER = "leader"

	ELECTIONTIMEOUT = 500
	HEARTBEATTIMEOUT = 100
)

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex  //
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// persistent state on all servers
	currentTerm int  // latest term server has seen(init 0)
	voteFor int  // candidateId that received vote in current term (or -1 if none)
	log []LogEntry  // log entries

	// volatile state on all servers
	commitIndex int  // index of highest log entry known to be commit
	lastApplied int  // index of highest log entry to state machine

	// volatile state on leader
	nextIndex []int  // for each server, index of the next log entry to send to that server
	matchIndex []int  // for each server, index of highest log entry known to be replicated on server

	//
	voteNum int  // received vote number
	state string  // state: FOLLOWER, CANDIDATE or LEADER

	// channels
	votedCh chan bool  // signal success vote
	appendCh chan bool  // signal receive heartbeat
	applyCh chan ApplyMsg  // signal

	electionTimer *time.Timer

}

func (rf *Raft) GetState() (int, bool) {
	/*
	Return currentTerm and whether this server believes it is the leader.
	*/
	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	return term, isLeader
}

func (rf *Raft) persist() {
	/*
	Save Raft's persistent state to stable storage,
	where it can later be retrieved after a crash and restart.
	see paper's Figure 2 for a description of what should be persistent.
	*/
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.voteFor)
	enc.Encode(rf.log)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	/*
	Restore previously persisted state.
	*/
	if data != nil {
		reader := bytes.NewBuffer(data)
		dec := gob.NewDecoder(reader)
		dec.Decode(&rf.currentTerm)
		dec.Decode(&rf.voteFor)
		dec.Decode(&rf.log)
	}
}

type RequestVoteArgs struct {
	Term int  // candidate's term
	CandidateId int  //candidate requesting vote
	LastLogIndex int  //index of candidate's last log entry(5.4)
	LastLogTerm int  //term of candidate's last log entry(5.4)
}

type RequestVoteReply struct {
	Term int  // currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate receive vote
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	/*
	RequestVote RPC handler.
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	logCheckPass := true

	// check log
	if len(rf.log) > 0 && (rf.log[rf.getLastLogIndex()].Term > args.LastLogTerm ||
		(rf.log[rf.getLastLogIndex()].Term == args.LastLogTerm && rf.getLastLogIndex() > args.LastLogIndex)) {
		logCheckPass = false
	}

	// check current term
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.updateState(FOLLOWER)  // candidate or leader convert to follower
		if logCheckPass {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
		}
	} else if rf.currentTerm == args.Term && logCheckPass {
		if rf.voteFor == -1 {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
		}
	} else if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	}

	if reply.VoteGranted {
		//log.Printf("[%d] success votes for [%d]", rf.me, args.CandidateId)
		rf.persist()
		rf.votedCh <- true
	} else {
		//log.Printf("[%d] reject votes for [%d], log check [%s]", rf.me, args.CandidateId, logCheckPass)
	}
}

type AppendEntriesArgs struct {
	Term int  // leader's term
	LeaderId int  // so follower can redirect clients
	PrevLogIndex int  // index of log entry immediately preceding new ones
	PrevLogTerm int  // term of prevLogIndex entry
	Entries []LogEntry  // log entries to store(empty for heartbeat)
	LeaderCommit int  // leader's commitIndex
}

type AppendEntriesReply struct {
	Term int  // current term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ExpectedNextIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
	AppendEntries RPC handler.
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	} else {
		rf.currentTerm = args.Term
		rf.updateState(FOLLOWER)  // for candidate: discover new term

		if rf.getLastLogIndex() < args.PrevLogIndex { // log length < prevLogIndex
			reply.ExpectedNextIndex =  rf.getLastLogIndex() + 1
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // log match fail
			extraneousTerm := rf.log[args.PrevLogIndex].Term
			i := args.PrevLogIndex
			for ; i >= 0 && rf.log[i].Term == extraneousTerm; i-- { }
			reply.ExpectedNextIndex = i + 1
		} else {
			/* Consistency check pass: 1. append log 2. heartbeat */
			if args.Entries != nil {
				rf.log = rf.log[:args.PrevLogIndex + 1] // delete conflict entries
				rf.log = append(rf.log, args.Entries...) // append new entries
			} else {
				// heartbeat
			}

			/* commit */
			if rf.commitIndex < args.LeaderCommit && rf.getLastLogIndex() >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLog()
			}
			reply.Success = true
			reply.ExpectedNextIndex = rf.getLastLogIndex() + 1
			rf.appendCh <- true   // fix bugs
		}
	}
	rf.persist()
	//log.Printf("Term [%d]: server [%d] receive append entries from [%d].\n", rf.currentTerm, rf.me, args.LeaderId)
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateState(state string) {
	if state == rf.state { return }

	preState := rf.state
	switch state {
	case LEADER:
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1  // init to leader last log index + 1
			rf.matchIndex[i] = 0
		}
		rf.state = LEADER
	case CANDIDATE:
		rf.state = CANDIDATE
		rf.startElection()  // on conversion to candidate, start election
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.voteFor = -1
	default:
		log.Fatalf("In updateState: invalid state %s.", state)
	}
	log.Printf("Term [%d]: server [%d] transfer from [%s] to [%s]\n", rf.currentTerm, rf.me, preState, rf.state)
}

func (rf *Raft) startElection() {
	/* On conversion to candidate, start election:
		1 increment current term
		2 vote for self
		3 reset election timer
		4 send request vote RPCs to all other servers
	*/
	// 1
	rf.currentTerm++

	// 2
	rf.voteFor = rf.me
	rf.voteNum = 1

	// 3
	rf.electionTimer.Reset(rf.getRandElectionTimeOut())

	// 4
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.log[rf.getLastLogIndex()].Term}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me { continue }
		go func(server int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			if rf.state == CANDIDATE && rf.sendRequestVote(server, args, &reply) { // RequestVote RPC
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.VoteGranted {
					rf.voteNum++
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.updateState(FOLLOWER)
					}
				}
			} else {
				//log.Printf("Term [%d]: server [%d] send vote request to [%d] failed.\n", rf.currentTerm, rf.me, server)
			}
		}(i, args)
	}
}

func (rf* Raft) logReplication() {
	/*
	Invoke by leader to replicate log entries, also used as heartbeat.
	*/
	if rf.state != LEADER { return }

	mask := make(map[int]bool, len(rf.peers)) // filter servers which match logs fail
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me || mask[i] { continue }
		rf.nextIndex[i] = min(rf.nextIndex[i], rf.getLastLogIndex() + 1)  // for robustness
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm: rf.log[rf.nextIndex[i] - 1].Term,
			LeaderCommit: rf.commitIndex}
		if rf.getLastLogIndex() >= rf.nextIndex[i] { // last log index > nextIndex: add logs to entry
			args.Entries = rf.log[rf.nextIndex[i]:]
		}
		go func(server int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			if rf.state == LEADER && rf.sendAppendEntries(server, args, &reply) { // Append entries RPC
				rf.logReplicationHandler(server, reply, mask)
			} else {
				//log.Printf("Term [%d]: server [%d] send append entries to [%d] failed.\n", rf.currentTerm, rf.me, i)
			}
		}(i, args)
	}
}

func (rf *Raft) logReplicationHandler(server int, reply AppendEntriesReply, mask map[int]bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER { return }
	rf.nextIndex[server] = reply.ExpectedNextIndex
	if reply.Success {  // update matchIndex
		rf.matchIndex[server] = reply.ExpectedNextIndex - 1
		mask[server] = true
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.updateState(FOLLOWER)
		} else {
			// log inconsistent (include len(server.log) < preLogIndex)
			rf.logReplication()
		}
	}
}

func (rf *Raft) updateCommitIndex() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for ix := rf.getLastLogIndex(); ix > rf.commitIndex; ix-- {
		replicatedCnt := 1
		for server, matched := range rf.matchIndex {
			if server == rf.me { continue }
			if matched >= ix { replicatedCnt++ }
		}
		if replicatedCnt > len(rf.peers) / 2 && rf.log[ix].Term == rf.currentTerm {
			rf.commitIndex = ix
			return true
		}
	}
	return false
}

func (rf *Raft) commitLog() {
	/*
	1. Apply log[lastApplied:commitIndex] to state machine
	2. increment lastApplied
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.commitIndex = min(rf.commitIndex, rf.getLastLogIndex())
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Index: i, Command: rf.log[i].Command}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) run() {
	rf.electionTimer = time.NewTimer(rf.getRandElectionTimeOut())
	for {
		switch rf.state {
		case LEADER:
			rf.runAsLeader()
		case CANDIDATE:
			rf.runAsCandidate()
		case FOLLOWER:
			rf.runAsFollower()
		default:
			log.Fatalf("Invalid state %s.", rf.state)
		}
		//go rf.commitLog()
	}
}

func (rf *Raft) runAsLeader() {
	if rf.state != LEADER { return }

	heartbeatTimeout := time.After(rf.getHeartbeatTimeOut())
	rf.logReplication()
	if rf.state != LEADER { return }

	if rf.updateCommitIndex() {
		rf.commitLog()
	}

	select {
	case <- heartbeatTimeout:
		return
		//something else
	}
}

func (rf *Raft) runAsCandidate() {
	if rf.state != CANDIDATE { return }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	select {
	case <- rf.appendCh:  // discovers current leader
		rf.updateState(FOLLOWER)
	case <- rf.electionTimer.C:  // election timeout
		if rf.state == CANDIDATE {
			rf.startElection()
		}
	default:
		if rf.voteNum > len(rf.peers) / 2 { rf.updateState(LEADER) }  // receive majority vote
	}
}

func (rf *Raft) runAsFollower() {
	if rf.state != FOLLOWER { return }
	rf.electionTimer.Reset(rf.getRandElectionTimeOut())

	select {
	case <-rf.votedCh:  // success vote for a candidate
	case <-rf.appendCh:  // receive append entries (log or heartbeat)

	case <-rf.electionTimer.C:  // time out, update to CANDIDATE, start new election
		rf.mu.Lock()
		rf.updateState(CANDIDATE)
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	term, isLeader = rf.GetState()
	if isLeader {
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Term:term, Index:index, Command:command})
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// persistent state
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 1)

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	//volatile state on leaders
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))


	rf.state = FOLLOWER

	rf.votedCh = make(chan bool, len(rf.peers))
	rf.appendCh = make(chan bool, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	rf.persist()
	return rf
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

/*-----------other func-----------*/

func (rf *Raft)getRandElectionTimeOut() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Int63n(100) + 400)
}
/*
func (rf *Raft) getRandElectionTimeOut() time.Duration {
	rand.Seed(int64(rf.me + time.Now().Nanosecond()))  // (rf.me + now.nanosecond) as seed
	return time.Duration(ELECTIONTIMEOUT + int64(rand.Intn(300))) * time.Millisecond
}*/

func (rf *Raft) getHeartbeatTimeOut() time.Duration {
	return time.Duration(HEARTBEATTIMEOUT) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}