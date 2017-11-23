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

import "sync"
import (
	"labrpc"
	"log"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
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
)

type LogEntry struct {
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex  //
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// persistent state on all servers
	currentTerm int  // latest term server has seen(init 0)
	voteFor int  // candidateId that received vote in current term (or -1 if none)
	logs []LogEntry  // log entries

	// volatile state on all servers
	commitIndex int  // index of highest log entry known to be commit
	lastApplied int  // index of highest log entry to state machine

	// volatile state on leader
	nextIndex []int  // for each server, index of the next log entry to send to that server
	matchIndex []int  // for each server, index of highest log entry known to be replicated on server

	voteNum int  // received vote number
	state string  // state: FOLLOWER, CANDIDATE or LEADER

	votedCh chan bool  // signal success vote
	appendCh chan bool  // signal receive heartbeat

	electionTimer *time.Timer

	electionTimeOut int64
	heartbeatTimeOut int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
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
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.updateState(FOLLOWER)  // candidate convert to follower
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	} else if rf.voteFor == -1 {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}

	if reply.VoteGranted { rf.votedCh <- true }
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
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.updateState(FOLLOWER)
		reply.Success = true
	} else if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else { reply.Success = true }
	//log.Printf("Term [%d]: server [%d] receive append entries from [%d].\n", rf.currentTerm, rf.me, args.LeaderId)
	rf.appendCh <- true
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

	//preState := rf.state
	switch state {
	case LEADER:
		rf.state = LEADER
	case CANDIDATE:
		rf.state = CANDIDATE
		rf.startElection()  // on conversion to candidate, start election
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.voteFor = -1
	default:
		log.Fatalf("Invalid state %s.", state)
	}
	//log.Printf("Term [%d]: server [%d] transfer from [%s] to [%s]\n", rf.currentTerm, rf.me, preState, rf.state)
}

func (rf *Raft) startElection() {
	/* on conversion to candidate, start election:
		1 increment current term
		2 vote for self
		3 reset election timer
		4 send request vote RPCs to all other servers
	*/
	rf.currentTerm++  // 1

	rf.voteFor = rf.me  // 2
	rf.voteNum = 1

	rf.electionTimer.Reset(rf.getRandElectionTimeOut())  // 3

	// 4
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId == rf.me { continue }
		go func(i int) {
			reply := RequestVoteReply{}
			if rf.state == CANDIDATE && rf.sendRequestVote(i, args, &reply) { // RequestVote RPC
				if reply.VoteGranted {
					rf.voteNum++
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.updateState(FOLLOWER)
					}
				}
			} else {
				//log.Printf("Server [%d] send vote request to [%d] failed.\n", rf.me, i)
			}
		}(serverId)
	}
}

func (rf* Raft) logReplication()  {
	/* Invoke by leader to replicate log entries, alse used as heartbeat.
	*/
	if rf.state != LEADER { return }

	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId == rf.me { continue }
		go func(i int) {
			reply := AppendEntriesReply{}
			if rf.state == LEADER && rf.sendAppendEntries(i, args, &reply) { // Append entries RPC
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.updateState(FOLLOWER)
				}
			} else {
				//log.Printf("Term [%d]: server [%d] send append entries to [%d] failed.\n", rf.currentTerm, rf.me, i)
			}
		}(serverId)
	}
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
	}
}

func (rf *Raft) runAsLeader() {
	if rf.state != LEADER { return }

	rf.logReplication()
	time.Sleep(rf.getHeartbeatTimeOut())
}

func (rf *Raft) runAsCandidate() {
	if rf.state != CANDIDATE { return }

	select {
	case <- rf.appendCh:  // receive from new leader
		rf.updateState(FOLLOWER)
	case <- rf.electionTimer.C:  // election timeout
		rf.electionTimer.Reset(rf.getRandElectionTimeOut())
		rf.startElection()
	default:
		if rf.voteNum > len(rf.peers) / 2 { rf.updateState(LEADER) }  // receive majority vote
	}
}

func (rf *Raft) runAsFollower() {
	if rf.state != FOLLOWER { return }

	select {
	case <-rf.votedCh:  // success vote for a candidate
		rf.electionTimer.Reset(rf.getRandElectionTimeOut())
	case <-rf.appendCh:  // receive append entries (log or heartbeat)
		rf.electionTimer.Reset(rf.getRandElectionTimeOut())
	case <-rf.electionTimer.C:  // time out, update to CANDIDATE, start new election
		rf.updateState(CANDIDATE)
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
	index := -1
	term := -1
	isLeader := true
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
	rf.logs = make([]LogEntry, 0)

	// volatile state on all servers
	rf.commitIndex = -1
	rf.lastApplied = -1

	//volatile state on leaders
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))


	rf.state = FOLLOWER
	rf.votedCh = make(chan bool, len(rf.peers))
	rf.appendCh = make(chan bool, len(rf.peers))

	rf.electionTimeOut = 500
	rf.heartbeatTimeOut = 100

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}



/*------------------------------------*/
func (rf *Raft) getRandElectionTimeOut() time.Duration {
	rand.Seed(int64(rf.me + time.Now().Nanosecond()))  // (rf.me + now.nanosecond) as seed
	return time.Duration(rf.electionTimeOut + int64(rand.Intn(300))) * time.Millisecond
}

func (rf *Raft) getHeartbeatTimeOut() time.Duration {
	return time.Duration(rf.heartbeatTimeOut) * time.Millisecond
}
