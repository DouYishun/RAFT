package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	Id int64
	ReqId int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
	result map[int]chan Op
	ack map[int64]int64
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {
	ix, _, isLeader := kv.rf.Start(entry)
	if !isLeader { return false }

	timeout := time.After(time.Millisecond * 1000)

	kv.mu.Lock()
	ch, ok := kv.result[ix]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[ix] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		return op == entry
	case <-timeout:
		return false
	}
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{Type:"Get", Key:args.Key, Id:args.Id, ReqId:args.ReqId}

	if ok := kv.AppendEntryToLog(entry); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK

		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.data[args.Key]
		kv.ack[args.Id] = args.ReqId
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{Type:args.Op, Key:args.Key, Value:args.Value, Id:args.Id, ReqId:args.ReqId}
	if ok := kv.AppendEntryToLog(entry); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}


func (kv *RaftKV) Apply(args Op) {
	switch args.Type {
	case "Get":
		kv.data[args.Key] = args.Value
	case "Append":
		kv.data[args.Key] += args.Value
	}
	kv.ack[args.Id] = args.ReqId
}


//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *RaftKV) DuplicateDetector(id int64, reqid int64) bool {
	if v, ok := kv.ack[id]; ok {
		return v >= reqid
	}
	return false
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.result = make(map[int]chan Op)


	go func() {
		for msg := range kv.applyCh {
			op := msg.Command.(Op)
			kv.mu.Lock()
			if !kv.DuplicateDetector(op.Id, op.ReqId) {
				kv.Apply(op)
			}
			if ch, ok := kv.result[msg.Index]; ok {
				select {
				case <- kv.result[msg.Index]:
				default:
				}
				ch <- op
			} else {
				kv.result[msg.Index] = make(chan Op, 1)
			}
			kv.mu.Unlock()
		}
	}()


	return kv
}



































