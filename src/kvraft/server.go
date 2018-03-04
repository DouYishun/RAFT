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
	Type string  // type of operation, "Get", "Put" and "Append"
	Key string
	Value string
	Id int64
	ReqCnt int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
	res map[int]chan Op
	lastReq map[int64]int64
}

func (kv *RaftKV) AppendLog(entry Op) bool {
	timeout := time.After(time.Millisecond * 1000)

	ix, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		DPrintf("not a leader\n")
		return false
	}
	kv.mu.Lock()
	ch, ok := kv.res[ix]
	if !ok {
		ch = make(chan Op, 1)
		kv.res[ix] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op == entry {  // noticing that a different request has appeared at the index returned by Start()
			return true
		} else {
			//DPrintf("Lost leadership\n")
			return false
		}
	case <-timeout:
		DPrintf("timeout\n")
		return false
	}
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	/* Get() handler */
	entry := Op{Type:"Get", Key:args.Key, Id:args.Id, ReqCnt:args.ReqCnt}

	if ok := kv.AppendLog(entry); ok {
		reply.WrongLeader = false
		reply.Err = OK

		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.data[args.Key]
	} else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	/* PutAppend() handler */
	entry := Op{Type:args.Op, Key:args.Key, Value:args.Value, Id:args.Id, ReqCnt:args.ReqCnt}

	if ok := kv.AppendLog(entry); ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
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


func (kv *RaftKV) isDuplicate(id int64, reqCnt int64) bool {
	/*
		Check duplicate. Return true for duplicate.
	*/
	if v, ok := kv.lastReq[id]; ok {
		if v >= reqCnt {
			DPrintf("Duplicate\n")
			return true
		} else {
			return false
		}
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
	kv.lastReq = make(map[int64]int64)
	kv.res = make(map[int]chan Op)


	go func() {
		for msg := range kv.applyCh {
			op := msg.Command.(Op)
			kv.mu.Lock()
			if !kv.isDuplicate(op.Id, op.ReqCnt) {
				switch op.Type {
				case "Put":
					kv.data[op.Key] = op.Value
				case "Append":
					kv.data[op.Key] += op.Value
				}
				kv.lastReq[op.Id] = op.ReqCnt
			}
			if _, ok := kv.res[msg.Index]; ok {
				kv.res[msg.Index] <-op
			} else {
				kv.res[msg.Index] = make(chan Op, 1)
			}
			kv.mu.Unlock()
		}
	}()
	return kv
}
