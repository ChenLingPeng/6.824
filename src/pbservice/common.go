package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	OpPut          = "Put"
	OpAppend       = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op  string
	Me  string
	Seq int64 // for duplicate case
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type PutAppendSyncArgs struct {
	Key   string
	Value string
	Me    string
	Seq   int64
}

type FlushArgs struct {
	KV      map[string]string
	Visited map[string]int64
}
