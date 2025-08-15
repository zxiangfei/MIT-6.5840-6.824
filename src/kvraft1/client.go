package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderHint int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leaderHint: 0}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	n := len(ck.servers)
	start := ck.leaderHint

	for i := 0; ; i++ {
		si := (start + i) % n
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[si], "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				// 成功或不存在都算确定结果
				ck.leaderHint = si
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrWrongLeader:
				// 不是 leader，换下一台
			default:
				// 其他错误（如果有）：继续重试
			}
		}
		// RPC 失败或非确定性错误：短暂休眠后换下一台
		time.Sleep(3 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	n := len(ck.servers)
	start := ck.leaderHint

	firstAttempt := true
	for i := 0; ; i++ {
		si := (start + i) % n
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[si], "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				ck.leaderHint = si
				return rpc.OK
			case rpc.ErrVersion:
				// 规则：第一次 RPC 收到 ErrVersion -> 一定没执行；重发期间收到 -> ErrMaybe
				if firstAttempt {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			case rpc.ErrWrongLeader:
				// 不是 leader，继续探测
			default:
				// 其他错误：继续重试
			}
		}

		// 走到这里，说明要进行“重发”了
		firstAttempt = false
		time.Sleep(3 * time.Millisecond)
	}
}
