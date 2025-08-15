/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-15 23:03:25
 * @FilePath: /MIT-6.5840-6.824/src/kvraft1/server.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.Mutex // 互斥锁

	// 简单做法，把value 与 version 存在一个 map 中
	store map[string]struct {
		value   string
		version rpc.Tversion
	}
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
// DoOp 是由rsm的reader 在 日志提交之后 调用，是真正的状态机执行点
// 返回值是一个 值类型 的reply 而不是指针
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		// 处理 Get 请求
		ent, ok := kv.store[args.Key]
		if !ok {
			// 如果 key 不存在，返回 ErrNoKey （Version 可忽略/置0）
			return rpc.GetReply{Err: rpc.ErrNoKey, Value: "", Version: 0}
		}
		return rpc.GetReply{Value: ent.value, Version: ent.version, Err: rpc.OK}

	case rpc.PutArgs:
		// 处理 Put 请求
		ent, ok := kv.store[args.Key]
		// 存在视为版本 0；要想创建/覆盖，客户端必须携带 Version==0
		curVer := rpc.Tversion(0)
		if ok {
			curVer = ent.version
		}
		if args.Version != curVer {
			// 如果版本不匹配，返回 ErrVersion
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		// 版本匹配，执行写入并自增版本
		newVer := curVer + 1
		kv.store[args.Key] = struct {
			value   string
			version rpc.Tversion
		}{
			value:   args.Value,
			version: newVer,
		}
		// 返回成功
		return rpc.PutReply{Err: rpc.OK}

	default:
		// 未知请求类型，返回错误
		return nil

	}

}

// 将当前 KV 状态 + 版本号拷贝到一个“全大写字段”的快照结构里并 GOB 编码
func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 快照里的条目（导出字段，便于 GOB 编码）
	type SnapshotEntry struct {
		Value   string
		Version rpc.Tversion
	}

	// 整体快照（可按需扩展更多字段）
	type Snapshot struct {
		Store map[string]SnapshotEntry // 存储所有键值对的快照
	}

	// 创建快照实例
	snapshot := Snapshot{Store: make(map[string]SnapshotEntry, len(kv.store))}

	// 遍历当前存储，将每个键值对添加到快照中
	for key, ent := range kv.store {
		snapshot.Store[key] = SnapshotEntry{
			Value:   ent.value,
			Version: ent.version,
		}
	}

	// GOB 编码快照
	var buf bytes.Buffer
	encoder := labgob.NewEncoder(&buf)
	if err := encoder.Encode(snapshot); err != nil {
		return nil
	}
	return buf.Bytes()
}

// 从快照字节恢复；data 为空则恢复为初始空状态
func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 先重置为干净的空状态
	kv.store = make(map[string]struct {
		value   string
		version rpc.Tversion
	})

	if len(data) == 0 {
		return // 没有快照，直接返回
	}

	// 解码快照
	// 快照里的条目（导出字段，便于 GOB 编码）
	type SnapshotEntry struct {
		Value   string
		Version rpc.Tversion
	}

	// 整体快照（可按需扩展更多字段）
	type Snapshot struct {
		Store map[string]SnapshotEntry // 存储所有键值对的快照
	}

	var snapshot Snapshot
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&snapshot); err != nil {
		return // 解码失败，直接返回
	}

	// 将快照中的数据恢复到当前状态
	for key, ent := range snapshot.Store {
		kv.store[key] = struct {
			value   string
			version rpc.Tversion
		}{
			value:   ent.Value,
			version: ent.Version,
		}
	}
}

// Get RPC：把参数通过 rsm.Submit() 交给 Raft；
// rsm 返回 (rpc.Err, any)。err!=OK -> 可能不是 leader；
// err==OK -> any 断言为 rpc.GetReply。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err // 通常是 ErrWrongLeader
		return
	}
	r := rep.(rpc.GetReply)
	*reply = r
}

// Put RPC：同理，通过 rsm.Submit() 线性化。
// 服务器端只会返回 OK 或 ErrVersion；
// Clerk 会根据“是不是第一次 RPC”把 ErrVersion 转换成 ErrVersion/ErrMaybe。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err // 通常是 ErrWrongLeader
		return
	}
	r := rep.(rpc.PutReply)
	*reply = r
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	labgob.Register(int(0))
	labgob.Register(int64(0))
	labgob.Register("")

	kv := &KVServer{
		me: me,
		store: make(map[string]struct {
			value   string
			version rpc.Tversion
		}),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
