/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-20 00:05:56
 * @FilePath: /MIT-6.5840-6.824/src/shardkv1/shardgrp/server.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-17 02:18:13
 * @FilePath: /MIT-6.5840-6.824/src/shardkv1/shardgrp/server.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// shardState 定义了分片的状态
type shardState int

const (
	shNone    shardState = iota // 本组不负责该 shard
	shFrozen                    // 该 shard 已冻结（迁移中）
	shServing                   // 该 shard 正在服务（本组负责）
)

// meta 定义了每个 shard 的元数据
type meta struct {
	Epoch shardcfg.Tnum // 分片的配置版本号
	State shardState    // 分片的状态
}

// 每个 key 的值与版本号（实现 CAS）
type SnapshotEntry struct {
	Value   string
	Version rpc.Tversion
}

// 跨组迁移时携带的一个 shard 的打包数据 + 全局幂等表（确保迁移后写入不会被重复应用）
type shardDump struct {
	Shard shardcfg.Tshid           // 迁移的分片编号
	Epoch shardcfg.Tnum            // 迁移时的配置版本号
	KVs   map[string]SnapshotEntry // 该分片内所有的元数据

	// 幂等去重表，按“客户端 ID → 逻辑请求号 → 历史回复”存已成功的 Put 请求结果：
	// 作用：客户端写入可能因 ErrMaybe/超时 重试。
	// 迁移后如果没有 Dup，同一个 (client, req) 在新组可能会被再次执行，造成重复写。
	// 旧组在冻结时把 整个 dup 表 带过去；新组安装时做并集合并（已有的不覆盖），
	// 这样迁移前已成功的请求在迁移后仍会被识别为“已处理”，保证跨组幂等。
	// 在具体实现中，Put 只有在 CAS 成功时才写入 dup；
	// ErrVersion 不写入，这样客户端可以调整版本后发起新的逻辑请求。
	Dup map[rpc.Tclient]map[rpc.Treq]rpc.PutReply // 用于幂等性
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu sync.Mutex

	// 数据 + 分片状态
	store  map[string]SnapshotEntry // 数据元数据(真实数据的键值对)
	shMeta map[shardcfg.Tshid]meta  // 分片元数据(配置版本号和分片状态)

	dup map[rpc.Tclient]map[rpc.Treq]rpc.PutReply // 用于幂等性
}

// 请求处理过程
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	// 读操作：Get
	case rpc.GetArgs:
		sh := shardcfg.Key2Shard(args.Key) // 计算 key 所属的分片编号
		m := kv.shMeta[sh]                 // 查询该分片的元数据
		// 如果分片不在 Serving 状态,返回ErrWrongGroup，上层会刷新配置后重新发起请求
		if m.State != shServing {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		// 如果分片在 Serving 状态,查询该分片的键值对
		ent, ok := kv.store[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey, Value: "", Version: 0}
		}
		return rpc.GetReply{Value: ent.Value, Version: ent.Version, Err: rpc.OK}

	// 写操作：Put 带 CAS 版本 + 幂等元信息（client, req）
	case rpc.PutArgs:
		sh := shardcfg.Key2Shard(args.Key)
		m := kv.shMeta[sh]
		if m.State != shServing {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

		// 精确去重：命中 (cid, req) → 返回之前的结果
		if mp, ok := kv.dup[args.Client]; ok {
			if rep, ok2 := mp[args.Req]; ok2 {
				return rep
			}
		}

		// CAS
		// 版本号不匹配，则返回 ErrVersion
		cur := kv.store[args.Key].Version
		if cur != args.Version {
			// 失败不写 dup，允许同一 req 调版本后重试
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		// 写入 + 版本自增
		kv.store[args.Key] = SnapshotEntry{Value: args.Value, Version: cur + 1}

		// 记录成功结果到幂等表
		rep := rpc.PutReply{Err: rpc.OK}
		if kv.dup[args.Client] == nil {
			kv.dup[args.Client] = make(map[rpc.Treq]rpc.PutReply)
		}
		kv.dup[args.Client][args.Req] = rep
		return rep

	// 冻结某个 shard，并导出其  跨组迁移时携带的一个 shard 的打包数据 + 全局幂等表
	case shardrpc.FreezeShardArgs:
		s, num := args.Shard, args.Num
		m := kv.shMeta[s]

		// 如果状态是不在本组或者版本号过低，返回 ErrWrongGroup
		if m.State == shNone || m.Epoch > num {
			return shardrpc.FreezeShardReply{State: nil, Num: m.Epoch, Err: rpc.ErrWrongGroup}
		}

		// 提升到 frozen@num（幂等等价）
		// 多个迁移同时进行时，就会出现m.Epoch < num的情况
		if m.Epoch < num || (m.Epoch == num && m.State != shFrozen) {
			kv.shMeta[s] = meta{Epoch: num, State: shFrozen}
		}

		// 打包该 shard 的 KVs + 整表 dup
		dump := shardDump{
			Shard: s,
			Epoch: num,
			KVs:   make(map[string]SnapshotEntry),
			Dup:   make(map[rpc.Tclient]map[rpc.Treq]rpc.PutReply),
		}
		for k, v := range kv.store {
			if shardcfg.Key2Shard(k) == s {
				dump.KVs[k] = v
			}
		}
		for cid, mp := range kv.dup {
			dump.Dup[cid] = make(map[rpc.Treq]rpc.PutReply, len(mp))
			for rq, rep := range mp {
				dump.Dup[cid][rq] = rep
			}
		}

		// 将迁移数据结构体编码之后返回
		var buf bytes.Buffer
		enc := labgob.NewEncoder(&buf)
		_ = enc.Encode(dump)
		return shardrpc.FreezeShardReply{State: buf.Bytes(), Num: num, Err: rpc.OK}

	// 从 跨组迁移时携带的一个 shard 的打包数据 + 全局幂等表 安装某个 shard
	case shardrpc.InstallShardArgs:
		s, num := args.Shard, args.Num
		m := kv.shMeta[s]

		// 幂等快路径：已安装到同一或更高 epoch 直接 OK
		if num < m.Epoch || (num == m.Epoch && m.State == shServing) {
			return shardrpc.InstallShardReply{Err: rpc.OK}
		}

		// 接受空安装：清理该 shard 的旧键，推进到 Serving@num
		if len(args.State) == 0 {
			for k := range kv.store {
				if shardcfg.Key2Shard(k) == s {
					delete(kv.store, k)
				}
			}
			kv.shMeta[s] = meta{Epoch: num, State: shServing}
			return shardrpc.InstallShardReply{Err: rpc.OK}
		}

		// 非空安装：解码并覆盖
		var dump shardDump
		if err := labgob.NewDecoder(bytes.NewReader(args.State)).Decode(&dump); err != nil {
			return shardrpc.InstallShardReply{Err: rpc.ErrMaybe}
		}
		if dump.Shard != s {
			return shardrpc.InstallShardReply{Err: rpc.ErrMaybe}
		}

		// 覆盖该 shard 的键
		for k := range kv.store {
			if shardcfg.Key2Shard(k) == s {
				delete(kv.store, k)
			}
		}
		for k, v := range dump.KVs {
			kv.store[k] = v
		}

		// 合并 dup（并集）
		for cid, mp := range dump.Dup {
			if kv.dup[cid] == nil {
				kv.dup[cid] = make(map[rpc.Treq]rpc.PutReply)
			}
			for rq, rep := range mp {
				if _, ok := kv.dup[cid][rq]; !ok {
					kv.dup[cid][rq] = rep
				}
			}
		}

		kv.shMeta[s] = meta{Epoch: num, State: shServing}
		return shardrpc.InstallShardReply{Err: rpc.OK}

	// 删除某个 shard
	case shardrpc.DeleteShardArgs:
		s, num := args.Shard, args.Num
		m := kv.shMeta[s]
		if num >= m.Epoch {
			for k := range kv.store {
				if shardcfg.Key2Shard(k) == s {
					delete(kv.store, k)
				}
			}
			kv.shMeta[s] = meta{Epoch: num, State: shNone}
		}
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}
	return nil
}

// 快照与恢复（供 RSM 压缩/重启）
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	type SnapMeta struct {
		Epoch shardcfg.Tnum
		State int
	}
	type Snapshot struct {
		Store  map[string]SnapshotEntry
		Shards map[shardcfg.Tshid]SnapMeta
		Dup    map[rpc.Tclient]map[rpc.Treq]rpc.PutReply
	}

	snap := Snapshot{
		Store:  make(map[string]SnapshotEntry, len(kv.store)),
		Shards: make(map[shardcfg.Tshid]SnapMeta, len(kv.shMeta)),
		Dup:    make(map[rpc.Tclient]map[rpc.Treq]rpc.PutReply, len(kv.dup)),
	}
	for k, ent := range kv.store {
		snap.Store[k] = ent
	}
	for sh, m := range kv.shMeta {
		snap.Shards[sh] = SnapMeta{Epoch: m.Epoch, State: int(m.State)}
	}
	for cid, mp := range kv.dup {
		snap.Dup[cid] = make(map[rpc.Treq]rpc.PutReply, len(mp))
		for rq, rep := range mp {
			snap.Dup[cid][rq] = rep
		}
	}

	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	_ = enc.Encode(snap)
	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.store = make(map[string]SnapshotEntry)
	kv.shMeta = make(map[shardcfg.Tshid]meta)
	kv.dup = make(map[rpc.Tclient]map[rpc.Treq]rpc.PutReply)

	if len(data) == 0 {
		return
	}

	type SnapMeta struct {
		Epoch shardcfg.Tnum
		State int
	}
	type Snapshot struct {
		Store  map[string]SnapshotEntry
		Shards map[shardcfg.Tshid]SnapMeta
		Dup    map[rpc.Tclient]map[rpc.Treq]rpc.PutReply
	}

	var snap Snapshot
	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&snap); err != nil {
		return
	}

	for k, e := range snap.Store {
		kv.store[k] = e
	}
	for sh, sm := range snap.Shards {
		kv.shMeta[sh] = meta{Epoch: sm.Epoch, State: shardState(sm.State)}
	}
	for cid, mp := range snap.Dup {
		kv.dup[cid] = make(map[rpc.Treq]rpc.PutReply)
		for rq, rep := range mp {
			kv.dup[cid][rq] = rep
		}
	}
}

// get RPC 服务端方法
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	r := rep.(rpc.GetReply)
	*reply = r
}

// put RPC 服务端方法
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {

	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// 冻结 RPC 服务端方法
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {

	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// 安装 RPC 服务端方法
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {

	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// 删除 RPC 服务端方法
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(shardcfg.Tshid(0)) // 注册分片编号类型

	kv := &KVServer{
		gid:    gid,
		me:     me,
		store:  make(map[string]SnapshotEntry),                  // 键值对存储
		shMeta: make(map[shardcfg.Tshid]meta),                   // 分片元数据
		dup:    make(map[rpc.Tclient]map[rpc.Treq]rpc.PutReply), // 用于幂等性
	}

	// 初始化分片元数据
	for i := 0; i < shardcfg.NShards; i++ {
		kv.shMeta[shardcfg.Tshid(i)] = meta{
			Epoch: shardcfg.NumFirst, // 初始 epoch
			State: shNone,            // 初始状态
		}
	}

	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shMeta[shardcfg.Tshid(i)] = meta{
				Epoch: shardcfg.NumFirst, // 初始 epoch
				State: shServing,         // 初始状态为 Serving
			}
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
