/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-18 01:02:37
 * @FilePath: /MIT-6.5840-6.824/src/shardkv1/shardgrp/client.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-18 00:51:26
 * @FilePath: /MIT-6.5840-6.824/src/shardkv1/shardgrp/client.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package shardgrp // 本文件实现“面向某个 gid 的组内 RPC 客户端（clerk）”

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt // 底层 RPC 发送器：负责 Call(server, method, args, reply)
	servers []string     // 该 分片组 的所有服务器地址（通常包含 leader + followers）
	// You will have to modify this struct.
	mu     sync.Mutex // 保护 leaderHint
	leader int        // leader hint：最近一次成功交互的服务器下标
}

// 让外部（如上层 shardkv 客户端）获取当前缓存的 servers 列表
func (ck *Clerk) Servers() []string {
	return ck.servers
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0}
	return ck
}

// 对某个下标 i 的服务器发起一次 RPC，并在 d 时长内等待结果；返回 (是否成功, 是否超时)
func (ck *Clerk) callTO(i int, m string, a, r any, d time.Duration) (ok bool, timeout bool) {
	done := make(chan bool, 1) // 带 1 缓冲的通道：避免慢返回时 goroutine send 阻塞
	go func() {
		done <- ck.clnt.Call(ck.servers[i], m, a, r) // 异步发起 RPC 调用
	}()
	select {
	case ok = <-done:
		return ok, false
	case <-time.After(d):
		return false, true
	}
}

// 组内读操作：到某个 gid 的服务器集合中，尝试找到 leader 并读取 key
// 通过 GetArgs 传递 key 等信息
// 返回参数 (value, version, rpc.Err)
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	ck.mu.Lock()
	start := ck.leader // 先从 leader hint 开始，提高命中率
	ck.mu.Unlock()

	timeout := 150 * time.Millisecond        // 单次 RPC 的等待上限
	for i := 0; i < len(ck.servers)*3; i++ { // 最多绕着服务器列表转 3 圈，尽量容错
		si := (start + i) % len(ck.servers)
		var rep rpc.GetReply
		if ok, to := ck.callTO(si, "KVServer.Get", args, &rep, timeout); to || !ok {
			continue // 超时/不可达，试下一个
		}
		switch rep.Err {
		case rpc.OK, rpc.ErrNoKey: // 成功获取值或是没有该 key,正常返回,更新leader
			ck.mu.Lock()
			ck.leader = si
			ck.mu.Unlock()
			return rep.Value, rep.Version, rep.Err
		case rpc.ErrWrongGroup: // key 不属于当前组，刷新配置重试
			return "", 0, rpc.ErrWrongGroup
		case rpc.ErrWrongLeader: // 非leader，试试下一个
			// 试下一个
		default:
			// 其它错误，试下一个
		}
	}
	// 尝试多轮仍未成功，告知上层“也许已提交/也许未提交”，触发上层重试与刷新
	return "", 0, rpc.ErrMaybe
}

// 组内写操作：Put 带 CAS 版本 + 幂等元信息（client, req）
// 通过 PutArgs 传递 key, value, version, client, req 等信息
// 返回参数 rpc.Err
func (ck *Clerk) Put(key string, value string, version rpc.Tversion,
	client rpc.Tclient, req rpc.Treq) rpc.Err {
	// Your code here
	args := &rpc.PutArgs{Key: key, Value: value, Version: version, Client: client, Req: req}
	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	timeout := 150 * time.Millisecond
	for i := 0; i < len(ck.servers); i++ { // 写只转一圈：避免重复提交风险
		si := (start + i) % len(ck.servers)
		var rep rpc.PutReply
		if ok, to := ck.callTO(si, "KVServer.Put", args, &rep, timeout); to || !ok {
			continue // 超时/不可达，试下一个
		}
		switch rep.Err {
		case rpc.OK: // 写成功：记录 leader hint
			ck.mu.Lock()
			ck.leader = si
			ck.mu.Unlock()
			return rpc.OK
		case rpc.ErrVersion: // 版本冲突也是来自 leader：可更新 hint
			ck.mu.Lock()
			ck.leader = si
			ck.mu.Unlock()
			return rpc.ErrVersion // 把 CAS 冲突上抛，交给上层决定是否发起新一轮 CAS
		case rpc.ErrWrongGroup: // 组不负责该 shard：上层需刷新配置
			return rpc.ErrWrongGroup
		case rpc.ErrWrongLeader:
			// 试下一个
		default:
		}
	}
	return rpc.ErrMaybe
}

// 在旧组冻结某个 shard，并导出其状态（快照/序列化字节）
// 传入参数 s 为要冻结的分片编号，num 为配置版本号
// 返回参数 (冻结的分片状态, rpc.Err)
func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := &shardrpc.FreezeShardArgs{Shard: s, Num: num}
	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	timeout := 150 * time.Millisecond
	for i := 0; i < len(ck.servers); i++ {
		si := (start + i) % len(ck.servers)
		var rep shardrpc.FreezeShardReply
		if ok, to := ck.callTO(si, "KVServer.FreezeShard", args, &rep, timeout); to || !ok {
			continue // 超时/不可达，试下一个
		}
		if rep.Err == rpc.OK {
			ck.mu.Lock()
			ck.leader = si
			ck.mu.Unlock()
			return rep.State, rpc.OK
		}
		if rep.Err == rpc.ErrWrongGroup {
			return nil, rpc.ErrWrongGroup
		}
	}
	return nil, rpc.ErrMaybe
}

// 在新组安装某个 shard 的状态（快照/序列化字节）
// 传入参数 s 为要安装的分片编号，state 为冻结的分片状态，num 为配置版本号
// 返回参数 rpc.Err
func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := &shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	timeout := 150 * time.Millisecond
	for i := 0; i < len(ck.servers); i++ {
		si := (start + i) % len(ck.servers)
		var rep shardrpc.InstallShardReply

		if ok, to := ck.callTO(si, "KVServer.InstallShard", args, &rep, timeout); to || !ok {
			continue // 超时/不可达，试下一个
		}
		if rep.Err == rpc.OK {
			ck.mu.Lock()
			ck.leader = si
			ck.mu.Unlock()
			return rpc.OK
		}
		if rep.Err == rpc.ErrWrongGroup {
			return rpc.ErrWrongGroup
		}
	}
	return rpc.ErrMaybe
}

// 删除某个 shard
// 传入参数 s 为要删除的分片编号，num 为配置版本号
// 返回参数 rpc.Err
func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := &shardrpc.DeleteShardArgs{Shard: s, Num: num}
	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	timeout := 150 * time.Millisecond
	for i := 0; i < len(ck.servers); i++ {
		si := (start + i) % len(ck.servers)
		var rep shardrpc.DeleteShardReply

		if ok, to := ck.callTO(si, "KVServer.DeleteShard", args, &rep, timeout); to || !ok {
			continue // 超时/不可达，试下一个
		}
		if rep.Err == rpc.OK {
			ck.mu.Lock()
			ck.leader = si
			ck.mu.Unlock()
			return rpc.OK
		}
		if rep.Err == rpc.ErrWrongGroup {
			return rpc.ErrWrongGroup
		}
	}
	return rpc.ErrMaybe
}
