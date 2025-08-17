package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt             // RPC 客户端
	sck  *shardctrler.ShardCtrler // 控制器
	// You will have to modify this struct.
	mu    sync.Mutex
	cfg   *shardcfg.ShardConfig           // 最近一次的配置缓存
	gclks map[tester.Tgid]*shardgrp.Clerk // 每个 gid 的组内 clerk 缓存

	cid rpc.Tclient // 客户端 ID（唯一标识）
	seq rpc.Treq    // 本次逻辑请求的序列号（从 1 开始）
}

// 使用全局原子计数器生成进程内唯一的 cid。（简单且线程安全）
var globalCID uint64 // 默认 0
func newClientID() rpc.Tclient {
	// +1 避免 0
	return rpc.Tclient(atomic.AddUint64(&globalCID, 1))
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,

		// cfg 在第一次调用时再 Query
		gclks: make(map[tester.Tgid]*shardgrp.Clerk),
	}

	// You'll have to add code here.

	// 生成 cid（避免 unsafe，用时间异或随机）
	ck.cid = newClientID()
	ck.seq = 1

	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
// 模拟用户，对外提供数据读接口
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	backoff := 5 * time.Millisecond
	// 循环发起请求
	for {
		cfg := ck.queryCfg()                // 查询当前配置
		sh := shardcfg.Key2Shard(key)       // 计算 key 所属的分片编号
		gid, srvs, ok := cfg.GidServers(sh) // 查询分片对应的组 ID 和服务器列表

		// 如果查询失败，或是组 ID 为 0，或是服务器列表为空，表示配置还为完成
		// 需要等待一段时间后刷新配置重试（可能是控制器刚发了新配置）
		if !ok || gid == 0 || len(srvs) == 0 {
			// 没有负责人时也要刷新（控制器可能刚发了新 cfg）
			time.Sleep(backoff)
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
			ck.refreshCfg()
			continue
		}

		// 成功读取配置之后，获取组内客户端并发送真正的get请求
		gck := ck.grpClerk(gid, srvs)
		val, ver, err := gck.Get(key)

		switch err {
		case rpc.OK, rpc.ErrNoKey:
			return val, ver, err // 成功获取值或是没有该 key,正常返回
		case rpc.ErrWrongGroup: // key 不属于当前组，刷新配置重试
			// 立刻刷新；cfg.Num 变了会清掉 gclks
			ck.refreshCfg()
		case rpc.ErrWrongLeader, rpc.ErrMaybe:
			// leader 切换/提交不确定等：退避 + 刷新，避免卡在旧 leader
			time.Sleep(backoff)
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
			ck.refreshCfg()
		default:
			// 其它异常：退避 + 刷新
			time.Sleep(backoff)
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
			ck.refreshCfg()
		}
	}
}

// 模拟用户，对外提供数据写接口
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	backoff := 5 * time.Millisecond
	req := ck.nextReq() // 本次逻辑请求固定 req，用于幂等

	for {
		cfg := ck.queryCfg()                // 查询当前配置
		sh := shardcfg.Key2Shard(key)       // 计算 key 所属的分片编号
		gid, srvs, ok := cfg.GidServers(sh) // 查询分片对应的组 ID 和服务器列表

		// 如果查询失败，或是组 ID 为 0，或是服务器列表为空，表示配置还为完成
		// 需要等待一段时间后刷新配置重试（可能是控制器刚发了新配置）
		if !ok || gid == 0 || len(srvs) == 0 {
			time.Sleep(backoff)
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
			ck.refreshCfg()
			continue
		}

		// 成功读取配置之后，获取组内客户端并发送真正的put请求
		gck := ck.grpClerk(gid, srvs)
		err := gck.Put(key, value, version, ck.cid, req)
		switch err {
		case rpc.OK:
			return rpc.OK
		case rpc.ErrVersion:
			// ★ 关键：不要在这里自己 Get 并改 version 重试！
			// 直接把 ErrVersion 交给上层（测试）作为一次独立 CAS 的结果。
			return rpc.ErrVersion
		case rpc.ErrWrongGroup:
			ck.refreshCfg()
		default: // ErrWrongLeader / ErrMaybe / 超时
			time.Sleep(backoff)
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
			ck.refreshCfg()
		}
	}
}

// queryCfg 返回当前的配置，必要时会刷新
func (ck *Clerk) queryCfg() *shardcfg.ShardConfig {
	ck.mu.Lock()
	c := ck.cfg // 获取缓存的配置，可能为nil
	ck.mu.Unlock()

	// 如果缓存的配置不存在，或是第一次调用，则从控制器查询
	if c == nil {
		c = ck.sck.Query()
		ck.mu.Lock()
		ck.cfg = c
		// 第一次拿到 cfg 时也初始化一次（保持语义一致）
		// rpc client缓存了leader hint，
		// 初始化时清空 gclks，保证“配置切换 → 不沿用旧 leader hint”
		ck.gclks = make(map[tester.Tgid]*shardgrp.Clerk)
		ck.mu.Unlock()
	}
	return c
}

// refreshCfg 刷新配置，必要时清空组内 clerk 缓存
func (ck *Clerk) refreshCfg() *shardcfg.ShardConfig {
	c := ck.sck.Query() // 获取最新配置
	ck.mu.Lock()
	old := ck.cfg // 缓存的旧配置
	ck.cfg = c    // 更新缓存的配置
	// 关键：配置号变化 ⇒ 丢弃所有组内 clerk（含 leader hint）
	// 仅当配置号变更时，才清空所有 gclks（因为 leader / 成员很可能变了）
	if old == nil || old.Num != c.Num {
		ck.gclks = make(map[tester.Tgid]*shardgrp.Clerk)
	}
	ck.mu.Unlock()
	return c
}

// grpClerk 返回指定组的 clerk，
func (ck *Clerk) grpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if c, ok := ck.gclks[gid]; ok && sameServers(c.Servers(), servers) {
		return c // 服务器列表没变 → 复用（保留 leader hint）
	}

	// 服务器列表变了 → 新建 clerk
	c := shardgrp.MakeClerk(ck.clnt, servers)
	ck.gclks[gid] = c // 缓存新建的 clerk
	return c
}

// 长度与顺序完全一致才算“同一组服务器列表”
func sameServers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// nextReq 返回下一个逻辑请求的序列号（从 1 开始）
// 每次 Put 开始前拿一个新的逻辑请求号，
// 该号在本次 Put 的整个重试周期内保持不变，便于服务端去重/判重
func (ck *Clerk) nextReq() rpc.Treq {
	ck.mu.Lock()
	ck.seq++
	s := ck.seq
	ck.mu.Unlock()
	return s
}
