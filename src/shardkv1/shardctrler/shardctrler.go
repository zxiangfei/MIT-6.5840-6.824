package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const cfgKey = "shardcfg/current" // 用于标识当前配置的键名

// ShardCtrler for the controller and kv clerk.
// 控制器
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)  // kvsrv 的服务器名
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv) // 创建一个 kvsrv 的 Clerk
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
// Part A： 不需要做任何事
// Part B/C： 会在新控制器重启/接管时调用，用来“恢复状态”
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
// 在系统最开始写入“第一个配置”
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	_ = sck.IKVClerk.Put(cfgKey, cfg.String(), rpc.Tversion(0)) // 把第一个配置写入 kvsrv
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
// 把“当前配置”安全地变更为 new。要做分片迁移并保持线性一致性。标准流程
// 跨分组迁移所有需要变更的 shard，并在全部迁移成功后一次性发布新配置
// 每个配置包含 版本号，每个分片到分组的映射   每个分组到服务器列表的映射
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	target := new.String() // 目标配置的字符串表示

	// 外层无限循环，直到成功发布新配置
	for {
		curStr, _, e := sck.IKVClerk.Get(cfgKey) // 获取当前配置的字符串表示
		if e != rpc.OK {
			return
		}
		if curStr == target { // 如果当前配置已经是目标配置，则直接返回
			return
		}
		old := shardcfg.FromString(curStr) // 从字符串还原当前配置对象

		// 用于标识是否所有分片迁移都完成，开始默认可以把所有迁移都做完，
		// 过程中只要遇到一个 shard 未完成，就把它置为 false
		allDone := true

		// 遍历每个分片，判断当前分片是否需要迁移
		for i := 0; i < shardcfg.NShards; i++ {
			s := shardcfg.Tshid(i)                     // 将i转为当前分片编号
			ogid, ngid := old.Shards[i], new.Shards[i] // 取新旧配置中当前分片所在的组

			// 如果新旧配置中当前分片所在的组相同，则不需要迁移
			if ogid == ngid {
				continue
			}

			// 到此表示该分片需要迁移

			// 取新旧配置中当前分片所在组的服务器列表
			osrvs, okO := old.Groups[ogid]
			nsrvs, okN := new.Groups[ngid]

			// 任何一边不存在、gid 为 0（未分配）、或成员列表为空，都说明当前还不具备迁移条件
			// 标记本轮未完成，跳过该 shard，下轮再试
			if !okO || !okN || ogid == 0 || ngid == 0 || len(osrvs) == 0 || len(nsrvs) == 0 {
				allDone = false
				continue
			}

			// 至此，可以直线当前分片的迁移了

			// 构造访问新旧组的 RPC客户端,用于调用freeze/install/delete RPC
			ock := shardgrp.MakeClerk(sck.clnt, osrvs)
			nck := shardgrp.MakeClerk(sck.clnt, nsrvs)

			// 分片的迁移过程是先冻结旧组的分片，然后安装到新组，最后删除旧组的分片
			// 1) Freeze @ old：只认 OK，WrongGroup 这一轮不算完成
			froze := false // 标记是否成功冻结
			var state []byte
			{
				backoff := 5 * time.Millisecond
				// 用指数回退（5ms 起，封顶 100ms，最多 20 次）进行重试
				for attempt := 0; attempt < 20; attempt++ {
					st, err := ock.FreezeShard(s, new.Num) // 调用旧组的 FreezeShard RPC

					// 如果返回 OK，表示成功冻结
					if err == rpc.OK {
						state = st
						froze = true
						break
					}

					// 如果返回 WrongGroup，表示当前分片不属于旧组，跳出重试
					if err == rpc.ErrWrongGroup {
						break
					}
					time.Sleep(backoff)
					if backoff < 100*time.Millisecond {
						backoff *= 2
					}
				}
			}

			// 如果没有成功冻结，标记本轮未完成，跳过该 shard，下轮再试
			if !froze {
				allDone = false
				continue
			}

			// 2) Install @ new：只有非空 state 且返回 OK 才算成功
			installed := false // 标记是否成功安装

			// 只有当 state 非空（说明 shard 在旧组确实有内容）才去安装
			if len(state) > 0 {
				backoff := 5 * time.Millisecond
				// 用指数回退（5ms 起，封顶 100ms，最多 20 次）进行重试
				for attempt := 0; attempt < 20; attempt++ {
					err := nck.InstallShard(s, state, new.Num) // 调用新组的 InstallShard RPC
					// 如果返回 OK，表示成功安装
					if err == rpc.OK {
						installed = true
						break
					}
					// 如果返回 WrongGroup，表示当前分片不属于新组，跳出重试
					if err == rpc.ErrWrongGroup {
						break
					}
					time.Sleep(backoff)
					if backoff < 100*time.Millisecond {
						backoff *= 2
					}
				}
			}

			// 如果没有成功安装，标记本轮未完成，跳过该 shard，下轮再试
			if !installed {
				allDone = false
				continue
			}

			// 3) Delete @ old：尽力
			// 只要返回 OK 或 WrongGroup，就算成功删除
			for j := 0; j < 5; j++ {
				if err := ock.DeleteShard(s, new.Num); err == rpc.OK || err == rpc.ErrWrongGroup {
					break
				}
				time.Sleep(time.Duration(10*(j+1)) * time.Millisecond)
			}
		}

		// 如果任意 shard 未完成迁移，跳出本轮循环，重试
		// 重试时根据内层逻辑，已经完成的 shard 不会再被迁移
		if !allDone {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		// 全部完成 → CAS 发布 new
		// CAS 是 Compare-And-Set / Compare-And-Swap（比较并设置/交换） 的缩写
		// 只有当当前位置的值（或版本）等于你期望的值时，才原子地写入新值；
		// 否则写入失败。它是一种乐观并发控制原语，避免“你覆盖了别人刚写入的内容”
		// 所有迁移完成后，循环尝试发布新配置(写入server中)
		// cur == target：别人已经抢先发布了，直接返回
		// Put 成功：我们成功发布，返回
		// Put 失败：说明版本冲突（期间有人改了 cfgKey），跳出该小循环，回到最外层大循环重新评估
		for {
			cur, ver, e := sck.IKVClerk.Get(cfgKey)
			if e != rpc.OK {
				return
			}
			if cur == target {
				return
			}
			if sck.IKVClerk.Put(cfgKey, target, ver) == rpc.OK {
				return
			}
			break // 版本冲突 → 外层重来
		}
	}
}

// Return the current configuration
// 返回当前生效的配置
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfgStr, _, err := sck.IKVClerk.Get(cfgKey) // 从 kvsrv 读取当前配置
	if err != rpc.OK || cfgStr == "" {
		return shardcfg.MakeShardConfig() // 如果没有配置，则返回一个空配置
	}
	return shardcfg.FromString(cfgStr) // 从字符串还原配置对象
}
