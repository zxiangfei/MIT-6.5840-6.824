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

const (
	cfgKey     = "shardcfg/current" // 用于标识当前配置的键名
	nextCfgKey = "shardcfg/next"    // 用于标识下一个配置的键名,在新配置未完成时使用
)

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
	// Your code here.
	// Part A: 不需要做任何事
	// Part B/C: 可以在这里实现恢复状态的逻辑
	// 例如：从 kvsrv 中读取当前配置，或初始化一些内部状态

	// 检查kvserver是否存在为完成的新配置
	curStr, _, e1 := sck.IKVClerk.Get(cfgKey)
	if e1 != rpc.OK || curStr == "" {
		return
	}
	cur := shardcfg.FromString(curStr)

	nextStr, _, e2 := sck.IKVClerk.Get(nextCfgKey)
	if e2 != rpc.OK || nextStr == "" {
		return
	}
	next := shardcfg.FromString(nextStr)

	// 如果 next 配置号比当前大 → 说明有没完成的 reconfig
	if next.Num > cur.Num {
		sck.ChangeConfigTo(next) // 启动一个 新的goroutine 接着进行 reconfig
		return
	}

	// next 过期了，清理一下（防止误触发）
	if next.Num <= cur.Num {
		sck.casClearNext()
	}

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

	// 在迁移开始前将new 配置写入 kvsrv 的 nextCfgKey 键中
	// 这样可以在重启时知道有未完成的迁移
	// nextStr, _, _ := sck.IKVClerk.Get(nextCfgKey) // 获取当前 next 配置
	// if nextStr != target {                        // 如果当前 next 配置不是目标配置，则更新
	// 	// 更新 nextCfgKey 键为目标配置
	// 	sck.casSetNext(target)
	// }

	// 0) 若已经是目标配置，则（只在 next==target 时）清掉 next 并返回
	if curStr, _, e := sck.IKVClerk.Get(cfgKey); e == rpc.OK && curStr == target {
		// 避免误删别人的任务：仅在 next 仍等于我们这份 target 时清理
		sck.clearNextIfEq(target)
		return
	}

	// 1) 需要推进一次 reconfig，先判定/发布 next
	curStr, _, e := sck.IKVClerk.Get(cfgKey)
	if e != rpc.OK {
		return
	}
	cur := shardcfg.FromString(curStr)

	// 仅当我们确实在推进“更高版本”的配置时，才需要占/跟随 next；
	// 若 new.Num <= cur.Num，说明这次调用是冗余的，直接下面循环会快速返回。
	if cur.Num < new.Num {
		ok := sck.tryPublishNextIfEmpty(target)
		if !ok {
			// 有别的 next 正在进行，且目标不同：让出并退出
			return
		}
	}

	// 外层无限循环，直到成功发布新配置
	for {
		curStr, _, e := sck.IKVClerk.Get(cfgKey) // 获取当前配置的字符串表示
		if e != rpc.OK {
			return
		}
		if curStr == target { // 如果当前配置已经是目标配置，则直接返回
			// 已经是目标配置 → 清掉 nextCfgKey
			sck.clearNextIfEq(target)
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
			// if !okO || !okN || ogid == 0 || ngid == 0 || len(osrvs) == 0 || len(nsrvs) == 0 {
			// 	allDone = false
			// 	continue
			// }

			// 改为（只卡新组；旧组可缺席）：
			if !okN || ngid == 0 || len(nsrvs) == 0 {
				allDone = false
				continue
			}

			// 至此，可以直线当前分片的迁移了

			// 构造访问新旧组的 RPC客户端,用于调用freeze/install/delete RPC
			nck := shardgrp.MakeClerk(sck.clnt, nsrvs)

			// 分片的迁移过程是先冻结旧组的分片，然后安装到新组，最后删除旧组的分片
			// 1) Freeze @ old：
			froze := false // 标记是否成功冻结
			var state []byte
			if okO && ogid != 0 && len(osrvs) > 0 {
				// 旧组存在：尝试真正 Freeze
				ock := shardgrp.MakeClerk(sck.clnt, osrvs)
				backoff := 5 * time.Millisecond
				for attempt := 0; attempt < 20; attempt++ {
					st, err := ock.FreezeShard(s, new.Num)
					if err == rpc.OK {
						state = st
						froze = true
						break
					}
					if err == rpc.ErrWrongGroup {
						// 旧组已不再负责该 shard：视为“已冻结且为空”
						froze = true
						state = nil
						break
					}
					time.Sleep(backoff)
					if backoff < 100*time.Millisecond {
						backoff *= 2
					}
				}
			} else {
				// 旧组缺席（ogid==0 / 没在 old.Groups / 成员表空）：直接视为“已冻结 + 空状态”
				froze = true
				state = nil
			}

			// 如果没有成功冻结，标记本轮未完成，跳过该 shard，下轮再试
			if !froze {
				allDone = false
				continue
			}

			// 2) Install @ new：只有非空 state 且返回 OK 才算成功
			installed := false // 标记是否成功安装

			// 无论 state 是否为空，都必须安装来推进配置号
			{
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

			// 3) Delete @ old：仅当旧组存在时“尽力删除”
			if okO && ogid != 0 && len(osrvs) > 0 {
				ock := shardgrp.MakeClerk(sck.clnt, osrvs)
				for j := 0; j < 5; j++ {
					if err := ock.DeleteShard(s, new.Num); err == rpc.OK || err == rpc.ErrWrongGroup {
						break
					}
					time.Sleep(time.Duration(10*(j+1)) * time.Millisecond)
				}
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
				sck.clearNextIfEq(target)
				return
			}
			if sck.IKVClerk.Put(cfgKey, target, ver) == rpc.OK {
				sck.clearNextIfEq(target)
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

// --- 新增 ---
func (sck *ShardCtrler) casSetNext(target string) {
	for {
		cur, ver, _ := sck.IKVClerk.Get(nextCfgKey)
		if cur == target {
			return
		}
		if sck.IKVClerk.Put(nextCfgKey, target, ver) == rpc.OK {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (sck *ShardCtrler) casClearNext() {
	for {
		cur, ver, _ := sck.IKVClerk.Get(nextCfgKey)
		if cur == "" {
			return
		}
		if sck.IKVClerk.Put(nextCfgKey, "", ver) == rpc.OK {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// 仅当 next 为空时尝试写入 target；
// 若 next 已是 target，则允许“跟随”（返回 true）；
// 若 next 是其它值，则认定为被他人占用（返回 false）。
func (sck *ShardCtrler) tryPublishNextIfEmpty(target string) bool {
	for {
		cur, ver, _ := sck.IKVClerk.Get(nextCfgKey)
		switch {
		case cur == target:
			// 别人已经发布了相同的目标：跟随
			return true
		case cur != "":
			// 被他人占用且目标不同：让出
			return false
		default:
			// cur == ""，尝试从空置写入 target
			if sck.IKVClerk.Put(nextCfgKey, target, ver) == rpc.OK {
				return true // 我们拿到发布权
			}
			// 竞争失败，重试一轮读取判定
			time.Sleep(2 * time.Millisecond)
		}
	}
}

// 仅当 next 仍等于 expected 时清空，避免误删他人条目
func (sck *ShardCtrler) clearNextIfEq(expected string) {
	for {
		cur, ver, _ := sck.IKVClerk.Get(nextCfgKey)
		if cur != expected {
			return
		}
		if sck.IKVClerk.Put(nextCfgKey, "", ver) == rpc.OK {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}
