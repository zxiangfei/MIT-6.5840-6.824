package shardcfg

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"runtime/debug"
	"slices"
	"testing"

	tester "6.5840/tester1"
)

type Tshid int // 分片编号类型
type Tnum int  // 配置版本号类型

const (
	NShards  = 12      // The number of shards.  分片的数量
	NumFirst = Tnum(1) // 第一版配置的版本号（从 1 开始）
)

const (
	Gid1 = tester.Tgid(1) // 一个示例/默认组ID（测试可能会用到）
)

// which shard is a key in?
// please use this function,
// and please do not change it.
// 根据 key 计算分片编号
func Key2Shard(key string) Tshid {
	h := fnv.New32a()
	h.Write([]byte(key))
	shard := Tshid(Tshid(h.Sum32()) % NShards)
	return shard
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
// 分片配置元数据结构
type ShardConfig struct {
	Num    Tnum                     // config number  配置版本号
	Shards [NShards]tester.Tgid     // shard -> gid  分片 -> 组ID
	Groups map[tester.Tgid][]string // gid -> servers[]   组ID -> 服务器列表
}

// 构造一个空配置（只初始化 Groups 映射）
func MakeShardConfig() *ShardConfig {
	c := &ShardConfig{
		Groups: make(map[tester.Tgid][]string),
	}
	return c
}

// 把配置转成 JSON 字符串（便于存到 kvsrv 或日志里）
func (cfg *ShardConfig) String() string {
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return string(b)
}

// 从 JSON 字符串还原配置对象
func FromString(s string) *ShardConfig {
	scfg := &ShardConfig{}
	if err := json.Unmarshal([]byte(s), scfg); err != nil {
		log.Fatalf("Unmarshall err %v", err)
	}
	return scfg
}

// 深拷贝一份配置（注意：Groups 的 slice 也要单独拷贝，避免别名）
func (cfg *ShardConfig) Copy() *ShardConfig {
	c := MakeShardConfig()
	c.Num = cfg.Num
	c.Shards = cfg.Shards
	for k, srvs := range cfg.Groups {
		s := make([]string, len(srvs))
		copy(s, srvs)
		c.Groups[k] = s
	}
	return c
}

// mostgroup, mostn, leastgroup, leastn
// 统计每个 group 持有的 shard 数，找出最多/最少者（并返回其计数）
// 返回：最多的组ID、最多数量、最少的组ID、最少数量
func analyze(c *ShardConfig) (tester.Tgid, int, tester.Tgid, int) {
	counts := map[tester.Tgid]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}

	mn := -1
	var mg tester.Tgid = -1
	ln := 257
	var lg tester.Tgid = -1
	// Enforce deterministic ordering, map iteration
	// is randomized in go
	groups := make([]tester.Tgid, len(c.Groups))
	i := 0
	for k := range c.Groups {
		groups[i] = k
		i++
	}
	slices.Sort(groups)
	for _, g := range groups {
		if counts[g] < ln {
			ln = counts[g]
			lg = g
		}
		if counts[g] > mn {
			mn = counts[g]
			mg = g
		}
	}

	return mg, mn, lg, ln
}

// return GID of group with least number of
// assigned shards.
// 返回当前拥有最少 shard 的 group（用于分配新 shard）
func least(c *ShardConfig) tester.Tgid {
	_, _, lg, _ := analyze(c)
	return lg
}

// balance assignment of shards to groups.
// modifies c.
// 对配置进行“负载均衡”调整（就地修改 Shards）
// 目标：让各组持有的 shard 数量尽可能均衡（差值不超过 1）
func (c *ShardConfig) Rebalance() {
	// if no groups, un-assign all shards
	if len(c.Groups) < 1 {
		for s, _ := range c.Shards {
			c.Shards[s] = 0
		}
		return
	}

	// assign all unassigned shards
	for s, g := range c.Shards {
		_, ok := c.Groups[g]
		if ok == false {
			lg := least(c)
			c.Shards[s] = lg
		}
	}

	// move shards from most to least heavily loaded
	for {
		mg, mn, lg, ln := analyze(c)
		if mn < ln+2 {
			break
		}
		// move 1 shard from mg to lg
		for s, g := range c.Shards {
			if g == mg {
				c.Shards[s] = lg
				break
			}
		}
	}
}

// 处理“加入组”（只修改 Groups，不做 Rebalance；版本号自增）
// 返回 true 表示状态变更成功；false 表示请求无效（例如重复 Join）
func (cfg *ShardConfig) Join(servers map[tester.Tgid][]string) bool {
	changed := false
	for gid, servers := range servers {
		_, ok := cfg.Groups[gid]
		if ok {
			log.Printf("re-Join %v", gid)
			return false
		}
		for xgid, xservers := range cfg.Groups {
			for _, s1 := range xservers {
				for _, s2 := range servers {
					if s1 == s2 {
						log.Fatalf("Join(%v) puts server %v in groups %v and %v", gid, s1, xgid, gid)
					}
				}
			}
		}
		// new GID
		// modify cfg to reflect the Join()
		cfg.Groups[gid] = servers
		changed = true
	}
	if changed == false {
		log.Fatalf("Join but no change")
	}
	cfg.Num += 1
	return true
}

// 处理“离开组”（只修改 Groups，不做 Rebalance；版本号自增）
func (cfg *ShardConfig) Leave(gids []tester.Tgid) bool {
	changed := false
	for _, gid := range gids {
		_, ok := cfg.Groups[gid]
		if ok == false {
			// already no GID!
			log.Printf("Leave(%v) but not in config", gid)
			return false
		} else {
			// modify op.Config to reflect the Leave()
			delete(cfg.Groups, gid)
			changed = true
		}
	}
	if changed == false {
		debug.PrintStack()
		log.Fatalf("Leave but no change")
	}
	cfg.Num += 1
	return true
}

// Join + 立即均衡（对 Shards 重新分配）
func (cfg *ShardConfig) JoinBalance(servers map[tester.Tgid][]string) bool {
	if !cfg.Join(servers) {
		return false
	}
	cfg.Rebalance()
	return true
}

// Leave + 立即均衡（把失去归属的 shard 重新分配）
func (cfg *ShardConfig) LeaveBalance(gids []tester.Tgid) bool {
	if !cfg.Leave(gids) {
		return false
	}
	cfg.Rebalance()
	return true
}

// 给定分片，返回 (gid, 该组的服务器列表, 该 gid 是否存在于 Groups)
// ok=false 表示当前分片映射到了一个不存在/已离开的组
func (cfg *ShardConfig) GidServers(sh Tshid) (tester.Tgid, []string, bool) {
	gid := cfg.Shards[sh]
	srvs, ok := cfg.Groups[gid]
	return gid, srvs, ok
}

// 判断某个 gid 是否“持有至少一个 shard”
func (cfg *ShardConfig) IsMember(gid tester.Tgid) bool {
	for _, g := range cfg.Shards {
		if g == gid {
			return true
		}
	}
	return false
}

func (cfg *ShardConfig) CheckConfig(t *testing.T, groups []tester.Tgid) {
	if len(cfg.Groups) != len(groups) {
		fatalf(t, "wanted %v groups, got %v", len(groups), len(cfg.Groups))
	}

	// are the groups as expected?
	for _, g := range groups {
		_, ok := cfg.Groups[g]
		if ok != true {
			fatalf(t, "missing group %v", g)
		}
	}

	// any un-allocated shards?
	if len(groups) > 0 {
		for s, g := range cfg.Shards {
			_, ok := cfg.Groups[g]
			if ok == false {
				fatalf(t, "shard %v -> invalid group %v", s, g)
			}
		}
	}

	// more or less balanced sharding?
	counts := map[tester.Tgid]int{}
	for _, g := range cfg.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range cfg.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		fatalf(t, "max %v too much larger than min %v", max, min)
	}
}

func fatalf(t *testing.T, format string, args ...any) {
	debug.PrintStack()
	t.Fatalf(format, args...)
}
