package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid // 要冻结的分片编号
	Num   shardcfg.Tnum  // 配置版本号
}

type FreezeShardReply struct {
	State []byte        // 冻结的分片状态（序列化字节）
	Num   shardcfg.Tnum // 配置版本号
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}
