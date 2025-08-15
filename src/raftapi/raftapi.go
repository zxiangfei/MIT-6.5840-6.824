package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// For the tester to indicate to your code that is should cleanup
	// any long-running go routines.
	Kill()
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
//
// In Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool        // true表示 Command 字段包含一个已提交的日志条目，false 表示 Command 字段包含其他类型的消息（如快照）
	Command      interface{} // 提交的应用层命令（比如 kv 的 Put/get）
	CommandIndex int         // 该命令在 Raft 日志里的 索引。上层通常会用它做去重/顺序检查。

	SnapshotValid bool   // 这是一条快照消息（3D 才用）。此时 CommandValid 必须是 false
	Snapshot      []byte // 快照的原始字节（由上层生成/持久化）
	SnapshotTerm  int    // 该快照覆盖到的最后一条日志的任期
	SnapshotIndex int    // 该快照覆盖到的最后一条日志的索引（3D 才用）
}
