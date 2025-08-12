package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

// raft.go 中定义了服务/测试程序与 Raft 交互所需的接口；
// 这里的文件需要实现该接口中声明的方法（见下方 Make、GetState、Start 等）。
// Make() 用来创建一个实现该接口的 Raft 节点。

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Follower  State = iota // Follower 状态：默认状态，等待选举或心跳
	Candidate              // Candidate 状态：候选者，发起选举
	Leader                 // Leader 状态：领导者，处理客户端请求并发送心跳
)

// 日志条目结构体
type LogEntry struct { // 日志条目（包含 command 和 term）
	Command interface{} // 日志条目包含的命令（上层服务的请求）
	Term    int         // 日志条目对应的任期号（用于选举和一致性检查）
	Index   int         // 日志条目的索引（在日志中的位置）
}

// A Go object implementing a single Raft peer.  raft结构体
type Raft struct {
	mu        sync.Mutex          // 互斥锁：保护此节点所有共享状态（Raft 的并发入口很多）
	peers     []*labrpc.ClientEnd // 所有同伴节点的 RPC 端点（含自己）；用于向其它节点发 RPC
	persister *tester.Persister   // 稳定存储句柄：保存/恢复持久化状态（崩溃后可恢复）
	me        int                 // 当前节点在 peers[] 中的索引（唯一 ID）
	dead      int32               // 是否被 Kill() 标记为“停止”；用原子变量读写

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 要在 3A/3B/3C 中添加的状态字段：
	// 参考论文 Figure 2：
	//  - 持久化状态（crash 后需恢复）：currentTerm、votedFor、log[]
	//  - 所有服务器的易失状态：commitIndex、lastApplied
	//  - leader 的易失状态：nextIndex[]、matchIndex[]
	// 注意：字段要导出（首字母大写）才可被 labgob 编码；或者统一在 persist()/readPersist() 中处理。
	currentTerm int        // 当前任期号（从 0 开始）
	votedFor    int        // 当前任期内投票给的候选者（-1 表示未投票）
	log         []LogEntry // Raft 日志条目（每个条目包含 command 和 term）
	commitIndex int        // 已提交的日志条目的索引（>= lastApplied）
	lastApplied int        // 已应用到状态机的日志条目的索引（ <= commitIndex）
	nextIndex   []int      // leader 的易失状态：每个同伴的下一个日志索引（用于追加日志）
	matchIndex  []int      // leader 的易失状态：每个同伴的已匹配日志索引（用于确认日志已提交）

	state           State     // 当前状态（Follower/Candidate/Leader）
	electionTimeout time.Time // 选举超时：用于判断是否需要发起选举

	// 3B
	applyCh chan raftapi.ApplyMsg // 用于发送已提交日志到上层服务的通道（ApplyMsg）

	// 用于通知 applyCh 的条件变量（当有新日志提交时唤醒等待的 goroutine），解决并发应用日志时的乱序的问题
	applyCond *sync.Cond

	// 3D
	LastIncludedIndex int // 快照的索引（表示快照包含的最后日志条目索引）
	LastIncludedTerm  int // 快照的任期号（表示快照包含的最后日志条目的任期）
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { // 返回当前任期，以及该节点是否“自认为”是 leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	// 返回当前任期号和是否为 leader
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() { // 将持久化状态写入稳定存储（崩溃后可恢复）
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)    // 创建一个字节缓冲区
	e := labgob.NewEncoder(w) // 创建一个 labgob 编码器

	// 只编码需要持久化的状态 `currentTerm`（当前任期）、`votedFor`（本任期投给谁）、`log[]`
	// 3D增加 LastIncludedIndex /LastIncludedTerm
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludedIndex) // 编码快照索引
	e.Encode(rf.LastIncludedTerm)  // 编码快照任期号

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot()) // 3C无快照，第二个参数传nil   3D有快照时传快照数据

}

// 增加一个修改快照之后持久化raft状态的方法
// 上面的 persist() 方法只在快照不变的情况下调用，这个方法在更改快照的情况下调用
func (rf *Raft) persistWithSnapshot(snapshot []byte) { // 持久化状态并更新快照
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) { // 从稳定存储恢复之前持久化的状态（在 Make() 中调用）
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data) // 创建一个字节缓冲区
	d := labgob.NewDecoder(r)  // 创建一个 labgob 解码器

	var term int
	var votedFor int
	var log []LogEntry // 用于存储解码后的日志条目

	// 3D
	var LastIncludedIndex int // 用于存储快照索引
	var LastIncludedTerm int  // 用于存储快照任期号

	if d.Decode(&term) != nil || // 解码当前任期
		d.Decode(&votedFor) != nil || // 解码投票给的候选者
		d.Decode(&log) != nil { // 解码日志条目
		return // 如果解码失败，直接返回
	}

	// 3D 第一次启动时还没写过 3D 的字段
	if d.Decode(&LastIncludedIndex) != nil || // 解码快照索引
		d.Decode(&LastIncludedTerm) != nil { // 解码快照任期号
		LastIncludedIndex = 0 // 如果解码失败，设置为默认值
		LastIncludedTerm = 0  // 如果解码失败，设置为默认值
	}

	rf.currentTerm = term  // 设置当前任期
	rf.votedFor = votedFor // 设置投票给的候选者

	// 确保日志非空；若持久化里就是空，也至少保留哨兵
	if len(log) == 0 {
		rf.log = make([]LogEntry, 1)            // 初始化日志为哨兵
		rf.log[0] = LogEntry{Index: 0, Term: 0} // 哨兵条目
	} else {
		rf.log = log // 设置日志条目
	}
	rf.LastIncludedIndex = LastIncludedIndex // 设置快照索引
	rf.LastIncludedTerm = LastIncludedTerm   // 设置快照任期号
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int { // 返回当前持久化状态字节数（测试/调试用）
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 用于裁剪日志和原子保存，上层服务已经生成了一个快照，包含了所有信息直到 index（包括 index）
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock() // 获取锁，确保状态一致性
	defer rf.mu.Unlock()

	// 检查索引是否在快照范围内
	// 如果 index 小于等于以前的快照索引，或者大于当前提交索引，则不需要处理
	if index <= rf.LastIncludedIndex || index > rf.commitIndex {
		return // 不需要处理，直接返回
	}

	// 构造新日志，log[0] 作为哑元 = (index, term)，后面接着原来 >= index 的后缀
	newLog := make([]LogEntry, 1+rf.getLogLastIndex()-index) // 新日志长度为 1 + 原日志中 >= index 的后缀长度
	term := rf.getLogTerm(index)                             // 获取 index 处的任期号
	newLog[0] = LogEntry{Index: index, Term: term}           // 哑元条目
	// 复制原日志中 >= index 的后缀
	if index < rf.getLogLastIndex() {
		copy(newLog[1:], rf.log[rf.getLogIndex(index+1):]) // 复制后缀日志条目
	}

	// 更新 Raft 的日志状态
	rf.log = newLog              // 更新日志为新日志
	rf.LastIncludedIndex = index // 更新快照索引
	rf.LastIncludedTerm = term   // 更新快照任期号

	if rf.commitIndex < index { // 如果当前提交索引小于快照索引
		rf.commitIndex = index
	}
	if rf.lastApplied < index { // 如果当前已应用索引小于快照索引
		rf.lastApplied = index // 更新已应用索引为快照索引
	}

	rf.persistWithSnapshot(snapshot) // 持久化状态并更新快照

	for i := range rf.peers {
		if i == rf.me {
			// 自己的 nextIndex/matchIndex 保持即可；你也可以把 matchIndex[me] 提到最后一条
			continue
		}
		if rf.nextIndex[i] < rf.LastIncludedIndex+1 {
			rf.nextIndex[i] = rf.LastIncludedIndex + 1
		}
		if rf.matchIndex[i] < rf.LastIncludedIndex {
			rf.matchIndex[i] = rf.LastIncludedIndex
		}
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct { // RequestVote RPC 的参数（字段需导出：首字母大写）
	// Your data here (3A, 3B).
	Term         int // 候选者的任期号
	CandidateId  int // 候选者的 ID（本节点的索引）
	LastLogIndex int // 候选者的最后日志条目的索引
	LastLogTerm  int // 候选者的最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct { // RequestVote RPC 的返回值（字段需导出）
	// Your data here (3A).
	Term        int  // 当前任期号（用于更新候选者）
	VoteGranted bool // 是否投票给候选者
}

// AppendEntries RPC
type AppendEntriesArgs struct { // AppendEntries RPC 的参数（用于心跳/日志追加）
	Term         int        // 领导者的任期号
	LeaderId     int        // 领导者的 ID（本节点的索引）
	PrevLogIndex int        // 前一个日志条目的索引（用于一致性检查）
	PrevLogTerm  int        // 前一个日志条目的任期号（用于一致性检查）
	Entries      []LogEntry // 要追加的日志条目（可能为空）
	LeaderCommit int        // 领导者已提交的日志索引（用于更新 follower 的 commitIndex）
}

type AppendEntriesReply struct { // AppendEntries RPC 的返回值
	Term          int  // 当前任期号（用于更新领导者）
	Success       bool // 是否成功追加日志（true 表示成功，false 表示失败）
	ConflictTerm  int  // 冲突的任期号（如果失败时有冲突）,太短时为-1
	ConflictIndex int  // 冲突的索引（如果失败时有冲突），太短时为len(followerLog)
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct { // InstallSnapshot RPC 的参数
	Term              int    // 领导者的任期号
	LeaderId          int    // 领导者的 ID（本节点的索引）
	LastIncludedIndex int    // 快照包含的最后日志条目的索引
	LastIncludedTerm  int    // 快照包含的最后日志条目的任期号
	Snapshot          []byte // 快照数据（包含状态机的完整状态）
}
type InstallSnapshotReply struct { // InstallSnapshot RPC 的返回值
	Term int // 当前任期号（用于更新领导者）
}

// 给follower发送 InstallSnapshot RPC，用于同步日志
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply) // 发送 InstallSnapshot RPC
}

// 处理对方发来的 InstallSnapshot RPC（用于同步日志）
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock() // 获取锁，确保状态一致性
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm // 返回当前任期

	// 检查当前任期是否小于领导者的任期
	if args.Term < rf.currentTerm {
		return // 如果领导者的任期小于当前任期，直接返回
	}

	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term) // 降级为 Follower
	} else if rf.state != Follower {
		rf.state = Follower // 如果是 Candidate 或 Leader，降级为 Follower
	}
	rf.resetElectionTimeout() // 重置选举超时状态

	// 若对方的快照不比我新，直接忽略（不倒退）
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		rf.persist() // 持久化状态（可能更新了任期或状态）
		return       // 忽略旧快照
	}

	// 更新本地日志
	// 以 (LastIncludedIndex, LastIncludedTerm) 为新哑元
	// 清空本地新哑元之前的日志，之后的日志可以保留(如果我本地在该索引处存在同 term 的条目，可以保留后缀，否则丢弃全部日志)
	keep := 0
	if args.LastIncludedIndex <= rf.getLogLastIndex() &&
		rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		keep = rf.getLogLastIndex() - args.LastIncludedIndex
	}

	newLog := make([]LogEntry, 1+keep)                                               // 新日志长度为 1 + 保留的后缀长度
	newLog[0] = LogEntry{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm} // 哑元条目
	if keep > 0 {                                                                    // 如果有保留的后缀
		copy(newLog[1:], rf.log[rf.getLogIndex(args.LastIncludedIndex+1):]) // 复制后缀日志条目
	}

	// 更新 Raft
	rf.log = newLog                               // 更新日志为新日志
	rf.LastIncludedIndex = args.LastIncludedIndex // 更新快照索引
	rf.LastIncludedTerm = args.LastIncludedTerm   // 更新快照任期号

	if rf.commitIndex < args.LastIncludedIndex { // 如果当前提交索引小于快照索引
		rf.commitIndex = args.LastIncludedIndex // 更新提交索引为快照索引
	}
	if rf.lastApplied < args.LastIncludedIndex { // 如果当前已应用索引小于快照索引
		rf.lastApplied = args.LastIncludedIndex // 更新已应用索引为快照索引
	}

	// 持久化状态并更新快照
	rf.persistWithSnapshot(args.Snapshot) // 持久化状态和快照

	// 把快照通过 applyCh 发送给上层服务
	msg := raftapi.ApplyMsg{
		SnapshotValid: true,                   // 标记为快照消息
		Snapshot:      args.Snapshot,          // 快照数据
		SnapshotTerm:  args.LastIncludedTerm,  // 快照任期号
		SnapshotIndex: args.LastIncludedIndex, // 快照索引
	}
	ch := rf.applyCh // 获取 applyCh
	rf.mu.Unlock()   // 释放锁，允许其他操作
	ch <- msg        // 发送到 applyCh
	rf.mu.Lock()     // 重新获取锁，确保状态一致性

}

// 处理对方发来的 AppendEntries RPC（心跳/日志追加）
// 3D 修改 把所有对 rf.log[绝对索引] 的直接访问替换为“带基线”的逻辑，并且处理“PrevLogIndex 落在快照里”的情况
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = 0
	reply.ConflictIndex = 0

	// 检查当前任期是否小于领导者的任期
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term) // 降级为 Follower
	} else if rf.state != Follower {
		rf.state = Follower // 如果是 Candidate 或 Leader，降级为 Follower
	}
	rf.resetElectionTimeout() // 重置选举超时状态

	// 1.prev 在我快照之前：让 leader 快速跳到基线之后
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.ConflictIndex = rf.LastIncludedIndex + 1 // 返回快照之后的第一个索引
		reply.ConflictTerm = -1                        // 没有冲突的任期
		return                                         // 返回，表示追加失败
	}

	// 2.我太短，连 prev 都没有
	if args.PrevLogIndex > rf.getLogLastIndex() {
		// 如果 prevLogIndex 超过了我本地日志的最后索引，说明我太短了
		reply.ConflictIndex = rf.getLogLastIndex() + 1 // 返回当前日志长度的下一个索引
		reply.ConflictTerm = -1                        // 没有冲突的任期
		return                                         // 返回，表示追加失败
	}

	// 3.prev term 不匹配
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// 如果 prevLogIndex 的任期不匹配，说明有冲突
		ct := rf.getLogTerm(args.PrevLogIndex)                     // 获取当前日志条目的任期
		i := args.PrevLogIndex                                     // 从 prevLogIndex 开始查找
		for i > rf.LastIncludedIndex && rf.getLogTerm(i-1) == ct { // 向前查找直到找到不同的任期或到达快照索引
			i-- // 向前移动
		}
		reply.ConflictTerm = ct // 返回冲突的任期
		reply.ConflictIndex = i // 返回冲突任期的第一条索引
		return                  // 返回，表示追加失败
	}

	// 4.前缀完成匹配 可以追加日志条目
	// 前一个日志条目匹配，从 prev 之后逐个对齐；遇到任期冲突就截断本地再整体追加
	next := args.PrevLogIndex + 1

	// 先对齐；遇到任期冲突就截断
	i := 0
	for ; i < len(args.Entries); i++ {
		idx := next + i
		if idx <= rf.getLogLastIndex() {
			if rf.getLogTerm(idx) != args.Entries[i].Term {
				rf.log = rf.log[:rf.getLogIndex(idx)]
				break
			}
		} else {
			break
		}
	}

	// 追加剩余的新条目（从第 i 个未覆盖的 entry 开始）
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}
	rf.persist() // 持久化日志

	// 更新已提交日志索引
	if args.LeaderCommit > rf.commitIndex { // 如果领导者的已提交日志索引大于本地的
		lastNew := rf.getLogLastIndex() // 获取本地日志的最后索引
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
		// rf.applyCommittedLogsLocked() // 应用已提交的日志到状态机
		// 使用专职 goroutine异步应用已提交日志，代替applyCommittedLogsLocked
		if rf.applyCond != nil {
			rf.applyCond.Signal()
		}
	}

	reply.Term = rf.currentTerm // 返回当前任期
	reply.Success = true        // 追加成功
}

// example RequestVote RPC handler.
// 处理对方发来的 RequestVote RPC（选举投票请求）
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查当前任期是否小于候选者的任期以及是否已投票给其他候选者
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm // 返回当前任期
		reply.VoteGranted = false
		return
	}

	// Raft规则，如果当收到带有更大 term 的 RPC，必须降级为 Follower，并且重置投票状态
	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term) // 降级为 Follower
	}

	// 检查候选者的日志是否至少与当前节点的日志一样新
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.currentTerm // 返回当前任期
		reply.VoteGranted = false   // 不投票给候选者
		return
	}

	// 如果满足条件，投票给候选者
	rf.votedFor = args.CandidateId // 记录投票给候选者
	rf.persist()                   // 持久化投票状态
	reply.Term = rf.currentTerm    // 返回当前任期
	reply.VoteGranted = true       // 投票给候选者
	rf.resetElectionTimeout()      // 重置选举超时：因为投票给了候选者，重置选举超时状态
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 发送 RequestVote RPC 到指定服务器
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 上层请求把 command 追加到日志（仅 leader 才能受理）
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock() // 获取锁，确保状态一致性
	defer rf.mu.Unlock()

	if rf.state != Leader { // 如果不是 Leader，直接返回
		return -1, rf.currentTerm, false // 返回 -1 表示未能追加，返回当前任期和非 Leader 状态
	}
	// 如果是 Leader，创建新的日志条目
	index := rf.getLogLastIndex() + 1 // 新日志条目的索引为当前日志长度
	entry := LogEntry{
		Command: command,        // 设置命令
		Term:    rf.currentTerm, // 设置当前任期
		Index:   index,          // 设置索引
	}
	rf.log = append(rf.log, entry) // 将新日志条目追加到日志中
	rf.persist()                   // 持久化日志

	// Leader 需要更新 nextIndex 和 matchIndex
	rf.nextIndex[rf.me] = index + 1 // 更新自己的 nextIndex
	rf.matchIndex[rf.me] = index    // 更新自己的 matchIndex

	// 立刻触发一次心跳，通知其他节点有新日志
	go rf.broadcastHeartbeat(rf.currentTerm) // 异步发送心跳

	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() { // 测试结束时会调用，标记该节点应当停止后台协程
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	if rf.applyCond != nil {
		rf.applyCond.Broadcast() // 唤醒等待中的 applyLoop，及时退出
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { // 查询是否已被 Kill() 标记
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() { // 后台“心跳/选举节拍”协程：定期检查是否需要发起选举或发送心跳
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock() // 获取锁，确保状态一致性
		// 如果不是 Leader，且当前时间超过选举超时，则发起选举
		if rf.state != Leader && time.Now().After(rf.electionTimeout) {
			rf.startElectionLocked()
		}
		rf.mu.Unlock() // 释放锁
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft { // 创建并返回一个新的 Raft 节点（实现 raftapi.Raft 接口）
	rf := &Raft{}            // 分配 Raft 结构体
	rf.peers = peers         // 保存所有同伴的 RPC 端点
	rf.persister = persister // 保存稳定存储句柄
	rf.me = me               // 记录本节点的索引

	rf.applyCh = applyCh // 保存 applyCh（用于发送已提交日志到上层服务）

	// Your initialization code here (3A, 3B, 3C).
	// 3A/3B/3C：在这里做初始化：
	// - 初始化角色为 Follower、term=0、votedFor=nil/-1、空日志等
	// - 初始化 commitIndex/lastApplied、leader 才需要的 nextIndex[]/matchIndex[]
	// - 保存 applyCh（用于把已提交日志发给上层）
	// - 可能需要设置初始的选举超时计时器状态
	rf.state = Follower                     // 初始状态为 Follower
	rf.currentTerm = 0                      // 初始任期为 0
	rf.votedFor = -1                        // 初始未投票给任何候选者（-1 表示未投票）
	rf.log = make([]LogEntry, 1)            // 初始日志为空（可以添加第一个日志条目）
	rf.log[0] = LogEntry{Index: 0, Term: 0} // 哨兵，永不被提交/应用
	rf.commitIndex = 0                      // 初始已提交日志索引为 0
	rf.lastApplied = 0                      // 初始已应用日志索引为 0

	rf.nextIndex = make([]int, len(peers))  // 初始化每个同伴的下一个日志索引
	rf.matchIndex = make([]int, len(peers)) // 初始化每个同伴的已匹配日志索引

	rf.resetElectionTimeout() // 设置初始选举超时（150-350ms）

	rf.mu.Lock()
	rf.applyCond = sync.NewCond(&rf.mu) // 发布在 rf.mu 保护下
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	// 从持久化状态恢复（若之前崩溃过，这里能把 term/votedFor/log 恢复出来）
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	if rf.commitIndex < rf.LastIncludedIndex {
		rf.commitIndex = rf.LastIncludedIndex
	}
	if rf.lastApplied < rf.LastIncludedIndex {
		rf.lastApplied = rf.LastIncludedIndex
	}
	snap := persister.ReadSnapshot()
	li := rf.LastIncludedIndex
	lt := rf.LastIncludedTerm
	rf.mu.Unlock()

	// 如果存在快照，上线时就把它交给上层
	if len(snap) > 0 {
		go func(s []byte, idx, term int) {
			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      s,
				SnapshotIndex: idx,
				SnapshotTerm:  term,
			}
		}(snap, li, lt)
	}

	// start ticker goroutine to start elections
	// 启动后台节拍协程：负责超时选举/心跳发送等周期性工作
	go rf.ticker()

	go rf.applyLoop() // 启动应用日志的专职 goroutine

	return rf // 以接口类型返回（raftapi.Raft），便于测试程序/服务端按接口调用
}

// 检查候选者的日志是否比当前Raft新（用于 RequestVote RPC）
func (rf *Raft) isLogUpToDate(candidateLastTerm, candidateLastIndex int) bool {
	// 获取当前 Raft 的最后日志条目
	LastLogIndex := rf.getLogLastIndex()
	lastLogTerm := rf.getLogTerm(LastLogIndex)

	// 如果候选者的日志条目任期更大，或者相同任期但索引更大，则认为候选者的日志更新
	if candidateLastTerm > lastLogTerm ||
		(candidateLastTerm == lastLogTerm && candidateLastIndex >= LastLogIndex) {
		return true
	}
	return false
}

// 获取当前 Raft 的最后日志条目（用于检查日志新旧）
func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1] // 返回最后一个日志条目
}

// 重置选举超时：设置一个随机的选举超时时间（150-350ms）
func (rf *Raft) resetElectionTimeout() {
	d := time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机选举超时：150-350ms
	rf.electionTimeout = time.Now().Add(d)
}

// 将当前节点降级为 Follower 状态（在加锁的前提下调用）
func (rf *Raft) stepDownLocked(term int) {
	rf.state = Follower       // 转为 Follower 状态
	rf.currentTerm = term     // 更新当前任期
	rf.votedFor = -1          // 重置投票状态
	rf.resetElectionTimeout() // 重置选举超时状态
	rf.persist()              // 持久化状态
}

// 发起选举：将状态设置为 Candidate，发送 RequestVote RPC 等
// 在加锁的前提下调用
func (rf *Raft) startElectionLocked() {
	rf.state = Candidate // 转为 Candidate 状态
	rf.currentTerm++     // 任期号加 1
	rf.votedFor = rf.me  // 投票给自己
	rf.persist()         // 持久化状态

	// 重置选举超时状态
	rf.resetElectionTimeout()

	// 准备 RequestVote RPC 参数
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}

	votesReceived := 1              // 自己的投票算一个
	majority := len(rf.peers)/2 + 1 // 需要的多数票数

	for peer := range rf.peers { // 向所有同伴发送 RequestVote RPC
		if peer == rf.me { // 不给自己发请求
			continue
		}

		// 异步发送 RequestVote RPC
		go func(server int, args RequestVoteArgs, term int) {
			var reply RequestVoteReply
			if ok := rf.sendRequestVote(server, &args, &reply); !ok { // 发送 RPC
				return // 如果发送失败，直接返回
			}
			// 检查 RPC 返回值
			rf.mu.Lock() // 获取锁，确保状态一致性
			defer rf.mu.Unlock()

			// 只处理当前任期，且还在任选阶段的回复，因为可能有旧的回复
			if rf.currentTerm != term || rf.state != Candidate {
				return // 忽略过期的回复
			}

			// 发现对方的任期更大，更新自己的状态
			if reply.Term > rf.currentTerm {
				rf.stepDownLocked(reply.Term) // 降级为 Follower
				return                        // 不再处理这个回复
			}

			// 如果对方同意投票
			if reply.VoteGranted {
				votesReceived++                // 收到一票
				if votesReceived >= majority { // 如果已获得多数票
					rf.state = Leader // 成为 Leader

					// 初始化 nextIndex 和 matchIndex
					last := rf.getLastLog().Index // 获取最后日志条目的索引
					for i := range rf.peers {
						rf.nextIndex[i] = last + 1 // 下一个日志索引为最后日志索引 + 1
						rf.matchIndex[i] = 0       // 已匹配日志索引初始化
					}
					rf.matchIndex[rf.me] = last // 自己的已匹配日志索引为最后日志索引

					rf.resetElectionTimeout() // 重置选举超时状态

					// 成为leader后，立刻发送一次心跳
					go rf.leaderLoop(term) // 启动领导者循环
					return                 // 成功选举，退出
				}
			}
		}(peer, args, rf.currentTerm) // 异步发送请求
	}
}

// leaderLoop：领导者的主循环（处理心跳、日志追加等）
func (rf *Raft) leaderLoop(term int) {
	// 当选后立刻发送一次心跳
	rf.broadcastHeartbeat(term) // 发送心跳给所有同伴

	for {
		// 100ms 后发送一次心跳
		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock() // 获取锁，确保状态一致性
		// 此时被kill或者不是leader或任期已变，退出循环
		if rf.killed() || rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock() // 释放锁
			return         // 退出领导者循环
		}
		rf.mu.Unlock() // 释放锁
		// 发送心跳给所有同伴
		rf.broadcastHeartbeat(term) // 广播心跳：向所有同伴发送 AppendEntries RPC（心跳）
	}
}

// 广播心跳：向所有同伴发送 AppendEntries RPC（心跳）
func (rf *Raft) broadcastHeartbeat(term int) {
	rf.mu.Lock() // 获取锁，确保状态一致性

	for peer := range rf.peers { // 向所有同伴发送 AppendEntries RPC
		if peer == rf.me { // 不给自己发请求
			continue
		}

		// 3D 当发现 nextIndex[peer] <= rf.lastIncludedIndex，不要再发 AppendEntries，改为发快照
		if rf.nextIndex[peer] <= rf.LastIncludedIndex {
			snapshot := rf.persister.ReadSnapshot() // 读取快照数据
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,       // 当前任期号
				LeaderId:          rf.me,                // 领导者的 ID（本节点的索引）
				LastIncludedIndex: rf.LastIncludedIndex, // 快照包含的最后日志条目的索引
				LastIncludedTerm:  rf.LastIncludedTerm,  // 快照包含的最后日志条目的任期号
				Snapshot:          snapshot,             // 快照数据
			}
			rf.mu.Unlock() // 释放锁，允许其他操作

			// 异步发送 InstallSnapshot RPC
			go func(server int, args InstallSnapshotArgs, leaderTerm int) {
				var reply InstallSnapshotReply
				if ok := rf.sendInstallSnapshot(server, &args, &reply); !ok { // 发送 InstallSnapshot RPC
					return // 如果发送失败，直接返回
				}

				// 检查 RPC 返回值
				rf.mu.Lock() // 获取锁，确保状态一致性
				defer rf.mu.Unlock()

				// 只处理当前任期且当前还是leader的回复，因为可能有旧的回复
				if rf.currentTerm != leaderTerm || rf.state != Leader {
					return // 忽略过期的回复
				}

				// 如果对方的任期更大，降级为 Follower
				if reply.Term > rf.currentTerm {
					rf.stepDownLocked(reply.Term) // 降级为 Follower
					return                        // 不再处理这个回复
				}
				// 如果 InstallSnapshot 成功，更新 nextIndex 和 matchIndex
				if rf.nextIndex[server] < rf.LastIncludedIndex+1 {
					rf.nextIndex[server] = rf.LastIncludedIndex + 1 // 更新下一个日志索引为快照的下一个索引
					rf.matchIndex[server] = rf.LastIncludedIndex    // 更新已匹配日志索引为快照的索引
				}

				// ====== ADD: 立刻续传快照之后的日志（或发一次空心跳） ======
				next := rf.nextIndex[server]
				if next <= rf.LastIncludedIndex {
					next = rf.LastIncludedIndex + 1
				}
				prev := next - 1
				ae := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prev,
					PrevLogTerm:  rf.getLogTerm(prev),
					Entries:      rf.getEntriesToSend(next), // 可能为空，作为心跳也可以
					LeaderCommit: rf.commitIndex,
				}
				go rf.sendAppendEntries(server, ae, rf.currentTerm)

				rf.checkCommitLocked()
				// ====== ADD END ======
			}(peer, args, term) // 异步发送 InstallSnapshot RPC
			rf.mu.Lock() // 重新获取锁，确保状态一致性
			continue     // 跳过当前 peer，继续下一个 peer
		}

		next := rf.nextIndex[peer] // 获取下一个日志索引
		if next <= rf.LastIncludedIndex {
			next = rf.LastIncludedIndex + 1 // 如果 next 小于等于快照索引，设置为快照的下一个索引
		}

		// 获取前一个日志条目的索引和任期号,用于follower端做一致性检查
		prevLogIndex := next - 1                // 前一个日志条目的索引
		prevTerm := rf.getLogTerm(prevLogIndex) // 前一个日志条目的任期号
		// 获取要发送的日志条目（心跳）
		entries := rf.getEntriesToSend(next)

		args := AppendEntriesArgs{
			Term:         term,           // 当前任期号
			LeaderId:     rf.me,          // 领导者的 ID（本节点的索引）
			PrevLogIndex: prevLogIndex,   // 前一个日志条目的索引
			PrevLogTerm:  prevTerm,       // 前一个日志条目的任期号
			Entries:      entries,        // 心跳
			LeaderCommit: rf.commitIndex, // 领导者已提交的日志索引
		}
		rf.mu.Unlock() // 释放锁

		go rf.sendAppendEntries(peer, args, term) // 异步发送 AppendEntries RPC
		rf.mu.Lock()                              // 重新获取锁，确保状态一致性
	}
	rf.mu.Unlock() // 释放锁

}

// 异步发送 AppendEntries RPC
func (rf *Raft) sendAppendEntries(peer int, args AppendEntriesArgs, term int) {
	var reply AppendEntriesReply
	if ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply); !ok { // 发送 RPC
		return // 如果发送失败，直接返回
	}

	// 检查 RPC 返回值
	rf.mu.Lock() // 获取锁，确保状态一致性
	defer rf.mu.Unlock()

	// 只处理当前任期且当前还是leader的回复，因为可能有旧的回复
	if rf.currentTerm != term || rf.state != Leader {
		return // 忽略过期的回复
	}

	// 如果对方的任期更大，降级为 Follower
	if reply.Term > rf.currentTerm {
		rf.stepDownLocked(reply.Term) // 降级为 Follower
		return                        // 不再处理这个回复
	}
	// 如果追加成功
	if reply.Success {
		// 更新 matchIndex 和 nextIndex
		matched := args.PrevLogIndex + len(args.Entries)
		if matched > rf.matchIndex[peer] { // ★ 只前进不回退
			rf.matchIndex[peer] = matched
		}
		next := matched + 1
		if next > rf.nextIndex[peer] { // ★ 只前进不回退
			rf.nextIndex[peer] = next
		}
		// Leader 尝试推进提交
		rf.checkCommitLocked() // 检查是否有日志可以提交（Leader 端）
	} else {

		// 这次 RPC 覆盖到的最后日志下标（含）
		sentEnd := args.PrevLogIndex + len(args.Entries)

		// 若这条失败回复对应的发送范围，比我们现在已知的进度还“旧”，忽略它
		if sentEnd < rf.matchIndex[peer] || sentEnd+1 < rf.nextIndex[peer] {
			return // 过期失败回复，不能用它来回退 nextIndex
		}

		// 如果追加失败，说明对方的日志不一致，需要回退 nextIndex
		// 快速回退
		if reply.ConflictTerm == -1 {
			// 对端太短：直接跳到它的长度
			rf.nextIndex[peer] = reply.ConflictIndex // 设置下一个日志索引为对方的长度
		} else {
			// 在我这边找“最后一个 ConflictTerm 的索引”
			last := -1
			for i := rf.getLogLastIndex(); i > rf.LastIncludedIndex; i-- {
				if rf.getLogTerm(i) == reply.ConflictTerm { // 找到冲突的任期
					last = i // 记录最后一个冲突的索引
					break    // 找到后退出循环
				}
			}
			if last != -1 {
				rf.nextIndex[peer] = last + 1 // 设置下一个日志索引为最后一个冲突的索引 + 1
			} else {
				rf.nextIndex[peer] = reply.ConflictIndex // 如果没有找到，回退到对方的冲突索引
			}
		}
		// if rf.nextIndex[peer] < rf.LastIncludedIndex+1 {
		// 	rf.nextIndex[peer] = rf.LastIncludedIndex + 1 // 确保 nextIndex 至少为快照的下一个索引
		// }
		// 追加失败，可能是因为日志不一致，需要重新发送心跳
		// 重新发送心跳，尝试修复日志不一致
		// go rf.broadcastHeartbeat(term) // 异步重新发送 AppendEntries RPC
		// 立刻只对这个 peer 定向重试一次
		peerNext := rf.nextIndex[peer]
		if peerNext <= rf.LastIncludedIndex {
			// 发 Snapshot
			snapshot := rf.persister.ReadSnapshot()
			isArgs := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.LastIncludedIndex,
				LastIncludedTerm:  rf.LastIncludedTerm,
				Snapshot:          snapshot,
			}
			// 用当前 term 作为回调比对
			curTerm := rf.currentTerm
			go func(server int, args InstallSnapshotArgs, leaderTerm int) {
				var r InstallSnapshotReply
				if !rf.sendInstallSnapshot(server, &args, &r) {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != leaderTerm || rf.state != Leader {
					return
				}
				if r.Term > rf.currentTerm {
					rf.stepDownLocked(r.Term)
					return
				}
				if rf.nextIndex[server] < rf.LastIncludedIndex+1 {
					rf.nextIndex[server] = rf.LastIncludedIndex + 1
					rf.matchIndex[server] = rf.LastIncludedIndex
				}
				// 紧接着从 LI+1 续传一波（可能为空心跳）
				next := rf.nextIndex[server]
				if next <= rf.LastIncludedIndex {
					next = rf.LastIncludedIndex + 1
				}
				prev := next - 1
				ae := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prev,
					PrevLogTerm:  rf.getLogTerm(prev),
					Entries:      rf.getEntriesToSend(next),
					LeaderCommit: rf.commitIndex,
				}
				go rf.sendAppendEntries(server, ae, rf.currentTerm)
			}(peer, isArgs, curTerm)
		} else {
			// 发 AE
			next := peerNext
			prev := next - 1
			ae := AppendEntriesArgs{
				Term:         term, // 这里可以用传入的 term；也可以用 rf.currentTerm（两者在活跃 leader 下等价）
				LeaderId:     rf.me,
				PrevLogIndex: prev,
				PrevLogTerm:  rf.getLogTerm(prev),
				Entries:      rf.getEntriesToSend(next),
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(peer, ae, term)
		}
	}
}

// 检查是否有日志可以提交（Leader 端）
// 在加锁的前提下调用
func (rf *Raft) checkCommitLocked() {
	// 从右到左遍历log，找到最大的已提交日志索引
	for i := rf.getLogLastIndex(); i > rf.commitIndex; i-- {
		if rf.getLogTerm(i) != rf.currentTerm { // 只考虑当前任期的日志
			continue // 跳过非当前任期的日志
		}
		// 检查是否有超过半数的节点已匹配该日志
		count := 1 // 自己算一个
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i { // 其他节点已匹配该日志
				count++
			}
		}
		if count >= len(rf.peers)/2+1 { // 如果超过半数节点已匹配
			rf.commitIndex = i // 更新已提交日志索引
			// rf.applyCommittedLogsLocked() // 应用已提交的日志到状态机
			// 使用专职 goroutine异步应用已提交日志，代替applyCommittedLogsLocked
			if rf.applyCond != nil {
				rf.applyCond.Signal() // 唤醒等待中的 applyLoop，应用已提交日志
			}
			return // 提交成功，退出函数
		}
	}
}

// 应用已提交的日志到状态机（Leader 端）
// 从 (lastApplied, commitIndex] 复制条目，解锁后逐条发到 applyCh
// 在加锁的前提下调用
// func (rf *Raft) applyCommittedLogsLocked() {
// 	if rf.commitIndex <= rf.lastApplied {
// 		return // 没有新的日志需要应用
// 	}
// 	// 复制可提交的条目
// 	start := rf.lastApplied + 1            // 从下一个未应用的日志开始
// 	end := rf.commitIndex + 1              // 到已提交的日志索引为止
// 	toApply := make([]LogEntry, end-start) // 创建待应用的日志条目切片
// 	copy(toApply, rf.log[start:end])       // 复制日志条目
// 	rf.lastApplied = rf.commitIndex        // 更新已应用日志索引

// 	// 解锁后逐条发到 applyCh
// 	ch := rf.applyCh
// 	rf.mu.Unlock() // 释放锁，允许其他操作
// 	for _, entry := range toApply {
// 		msg := raftapi.ApplyMsg{
// 			CommandValid: true,          // 表示这是一个应用层命令
// 			Command:      entry.Command, // 应用的命令
// 			CommandIndex: entry.Index,   // 命令在日志中的索引
// 		}
// 		ch <- msg // 发送到 applyCh
// 	}
// 	rf.mu.Lock() // 重新获取锁，确保状态一致性
// }

// 专职应用已提交日志的协程，代替applyCommittedLogsLocked
func (rf *Raft) applyLoop() {
	rf.mu.Lock() // 获取锁，确保状态一致性
	// 如果某些实例因为时序问题没初始化到，兜底再建一次
	if rf.applyCond == nil {
		rf.applyCond = sync.NewCond(&rf.mu)
	}
	defer rf.mu.Unlock() // 确保函数结束时释放锁

	for !rf.killed() { // 循环直到被 Kill()
		for rf.lastApplied >= rf.commitIndex { // 等待有新的日志可应用
			rf.applyCond.Wait() // 等待条件变量，直到有新的日志可应用
			if rf.killed() {    // 如果被 Kill()，退出循环
				return
			}
		}

		// 每次只推进一步，天然保证顺序
		index := rf.lastApplied + 1            // 下一个要应用的日志索引
		entry := rf.log[rf.getLogIndex(index)] // 获取要应用的日志条目
		rf.lastApplied = index                 // 更新已应用日志索引

		msg := raftapi.ApplyMsg{
			CommandValid: true,          // 表示这是一个应用层命令
			Command:      entry.Command, // 应用的命令
			CommandIndex: entry.Index,   // 命令在日志中的索引
		}

		ch := rf.applyCh // 获取 applyCh
		rf.mu.Unlock()   // 释放锁，允许其他操作
		ch <- msg        // 发送到 applyCh
		rf.mu.Lock()     // 重新获取锁，确保状态一致性
	}
}

// 绝对索引i -> 当前日志的索引
func (rf *Raft) getLogIndex(i int) int { // 获取绝对索引 i 在当前日志中的索引
	return i - rf.LastIncludedIndex
}

// 当前的最后一条日志的绝对索引
func (rf *Raft) getLogLastIndex() int {
	return rf.LastIncludedIndex + len(rf.log) - 1 // 返回当前日志的最后一条绝对索引
}

// 取任意绝对索引i的term  i==LastIncludedIndex  用rf.LastIncludedTerm，i<基线非法
func (rf *Raft) getLogTerm(i int) int {
	if i == rf.LastIncludedIndex { // 如果是快照的索引
		return rf.LastIncludedTerm // 返回快照的任期号
	}

	if i < rf.LastIncludedIndex || rf.getLogIndex(i) >= len(rf.log) { // 如果索引小于快照索引或超出日志范围
		return -1
	}
	return rf.log[rf.getLogIndex(i)].Term
}

// 从绝对索引 start 开始复制要发送的 entries（带好 Index 字段）
func (rf *Raft) getEntriesToSend(start int) []LogEntry {
	if start > rf.getLogLastIndex() { // 如果起始索引超过最后一条日志的绝对索引
		return nil // 返回空切片
	}

	entries := make([]LogEntry, rf.getLogLastIndex()-start+1) // 创建切片，长度为从 start 到最后一条日志的长度

	copy(entries, rf.log[rf.getLogIndex(start):]) // 从日志中复制条目到切片
	for i := range entries {                      // 设置每个条目的索引
		entries[i].Index = start + i // 设置索引为绝对索引
	}
	return entries // 返回要发送的日志条目切片
}
