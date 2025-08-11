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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	lastApplied int        // 已应用到状态机的日志条目的索引（>= commitIndex）
	nextIndex   []int      // leader 的易失状态：每个同伴的下一个日志索引（用于追加日志）
	matchIndex  []int      // leader 的易失状态：每个同伴的已匹配日志索引（用于确认日志已提交）

	state           State     // 当前状态（Follower/Candidate/Leader）
	electionTimeout time.Time // 选举超时：用于判断是否需要发起选举

	// 3B
	applyCh chan raftapi.ApplyMsg // 用于发送已提交日志到上层服务的通道（ApplyMsg）

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
func (rf *Raft) Snapshot(index int, snapshot []byte) { // 上层生成了包含 [..index] 的快照；可裁剪日志
	// Your code here (3D).

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

// 处理对方发来的 AppendEntries RPC（心跳/日志追加）
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

	// prev 是否存在
	if args.PrevLogIndex >= len(rf.log) {
		// 太短：告诉 leader 我长度
		reply.ConflictIndex = len(rf.log) // 返回当前日志长度
		reply.ConflictTerm = -1           // 没有冲突的任期
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 任期不匹配：返回该位置的任期，以及这个任期在我这的第一条索引
		ct := rf.log[args.PrevLogIndex].Term  // 当前日志条目的任期
		i := args.PrevLogIndex                // 从 prevLogIndex 开始查找
		for i > 0 && rf.log[i-1].Term == ct { // 向前查找直到找到不同的任期
			i-- // 向前移动
		}
		reply.ConflictTerm = ct // 返回冲突的任期
		reply.ConflictIndex = i // 返回冲突任期的第一条索引
		return                  // 返回，表示追加失败
	}

	// 前一个日志条目匹配，从 prev 之后逐个对齐；遇到任期冲突就截断本地再整体追加
	index := args.PrevLogIndex + 1 // 从 prevLogIndex 的下一个开始
	i := 0
	for ; i < len(args.Entries); i++ { // 遍历要追加的日志条目
		if index+i < len(rf.log) { // 如果本地日志中有这个索引
			if rf.log[index+i].Term != args.Entries[i].Term { // 如果任期不匹配
				rf.log = rf.log[:index+i] // 截断本地日志
				break
			}
			// 相同任期，继续对齐
		} else { // 如果本地日志中没有这个索引
			break
		}
	}

	// 追加新的日志条目
	for ; i < len(args.Entries); i++ { // 追加剩余的日志条目
		e := args.Entries[i]
		e.Index = len(rf.log)      // 设置索引
		rf.log = append(rf.log, e) // 追加到本地日志
	}
	rf.persist() // 持久化日志

	// 更新已提交日志索引
	if args.LeaderCommit > rf.commitIndex { // 如果领导者的已提交日志索引大于本地的
		lastNew := len(rf.log) - 1
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
		rf.applyCommittedLogsLocked() // 应用已提交的日志到状态机
	}

	reply.Term = rf.currentTerm // 返回当前任期
	reply.Success = true        // 追加成功

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) { // 处理对方发来的 RequestVote
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { // 发送 RequestVote
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
func (rf *Raft) Start(command interface{}) (int, int, bool) { // 上层请求把 command 追加到日志（仅 leader 才能受理）
	rf.mu.Lock() // 获取锁，确保状态一致性
	defer rf.mu.Unlock()

	if rf.state != Leader { // 如果不是 Leader，直接返回
		return -1, rf.currentTerm, false // 返回 -1 表示未能追加，返回当前任期和非 Leader 状态
	}
	// 如果是 Leader，创建新的日志条目
	index := len(rf.log) // 新日志条目的索引为当前日志长度
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

	// initialize from state persisted before a crash
	// 从持久化状态恢复（若之前崩溃过，这里能把 term/votedFor/log 恢复出来）
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 启动后台节拍协程：负责超时选举/心跳发送等周期性工作
	go rf.ticker()

	return rf // 以接口类型返回（raftapi.Raft），便于测试程序/服务端按接口调用
}

// 检查候选者的日志是否比当前Raft新（用于 RequestVote RPC）
func (rf *Raft) isLogUpToDate(candidateLastTerm, candidateLastIndex int) bool {
	// 获取当前 Raft 的最后日志条目
	lastLog := rf.getLastLog()

	// 如果候选者的日志条目任期更大，或者相同任期但索引更大，则认为候选者的日志更新
	if candidateLastTerm > lastLog.Term ||
		(candidateLastTerm == lastLog.Term && candidateLastIndex >= lastLog.Index) {
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

		next := rf.nextIndex[peer] // 获取下一个日志索引
		if next < 1 {
			next = 1 // 确保下一个索引至少为 1（因为索引 0 是哨兵）
		}

		// 获取前一个日志条目的索引和任期号,用于follower端做一致性检查
		prevLogIndex := next - 1 // 前一个日志条目的索引
		// 若prev 超界
		if prevLogIndex >= len(rf.log) {
			prevLogIndex = len(rf.log) - 1 // 确保不超出日志范围
			next = prevLogIndex + 1        // 更新下一个索引
		}
		prevTerm := rf.log[prevLogIndex].Term // 前一个日志条目的任期号

		// 拷贝要发送的日志条目
		entries := make([]LogEntry, len(rf.log[next:])) // 从 next 索引开始到日志末尾的条目
		copy(entries, rf.log[next:])                    // 复制日志条目

		// 确保index字段正确
		for i := range entries {
			entries[i].Index = next + i // 设置每个条目的索引
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm, // 当前任期号
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
		// 如果追加失败，说明对方的日志不一致，需要回退 nextIndex
		// 快速回退
		if reply.ConflictTerm == -1 {
			// 对端太短：直接跳到它的长度
			rf.nextIndex[peer] = reply.ConflictIndex // 设置下一个日志索引为对方的长度
		} else {
			// 在我这边找“最后一个 ConflictTerm 的索引”
			last := -1
			for i := len(rf.log) - 1; i >= 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm { // 找到冲突的任期
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
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1 // 确保 nextIndex 至少为 1（因为索引 0 是哨兵）
		}
		// 追加失败，可能是因为日志不一致，需要重新发送心跳
		// 重新发送心跳，尝试修复日志不一致
		go rf.broadcastHeartbeat(term) // 异步重新发送 AppendEntries RPC

	}
}

// 检查是否有日志可以提交（Leader 端）
// 在加锁的前提下调用
func (rf *Raft) checkCommitLocked() {
	// 从右到左遍历log，找到最大的已提交日志索引
	for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
		if rf.log[i].Term != rf.currentTerm { // 只考虑当前任期的日志
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
			rf.commitIndex = i            // 更新已提交日志索引
			rf.applyCommittedLogsLocked() // 应用已提交的日志到状态机
			return                        // 提交成功，退出函数
		}
	}
}

// 应用已提交的日志到状态机（Leader 端）
// 从 (lastApplied, commitIndex] 复制条目，解锁后逐条发到 applyCh
// 在加锁的前提下调用
func (rf *Raft) applyCommittedLogsLocked() {
	if rf.commitIndex <= rf.lastApplied {
		return // 没有新的日志需要应用
	}
	// 复制可提交的条目
	start := rf.lastApplied + 1            // 从下一个未应用的日志开始
	end := rf.commitIndex + 1              // 到已提交的日志索引为止
	toApply := make([]LogEntry, end-start) // 创建待应用的日志条目切片
	copy(toApply, rf.log[start:end])       // 复制日志条目
	rf.lastApplied = rf.commitIndex        // 更新已应用日志索引

	// 解锁后逐条发到 applyCh
	ch := rf.applyCh
	rf.mu.Unlock() // 释放锁，允许其他操作
	for _, entry := range toApply {
		msg := raftapi.ApplyMsg{
			CommandValid: true,          // 表示这是一个应用层命令
			Command:      entry.Command, // 应用的命令
			CommandIndex: entry.Index,   // 命令在日志中的索引
		}
		ch <- msg // 发送到 applyCh
	}
	rf.mu.Lock() // 重新获取锁，确保状态一致性
}
