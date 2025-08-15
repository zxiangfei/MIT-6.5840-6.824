/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-15 23:36:40
 * @FilePath: /MIT-6.5840-6.824/src/kvraft1/rsm/rsm.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

// Op：一条要复制并执行的业务操作
// 字段需导出（首字母大写）以便序列化/RPC
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int   // 服务器编号
	Id  int64 // 唯一标识符，确保每个操作唯一
	Req any   // 请求内容，可能是一个 Get 或 Put 操作
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
// 状态机接口：上层服务实现
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	// 4A
	waiters map[int]*waiter // 用每个 Start(index) 对应一个等待者；reader 在该 index 提交时唤醒它
	nextID  int64           // 生成唯一 Op.Id
	doneCh  chan struct{}   // 在 rf.Kill() 时 applyCh 会被关闭；reader 退出后关闭 doneCh，Submit 可据此退出等待

	// 4C
	lastApplied int // 最后应用的日志索引（用于快照）
}

// waiter 保存一次submit的等待结果
type waiter struct {
	term int         // 提交时的 term
	id   int64       // 提交的唯一标识符
	ch   chan result // 等待结果的 channel
}

// result 是交回submit的执行结果
type result struct {
	err rpc.Err // 错误类型
	val any     // 执行结果
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
// MakeRSM：构造 rsm，启动 Raft 与 reader
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,

		waiters: make(map[int]*waiter),
		doneCh:  make(chan struct{}),
	}

	// 重启后，先用persister 中最近快照恢复服务状态（若存在）
	if data := persister.ReadSnapshot(); len(data) > 0 {
		rsm.sm.Restore(data) // 恢复状态机
	}

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	//后台reader：从applyCh取提交的日志，执行DoOp，并把结果送回相应Submit
	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
// Submit：把请求提交到 Raft 并等待它被提交/执行
// 丢领导时返回 ErrWrongLeader
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

	// 生成唯一的 Op.Id
	id := atomic.AddInt64(&rsm.nextID, 1)
	op := Op{
		Me:  rsm.me,
		Id:  id,
		Req: req,
	}

	// 1. 提交到 Raft,非 leader 时返回 ErrWrongLeader
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil // 不是 leader，返回错误
	}

	// 2.为本次index创建一个等待者
	waiter := &waiter{
		term: term,
		id:   id,
		ch:   make(chan result, 1), // 缓冲通道，避免 reader 阻塞
	}
	rsm.mu.Lock()
	rsm.waiters[index] = waiter // 保存等待者
	rsm.mu.Unlock()

	// 3. 等待提交结果 / 检测失去领导权 / 被kill
	tricker := time.NewTicker(50 * time.Millisecond)
	defer tricker.Stop() // 确保退出时停止 ticker
	for {
		select {
		case res := <-waiter.ch: // 等待者被唤醒
			// reader 带回了执行结果（OK 或 WrongLeader）
			return res.err, res.val
		case <-rsm.doneCh:
			// Raft 被 Kill，applyCh 关闭，reader 退出，doneCh 被关闭
			rsm.mu.Lock()
			// 检查是否还有等待者
			if ww, ok := rsm.waiters[index]; ok && ww.id == id {
				delete(rsm.waiters, index) // 清理等待者
			}
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil // 返回 ErrWrongLeader
		case <-tricker.C: // 定时检查
			// 轮询检测“丢领导”：任期变了或已不是 leader
			if currentTerm, isLeader := rsm.rf.GetState(); !isLeader || currentTerm != term {
				rsm.mu.Lock()
				// 检查是否还有等待者
				if ww, ok := rsm.waiters[index]; ok && ww.id == id {
					delete(rsm.waiters, index) // 清理等待者
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil // 返回 ErrWrongLeader
			}
		}
	}
}

// reader：把已提交日志交给状态机执行；
// 若能确认这是本节点作为 leader 在 term0/index 提交的同一条命令，
// 则把 DoOp 的返回值送回对应 Submit；
// 若发现“索引错配”（同一 index 提交了不同 term 或不同 Op.Id），
// 说明我们那次 Start 没成功，唤醒等待者并返回 ErrWrongLeader
func (rsm *RSM) reader() {
	defer close(rsm.doneCh) // 退出时关闭 doneCh

	for msg := range rsm.applyCh {
		// 4C.优先处理快照类消息
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)        // 恢复状态机
			rsm.lastApplied = msg.SnapshotIndex // 更新 lastApplied

			// 快照覆盖到的 index 之前的等待者全部清理并唤醒
			rsm.mu.Lock()
			for idx, w := range rsm.waiters {
				if idx <= msg.SnapshotIndex {
					// 用 ErrWrongLeader 让上层重试最稳妥
					select {
					case w.ch <- result{err: rpc.ErrWrongLeader, val: nil}:
					default:
					}
					delete(rsm.waiters, idx)
				}
			}
			rsm.mu.Unlock()
			continue // 继续处理下一个消息
		}

		if !msg.CommandValid {
			continue
		}

		index := msg.CommandIndex
		op, _ := msg.Command.(Op)

		// 执行业务操作
		val := rsm.sm.DoOp(op.Req)

		// 更新 lastApplied
		rsm.lastApplied = index

		// 根据持久化大小决定是否触发快照
		rsm.MakeSnapshot()

		// 看看这个index是否有人在等
		rsm.mu.Lock()
		waiter, ok := rsm.waiters[index]

		if ok {
			// Op.Id 必须等于等待者记录的 Id（防止“内容不同但 term 相同”的误配）
			if waiter.id == op.Id {
				// 唤醒等待者，返回结果
				waiter.ch <- result{val: val, err: rpc.OK}
			} else {
				// 索引错配：term 不同或 Id 不同，说明这是个错误的提交
				// 唤醒等待者，返回 ErrWrongLeader
				waiter.ch <- result{val: nil, err: rpc.ErrWrongLeader}
			}
			delete(rsm.waiters, index) // 删除已处理的等待者
		}
		rsm.mu.Unlock()
	}

	// 注意：如果是快照或其他非命令消息，applyCh 会被关闭，
	// reader 退出时会关闭 doneCh，通知 Submit 等待者退出等待。
	rsm.mu.Lock()
	for index, waiter := range rsm.waiters {
		// 如果还有等待者，唤醒它们并返回 ErrWrongLeader
		waiter.ch <- result{val: nil, err: rpc.ErrWrongLeader}
		delete(rsm.waiters, index) // 清理已唤醒的等待者
	}
	rsm.mu.Unlock()
}

// 4C.达到阈值时，触发快照，只在以应用位置截断
func (rsm *RSM) MakeSnapshot() {
	if rsm.maxraftstate == -1 {
		return // 不需要快照
	}

	if rsm.rf.PersistBytes() >= rsm.maxraftstate && rsm.lastApplied > 0 {
		data := rsm.sm.Snapshot() // 获取状态机快照
		if len(data) == 0 {
			return // 没有快照数据，不需要提交
		}

		// 让 Raft 在 lastApplied 处保存快照并丢弃更早的日志
		rsm.rf.Snapshot(rsm.lastApplied, data)
	}
}
