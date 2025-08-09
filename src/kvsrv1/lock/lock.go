/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-09 23:49:59
 * @FilePath: /MIT-6.5840-6.824/src/kvsrv1/lock/lock.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	lockKey string // 锁类型，在K.V中是K，由MakeLock函数的参数l指定
	lockID  string // 锁的唯一标识符，由kvtest.RandValue(8)生成，是K/V中的V，用于标识该锁被哪个进程持有
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
// 初始化入口，测试程序调用此函数传入RPC客户端指针以及锁类型，这个类型是KV 的K
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:      ck,
		lockKey: l,
		lockID:  kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

// 获取锁
func (lk *Lock) Acquire() {
	// Your code here
	// 循环获取，直到拿到锁
	for {
		// 查看当前锁在K/V中的状态
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey { // 锁不存在
			// 锁不存在，尝试获取锁
			if lk.ck.Put(lk.lockKey, lk.lockID, 0) == rpc.OK {
				// 成功获取锁
				return
			}
			// 获取失败则重试
			continue
		}
		if err != rpc.OK { // 其他错误重试
			continue
		}

		if value == "" {
			// 锁存在但是没有被占用，尝试获取锁
			if lk.ck.Put(lk.lockKey, lk.lockID, version) == rpc.OK {
				// 成功获取锁
				return
			}
			// 获取失败则重试
			continue
		} else if value == lk.lockID {
			// 锁存在且被自己占用，直接返回
			return
		}
		// 锁存在且被其他客户端占用，等待一段时间后重试
		// 这里可以添加一个等待时间，避免过于频繁的请求
		time.Sleep(10 * time.Millisecond)

	}
}

// 解锁
func (lk *Lock) Release() {
	// Your code here
	// 循环释放锁，直到成功
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		if err != rpc.OK { // 获取锁状态失败，重试
			continue
		}

		if value == lk.lockID { // 锁被自己占用
			// 尝试释放锁
			if lk.ck.Put(lk.lockKey, "", version) == rpc.OK {
				// 成功释放锁
				return
			}
		} else if value == "" {
			// 锁未被占用
			return
		}
		// 锁被其他客户端占用，等待一段时间后重试
		time.Sleep(10 * time.Millisecond)
	}
}
