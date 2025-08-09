/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-09 00:54:43
 * @FilePath: /MIT-6.5840-6.824/src/kvsrv1/server.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data    map[string]string       // 存储键值对
	version map[string]rpc.Tversion // 存储键对应的版本号
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data:    make(map[string]string),
		version: make(map[string]rpc.Tversion),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断键是否存在
	if _, ok := kv.data[args.Key]; !ok { // 如果不存在，返回 ErrNoKey
		reply.Err = rpc.ErrNoKey
	} else { // 如果存在，返回对应的值和版本号
		reply.Value = kv.data[args.Key]
		reply.Version = kv.version[args.Key]
		reply.Err = rpc.OK
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查传入参数版本是否为0,0表示插入数据
	if args.Version == 0 {
		// 如果键存在，返回ErrVersion
		if _, ok := kv.data[args.Key]; ok {
			reply.Err = rpc.ErrVersion // 键已存在，不能插入
		} else { // 如果键不存在，插入新键值对
			kv.data[args.Key] = args.Value
			kv.version[args.Key] = 1
			reply.Err = rpc.OK // 成功插入
		}
	} else {
		// 如果不是插入操作，先看key是否已经存在
		if _, ok := kv.data[args.Key]; !ok {
			reply.Err = rpc.ErrNoKey // 键不存在，返回 ErrNoKey
		} else if kv.version[args.Key] != args.Version {
			reply.Err = rpc.ErrVersion // 版本不匹配，返回 ErrVersion
		} else {
			kv.data[args.Key] = args.Value
			kv.version[args.Key]++
			reply.Err = rpc.OK // 成功更新
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
