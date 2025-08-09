/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-08 14:29:15
 * @FilePath: /MIT-6.5840-6.824/src/mr/rpc.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// RPC请求获取任务的参数和返回值结构体
type GetTaskArgs struct { // 请求任务的参数不需要，内部直接为空
}

type GetTaskReply struct {
	Task *Task
}

// RPC通知协调器完成任务的参数和返回值结构体
type TaskCompletedArgs struct {
	Task *Task // 完成的任务
}
type TaskCompletedReply struct { // 返回值不需要，内部直接为空
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
