/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-08-06 17:18:58
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-08-08 17:15:25
 * @FilePath: /MIT-6.5840-6.824/src/mr/worker.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
//
// Map 函数返回的元素类型，每个中间结果就是一对 (Key, Value)
type KeyValue struct {
	Key   string
	Value string
}

// 用于排序
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
// 对一个 key 进行哈希，然后取低 31 位正整数，用来做 hash(key) % NReduce
// 决定这个 KV 对应该写入哪个 Reduce 任务
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
//
// worker工作入口
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 启动worker程序，循环从协调器获取任务
	for {
		// Rpc请求获取任务
		task := getTask()

		// 根据任务类型执行不同的操作
		switch task.TaskType {
		case Map:
			// 执行 Map 任务
			mapFunction(&task, mapf)
		case Reduce:
			// 执行 Reduce 任务
			reduceFunction(&task, reducef)
		case Wait:
			// 等待任务，可能是等待其他任务完成或等待新的任务
			time.Sleep(5 * time.Second)
		case Exit:
			// 退出任务
			return
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// RPC远程调用示例
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// RPC 通信封装：call 函数
// 用于实现RPC远程调用
// rpcname 是 RPC 方法名，args 是请求参数，reply 是响应结果
// 返回值表示调用是否成功
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		// 协调器未启动或连接失败，直接退出worker
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// 自定义函数

// RPC远程获取任务
func getTask() Task {
	// 发送 RPC 请求获取任务
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	// ok := call("Coordinator.GetTask", &args, &reply)
	// if !ok {
	// 	log.Fatal("GetTask failed")
	// }
	call("Coordinator.GetTask", &args, &reply)

	return *reply.Task
}

// 通过RPC通知协调器任务完成，将更新后的task传回协调器
func taskCompleted(task *Task) {
	args := TaskCompletedArgs{Task: task}
	reply := TaskCompletedReply{}

	// ok := call("Coordinator.TaskCompleted", &args, &reply)
	// if !ok {
	// 	log.Fatal("TaskCompleted failed")
	// }
	call("Coordinator.TaskCompleted", &args, &reply)
}

// Map函数实现，主要是调用mapf
func mapFunction(task *Task, mapf func(string, string) []KeyValue) {
	// 从task中获取文件名，打开文件，读取文件内容交给 mapf 处理
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Fatal("ReadFile failed:" + task.Filename + ", " + err.Error())
	}

	// 调用mapf处理文件内容
	intermediate := mapf(task.Filename, string(content))

	// 将中间结果通过哈希映射分配到 R 个Reducer 上
	buffer := make([][]KeyValue, task.NReducer) // buffer[i] 存储第 i 个 Reducer 的中间结果
	for _, kv := range intermediate {
		reduceTaskID := ihash(kv.Key) % task.NReducer           // 计算该 kv 对应的 Reducer 任务 ID
		buffer[reduceTaskID] = append(buffer[reduceTaskID], kv) // 将 kv 添加到对应的 Reducer 中间结果列表中
	}

	// 任务要求需要把中间结果写到worker本地磁盘
	// 中间文件名是 mr-X-Y, X 是Map 任务 ID，Y 是 Reducer 任务 ID
	// 但是我觉得对于分布式来说，中间结果存worker本地磁盘不太合适，应该直接发送给Coordinator，并存储在Coordinator的本地
	intermediateFiles := make([]string, task.NReducer)
	for i := 0; i < task.NReducer; i++ {
		intermediateFiles[i] = writeIntermediateFileToLocal(task.TaskID, i, buffer[i])
	}

	task.IntermediateFile = intermediateFiles // 更新任务的中间文件列表

	// 将更新的task元数据通过RPC发送给Coordinator
	// 真正的分布式应该把Map的中间结果发送给Coordinator，而不是存储在worker本地
	taskCompleted(task)
}

func reduceFunction(task *Task, reducef func(string, []string) string) {
	// 从task中获取中间文件列表，读取每个中间文件的内容
	intermediate := *readIntermediateFileFromLocal(task.IntermediateFile)

	// 排序
	sort.Sort(ByKey(intermediate))

	// 执行 Reduce 操作
	dir, _ := os.Getwd() // 获取当前工作目录
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("CreateTemp failed:", err)
	}

	// 统计，和mrsequential.go类似
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 将结果写入临时文件
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	// 重命名临时文件为最终输出文件
	finalName := fmt.Sprintf("mr-out-%d", task.TaskID)
	if err := os.Rename(tempFile.Name(), finalName); err != nil {
		log.Fatal("Rename failed:", err)
	}
	// 将最终输出文件名添加到任务中
	task.OutputFile = finalName
	// 通知Coordinator任务完成
	taskCompleted(task)
}

// 将中间结果写入本地磁盘
func writeIntermediateFileToLocal(taskID int, readerID int, intermediate []KeyValue) string {
	// 获取当前工作路径 “/home/zxf/WorkSpace/MIT-6.5840-6.824/src/mr/”
	workingDir, err := os.Getwd()
	if err != nil {
		log.Fatal("Getwd failed:", err)
	}

	// 创建中间文件
	// 先创建临时文件，好处是出错文件直接删除，不会残留
	// 后续再改文件名就行
	tempFile, err := os.CreateTemp(workingDir, "mr-tmp-*")
	if err != nil {
		log.Fatal("TempFile failed:", err)
	}

	// 序列化kv数据，方便后续使用
	encoder := json.NewEncoder(tempFile)
	for _, kv := range intermediate {
		if err := encoder.Encode(kv); err != nil {
			log.Fatal("Encode failed:", err)
		}
	}
	tempFile.Close()

	// 重命名临时文件
	finalName := fmt.Sprintf("mr-%d-%d", taskID, readerID)
	if err := os.Rename(tempFile.Name(), finalName); err != nil {
		log.Fatal("Rename failed:", err)
	}
	return filepath.Join(workingDir, finalName) // 返回最终的中间文件名
}

// 读取本地中间文件内容
func readIntermediateFileFromLocal(files []string) *[]KeyValue {
	intermediate := []KeyValue{}
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Open file failed:", filePath, err)
		}

		// 反序列化读取中间结果
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break // 读取完毕或出错
			}
			intermediate = append(intermediate, kv)
		}
	}
	return &intermediate
}
