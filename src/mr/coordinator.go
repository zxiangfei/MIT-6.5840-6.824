package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 用于标识task和Coordinator的状态
type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	Filename         string   // 输入文件名
	TaskType         State    // 任务类型：Map 或 Reduce 、Exit 、Wait
	TaskID           int      // 任务 ID
	NReducer         int      // 用于标识该task应该属于哪个 Reducer 任务
	IntermediateFile []string // Reduce阶段专用，存储Map阶段输出的中间文件名
	OutputFile       string   // Reduce阶段专用，存储最终输出文件名
}

// 标识task的执行状态，是 Idle, InProgress, Completed
type CoordinatorTaskState int

const (
	Idle CoordinatorTaskState = iota
	InProgress
	Completed
)

// TaskMeta 用于存储任务的元数据
type CoordinatorTask struct {
	TaskStatus CoordinatorTaskState // 标识task的执行状态，是 Idle, InProgress, Completed
	StartTime  time.Time            // 任务开始时间
	TaskMeta   *Task                // 任务的元数据
}

// 协调器结构体，用于存放整个作业调度状态的“核心”数据结构
type Coordinator struct {
	Tasks             chan *Task               // 所有任务的列表
	TaskMeta          map[int]*CoordinatorTask // 任务元数据，key为任务ID
	CoordinatorState  State                    // 协调器状态，Map、Reduce
	NReduce           int                      // Reduce任务数
	InputFiles        []string                 // 输入文件列表
	IntermediateFiles [][]string               // 中间文件列表
}

var mu sync.Mutex // 协调器锁

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// RPC处理函数示例
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
//
// 启动 RPC 服务
func (c *Coordinator) server() {
	rpc.Register(c)  // 把 Coordinator 对象的方法（满足 RPC 规则的）都注册为 RPC 服务
	rpc.HandleHTTP() // 将 RPC 服务挂到 HTTP 路由上（Worker 端会用 HTTP+RPC 调用）
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() // 获取协调器的 UNIX 域套接字名称
	os.Remove(sockname)           // 确保之前的套接字不存在
	// 监听 UNIX 域套接字
	l, e := net.Listen("unix", sockname) // 同一台机器上的进程间通信使用 UNIX 域套接字
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // 使用新的 goroutine 异步处理 HTTP 请求，监听并处理来自 Worker 的 RPC
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
// 检查作业是否全部完成
func (c *Coordinator) Done() bool {
	mu.Lock()         // 获取锁，防止并发访问
	defer mu.Unlock() // 确保函数结束时释放锁

	ret := c.CoordinatorState == Exit // 检查协调器状态是否为 Exit，表示所有任务已完成

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// 创建一个新的协调器实例
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Tasks:             make(chan *Task, max(nReduce, len(files))), // 任务队列，缓冲区最大为 nReduce 和输入文件数的最大值，确保两个阶段正常使用
		TaskMeta:          make(map[int]*CoordinatorTask),
		CoordinatorState:  Map,
		NReduce:           nReduce,
		InputFiles:        files,
		IntermediateFiles: make([][]string, nReduce),
	}

	// 将每个初始文件创建成一个Map任务
	c.createMapTask() // 创建Map任务,在函数中已经将创建的task放入Tasks channel中

	// 启动 服务
	c.server()

	// 启动一个 goroutine 来检查worker处理任务是否超时
	go c.catchTimeOut()

	return &c
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 创建Map任务
func (c *Coordinator) createMapTask() {
	// 遍历输入文件列表，为每个文件创建一个Map任务
	for i, file := range c.InputFiles {
		task := &Task{
			Filename: file,
			TaskType: Map,
			TaskID:   i,
			NReducer: c.NReduce,
		}
		c.Tasks <- task // 将任务放入任务队列
		c.TaskMeta[i] = &CoordinatorTask{
			TaskStatus: Idle,
			TaskMeta:   task,
		}
	}
}

// 创建Reduce任务
func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTask) // 清空任务元数据
	for i, files := range c.IntermediateFiles {
		task := &Task{
			TaskType:         Reduce,
			NReducer:         c.NReduce,
			TaskID:           i,
			IntermediateFile: files,
		}
		c.Tasks <- task // 将任务放入任务队列
		c.TaskMeta[i] = &CoordinatorTask{
			TaskStatus: Idle,
			TaskMeta:   task,
		}
	}
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second) // 每5秒检查一次
		mu.Lock()                   // 获取锁，防止并发访问
		if c.CoordinatorState == Exit {
			mu.Unlock() // 释放锁
			return      // 如果协调器状态为 Exit，退出函数
		}
		for _, task := range c.TaskMeta {
			if task.TaskStatus == InProgress && time.Since(task.StartTime) > 10*time.Second {
				// 如果任务状态为 InProgress 且超过10秒未完成，则重置任务状态为 Idle
				task.TaskStatus = Idle
				// log.Printf("Task %d timed out, resetting to Idle state\n", taskID)
				// 将任务重新放回任务队列
				c.Tasks <- task.TaskMeta
			}
		}
		mu.Unlock() // 释放锁
	}
}

// 等待Worker通过RPC远程调用获取task
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	mu.Lock()         // 获取锁，防止并发访问
	defer mu.Unlock() // 确保函数结束时释放锁

	// 从任务队列中获取一个任务
	if len(c.Tasks) > 0 { // 有任务
		reply.Task = new(Task)   // 创建一个新的Task实例
		*reply.Task = *<-c.Tasks // 将任务从通道中取出并赋值给回复
		// 更新task状态和记录时间，时间用于后续判断处理是否超时
		c.TaskMeta[reply.Task.TaskID].TaskStatus = InProgress
		c.TaskMeta[reply.Task.TaskID].StartTime = time.Now()
	} else if c.CoordinatorState == Exit { // 判断服务是否完成，需要退出
		reply.Task = &Task{TaskType: Exit} // 返回一个Exit类型的任务
	} else { // 没有任务
		reply.Task = &Task{TaskType: Wait} // 返回一个Wait类型的任务
	}
	return nil
}

func (c *Coordinator) TaskCompleted(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	mu.Lock()         // 获取锁，防止并发访问
	defer mu.Unlock() // 确保函数结束时释放锁

	// 更新任务状态为已完成
	if args.Task.TaskType != c.CoordinatorState ||
		c.TaskMeta[args.Task.TaskID].TaskStatus == Completed {
		// 如果传入的task类型与协调器状态不匹配，或者任务已经完成(重复)，则直接返回
		return nil // 如果任务类型不匹配或已完成，则直接返回
	}
	c.TaskMeta[args.Task.TaskID].TaskStatus = Completed // 更新任务状态为已完成

	// 启动一个新的goroutine来处理任务完成后的逻辑
	go c.processTaskResult(args.Task)
	return nil
}

// 处理task完成后的逻辑
// 主要是更新协调器状态
func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()         // 获取锁，防止并发访问
	defer mu.Unlock() // 确保函数结束时释放锁

	// 根据任务类型更新协调器状态
	switch task.TaskType {
	case Map:
		// Map任务完成后，将次task的中间文件添加到协调器的中间文件列表
		for reduceTaskID, file := range task.IntermediateFile {
			c.IntermediateFiles[reduceTaskID] = append(c.IntermediateFiles[reduceTaskID], file)
		}

		if c.allTaskDone() {
			c.createReduceTask()        // 所有Map任务完成后，创建Reduce任务
			c.CoordinatorState = Reduce // 所有Map任务完成后，切换到Reduce阶段
		}
	case Reduce:
		// Reduce任务完成后，更新协调器状态
		if c.allTaskDone() {
			c.CoordinatorState = Exit // 所有任务完成后，切换到Exit状态
			// log.Println("All tasks completed, coordinator exiting.")
		}
	}
}

// 检查所有任务是否都已完成
func (c *Coordinator) allTaskDone() bool {
	// 检查所有任务是否都已完成
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
