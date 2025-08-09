package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.5840/mr"
)

func main() {
	if len(os.Args) < 2 { // 命令行参数必须包含输入文件
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// 创建一个协调器实例，传入输入文件列表和reduce任务数
	m := mr.MakeCoordinator(os.Args[1:], 10)
	// 主 goroutine 进入一个简单的轮询
	// m.Done() 返回 true 表示所有 Map 和 Reduce 阶段都已完成
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
