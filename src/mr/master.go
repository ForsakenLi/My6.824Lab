package mr

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)


type Master struct {
	// Your definitions here.
	taskFiles [][]string
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
// https://studygolang.com/articles/14336
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeMaster 构造函数
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	stdFiles := splitFiles(files, )
	
	
	m.server()
	return &m
}

// files可能存在数据倾斜问题, 需要拆分文件为更小粒度的文件
// 但拆分文件的逻辑比较复杂, 文件切分的位置需要不在一个单词中间
// 应该采用冗余执行的策略，如果某一个worker宕机，备用任务也可以完成
// 可参考MapReduce论文3.5节
// 拆分给定文件为若干子文件，子文件大小应尽可能一致
func splitFiles(inputFiles []string, splitNum int) ([]string, error) {
	var totalSize int
	for _, f := range inputFiles {
		stat, err := os.Stat(f)
		if err != nil {
			return nil, fmt.Errorf("[splitFiles.Stat]%v", err)
		}
		totalSize += int(stat.Size())
	}
	chunkSize := totalSize / splitNum
	mem := make([]byte, 0)	// 暂存文件内容块
	contentStart := 0
	mapFileCount := 0
	resFileList := make([]string, 0)
	for i := 0; i < len(inputFiles); {
		mapFileCount++
		file, err := os.Open(inputFiles[i])
		if err != nil {
			return nil, fmt.Errorf("[splitFiles.OpenFile]%v", err)
		}
		content, err := ioutil.ReadAll(file)
		content = content[contentStart:]
		if err != nil {
			return nil, fmt.Errorf("[splitFiles.ReadAll]%v", err)
		}
		if len(content) + len(mem) >= chunkSize {
			// save mem to file system
			endIndex := chunkSize - len(mem)
			for endIndex < len(content) && content[endIndex] != byte(' ') && content[endIndex] != byte('\n') {
				endIndex++
			}
			contentStart += endIndex
			mem = append(mem, content[:endIndex]...)
			mapFileName := "map-" + strconv.Itoa(mapFileCount)
			err := ioutil.WriteFile(mapFileName, mem, 0644)
			if err != nil {
				return nil, fmt.Errorf("[splitFiles.WriteFile]%v", err)
			}
			resFileList = append(resFileList, mapFileName)
			mem = make([]byte, 0)
		} else {
			mem = append(mem, content...)
			contentStart = 0
			i++
		}   
        file.Close()
	}
	// 输出残余mem块
	mapFileName := "map-" + strconv.Itoa(mapFileCount)
	err := ioutil.WriteFile(mapFileName, mem, 0644)
	if err != nil {
		return nil, fmt.Errorf("[splitFiles.WriteFile]%v", err)
	}
	resFileList = append(resFileList, mapFileName)
	return resFileList, nil
}
