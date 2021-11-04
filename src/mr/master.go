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
	"sync"
	"testing/quick"
	"time"
)

type Master struct {
	// Your definitions here.
	mapFiles    []string
	reduceFiles []string
	inMapJob    bool
	mapJobState map[int]int
	// mapJobState: if value is 100, means job is finished, if value is 0
	// job is not been assigned or failed(need to be assigned again), 
	// if value is 0~10, which is a timer, reduce
	// itself 1 per second.
	wg           sync.WaitGroup
	mutex        sync.RWMutex
	timerCounter int
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
	go timer(m)
}

func timer(m *Master) {
	m.timerCounter++
	if m.timerCounter > 1 {
		return
	}
	for {
		if m.inMapJob {
			for k, v := range m.mapJobState {
				if v > 0 && v <= 10 {
					m.mutex.Lock()
					m.mapJobState[k] = v - 1
					m.mutex.Unlock()
				}
			}
		}
		time.Sleep(time.Second)
	}

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

// worker call this method, must mutli-thread safe
func GetJob() (files []string, isMapJob bool, outputFile string) {
	// need open a gooroutine to follow the job sent to worker is finish or not

	// map job worker need to save the word to special reduce file(by ihash func)
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
// note: needless to concern whether worker is done the job or not,
// just to look out whether the map-res-file and reduce-res-file is ok
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	stdFiles, err := splitFiles(files, 10*nReduce)
	if err != nil {
		panic(err)
	}
	m.mapFiles = stdFiles
	//m.mapIndex = 0
	m.reduceFiles = make([]string, 0)
	m.inMapJob = true
	m.wg = sync.WaitGroup{}
	m.mutex = sync.RWMutex{}
	m.server()
	return &m
}

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
	mem := make([]byte, 0) // 暂存文件内容块
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
		if len(content)+len(mem) >= chunkSize {
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
