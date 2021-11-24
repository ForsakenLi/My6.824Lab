package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mapFiles          []string
	mapResFiles       [][]string
	reduceTargetFiles [][]string
	resultFiles       []string
	inMapJob          bool
	allJobDone        bool
	mapJobState       map[int]int
	reduceJobState    map[int]int
	// JobState: if value is 100, means job is finished, if value is 0
	// job is not been assigned or failed(need to be assigned again),
	// if value is 0~10, which is a timer, reduce
	// itself 1 per second.
	// In a word, Master only assigned those job whose state value is 0.
	wg              sync.WaitGroup
	mutex           sync.RWMutex
	mutex2          sync.RWMutex // for reduce job
	timerCounter    int
	reduceWorkerNum int
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
			finishedNumber := 0
			for k, v := range m.mapJobState {
				if v > 0 && v <= 10 {
					isAllFinished := true
					for _, f := range m.mapResFiles[k] {
						if exist, err := pathExists(f); exist == false || err != nil {
							isAllFinished = false
							break
						}
					}
					if !isAllFinished {
						m.mutex.Lock()
						m.mapJobState[k] = v - 1
						m.mutex.Unlock()
					} else {
						m.mutex.Lock()
						m.mapJobState[k] = 100
						m.mutex.Unlock()
					}
				} else if v == 100 {
					finishedNumber++
				}
			}
			if finishedNumber == len(m.mapFiles) {
				m.inMapJob = false
			}
		} else {
			finishedNumber := 0
			for k, v := range m.reduceJobState {
				if v > 0 && v <= 10 {
					finished := true
					reduceFileName := "mr-out-" + strconv.Itoa(k)
					if exist, err := pathExists(reduceFileName); exist == false || err != nil {
						finished = false
					}
					if !finished {
						m.mutex.Lock()
						m.reduceJobState[k] = v - 1
						m.mutex.Unlock()
					} else {
						m.mutex.Lock()
						m.reduceJobState[k] = 100
						m.mutex.Unlock()
					}
				} else if v == 100 {
					finishedNumber++
				}
			}
			if finishedNumber == m.reduceWorkerNum {
				m.allJobDone = true
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
	return m.allJobDone
}

// GetJob worker call this method, must mutli-thread safe
// 无论是MapJob 还是 ReduceJob 都通过该方法分配任务
// Map: 进1出n Reduce: 进n出1
// Hang == true时表示当前暂时无任务分配，需要worker等待一段时间再尝试
func (m *Master) GetJob(args *WorkerArgs, allocation *JobAllocation) error {
	if allocation == nil {
		return errors.New("nil pointer error")
	}
	if m.allJobDone {
		allocation.AllJobDone = true
		return nil
	}
	if m.inMapJob {
		for k, v := range m.mapJobState {
			if v == 0 {
				allocation.InputFiles = []string{m.mapFiles[k]}
				allocation.IsMapJob = true
				allocation.OutputFile = m.mapResFiles[k]
				allocation.Hang = false
				allocation.JobId = k
				allocation.NReduce = m.reduceWorkerNum
				m.mutex.Lock()
				m.mapJobState[k] = 10 //开始计时
				m.mutex.Unlock()
				return nil
			}
		}
		allocation.Hang = true
		return nil
	} else {
		// 显然 reduce也需要一个State计时器来跟踪任务状态
		for k, v := range m.reduceJobState {
			if v == 0 {
				allocation.InputFiles = m.reduceTargetFiles[k]
				allocation.IsMapJob = false
				allocation.OutputFile = []string{"mr-out-" + strconv.Itoa(k)}
				allocation.Hang = false
				allocation.JobId = k
				allocation.NReduce = m.reduceWorkerNum
				m.mutex2.Lock()
				m.reduceJobState[k] = 10 //开始计时
				m.mutex2.Unlock()
				return nil
			}
		}
		allocation.Hang = true
		return nil
	}
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
	m.reduceWorkerNum = nReduce
	// Your code here.
	m.mapFiles = files
	// mabJob number is len(files)
	m.mapResFiles, m.reduceTargetFiles = genMapResList(len(files), nReduce)
	//m.mapIndex = 0
	//m.reduceFiles = make([]string, 0)
	m.mapJobState = make(map[int]int)
	for i := 0; i < len(files); i++ {
		m.mapJobState[i] = 0
	}
	m.reduceJobState = make(map[int]int)
	for i := 0; i < nReduce; i++ {
		m.reduceJobState[i] = 0
	}
	m.inMapJob = true
	m.wg = sync.WaitGroup{}
	m.mutex = sync.RWMutex{}
	m.mutex2 = sync.RWMutex{}
	m.server()
	return &m
}

func genMapResList(nMap, nReduce int) ([][]string, [][]string) {
	mapResFiles := make([][]string, 0)
	reduceTarFiles := make([][]string, 0)
	//将mapResFiles中的结果排列为以reduce为第一级index
	for j := 0; j < nReduce; j++ {
		l := make([]string, 0)
		for i := 0; i < nMap; i++ {
			l = append(l, "interm-"+strconv.Itoa(i)+"-"+strconv.Itoa(j))
		}
		reduceTarFiles = append(reduceTarFiles, l)
	}
	for i := 0; i < nMap; i++ {
		l := make([]string, 0)
		for j := 0; j < nReduce; j++ {
			l = append(l, "interm-"+strconv.Itoa(i)+"-"+strconv.Itoa(j))
		}
		mapResFiles = append(mapResFiles, l)
	}
	return mapResFiles, reduceTarFiles
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
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
