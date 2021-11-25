package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		allocation := GetJobFromMaster()
		for allocation.Hang == true {
			time.Sleep(time.Second)
			allocation = GetJobFromMaster()
		}
		if allocation.AllJobDone {
			return
		}
		// MapWork: save the map result to a temp-mapid-ihash file, reduce job will combine the
		// same ihash file to accelerate the reduce work
		// ReduceWork: use temp file to save intermediate output, then rename the file to
		// show the job is finished
		if allocation.IsMapJob {
			allKva := make([]KeyValue, 0)
			for _, filename := range allocation.InputFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %s", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %s", filename)
				}
				file.Close()
				kva := mapf(filename, string(content)) //kva中所有词频均为1
				allKva = append(allKva, kva...)
			}
			sort.Sort(ByKey(allKva))
			tempname := genTempName(allocation.JobId, len(allocation.OutputFile))
			pairsForJson := make([][]KeyValue, 0)
			for i := 0; i < len(allocation.OutputFile); i++ {
				pairsForJson = append(pairsForJson, make([]KeyValue, 0))
			}
			// hash to pairsForJson
			for _, pair := range allKva {
				hashNum := ihash(pair.Key) % allocation.NReduce
				pairsForJson[hashNum] = append(pairsForJson[hashNum], pair)
			}
			for i, pairs := range pairsForJson {
				// 用于暂存map结果的temp文件，在循环结束后分别rename到outputFile集合
				temp := tempname[i]
				var file *os.File
				if exist, err := pathExists(temp); exist == false || err != nil {
					err = nil
					file, err = os.Create(temp)
					if err != nil {
						log.Fatalf("cannot create %s", temp)
					}
				} else {
					err = nil
					file, err = os.Open(temp)
					if err != nil {
						log.Fatalf("cannot open %s", temp)
					}
				}
				enc := json.NewEncoder(file)
				err := enc.Encode(&pairs)
				if err != nil {
					log.Fatalf("cannot encode %s, err:%+v", pairs, err)
				}
				file.Close()
			}
			for i, temp := range tempname {
				err := os.Rename(temp, allocation.OutputFile[i])
				if err != nil {
					log.Fatalf("rename %s to %s failed", temp, allocation.OutputFile[i])
				}
			}
		} else {
			// reduce code
			kva := make([]KeyValue, 0)
			for _, filename := range allocation.InputFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %s", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv...)
				}
			}
			sort.Sort(ByKey(kva))
			outTempName := "temp_" + allocation.OutputFile[0] + strconv.FormatInt(time.Now().UnixNano(), 16)
			ofile, _ := os.Create(outTempName)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++	// 找到还是该词为止的intermediate下标
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)	//把所有"1"放到一个values切片里，交给reducef
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j	// 将i改为下一个单词下标起点
			}

			ofile.Close()

			err := os.Rename(outTempName, allocation.OutputFile[0])
			if err != nil {
				log.Fatalf("rename %v to %v failed", outTempName, allocation.OutputFile[0])
			}
		}
	}
}

func genTempName(jobId int, hashNum int) map[int]string {
	res := make(map[int]string)
	for i := 0; i < hashNum; i++ {
		res[i] = fmt.Sprintf("temp-%d-%d-%s", jobId, i, strconv.FormatInt(time.Now().UnixNano(), 16))
	}
	return res
}

func GetJobFromMaster() *JobAllocation {
	args := WorkerArgs{}

	res := JobAllocation{}

	call("Master.GetJob", &args, &res)

	fmt.Printf("Job Allocation: %+v", res)

	return &res
}

// CallExample
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
