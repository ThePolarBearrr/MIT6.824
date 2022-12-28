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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := MRArgs{}
		reply := MRReply{}
		call("Coordinator.ApplyForWork", &args, &reply)

		if reply.TaskType == MAP_TASK {
			inputs := make([]KeyValue, 0)

			// 读input文件
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("can not open %s", reply.FileName)
			}
			contents, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("can not read %s", reply.FileName)
			}
			file.Close()

			keyValue := mapf(reply.FileName, string(contents))
			inputs = append(inputs, keyValue...)

			// 负载均衡
			buckets := make([][]KeyValue, reply.NReduceTasks)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range inputs {
				index := ihash(kv.Key) % reply.NReduceTasks
				buckets[index] = append(buckets[index], kv)
			}

			// 写临时文件再rename，保证原子性
			for i, kvs := range buckets {
				fName := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i)
				tempFile, err := ioutil.TempFile("", fName+"*")
				if err != nil {
					log.Fatalf("can not make temp file")
				}
				enc := json.NewEncoder(tempFile)
				for _, kv := range kvs {
					enc.Encode(kv)
				}
				if err = os.Rename(tempFile.Name(), fName); err != nil {
					log.Fatalf("rename temp file failed")
				}
				tempFile.Close()
			}

			respArgs := &MRArgs{
				FinishedMapTaskNumber:    reply.MapTaskNumber,
				FinishedReduceTaskNumber: -1,
			}
			respReply := &MRReply{}
			call("Coordinator.FinishedMapWork", respArgs, respReply)

		} else if reply.TaskType == REDUCE_TASK {
			outputs := make([]KeyValue, 0)
			// 读map写的文件，这里是多个map task经过hash之后处理某个index
			for i := 0; i < reply.NMapTasks; i++ {
				fName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				file, err := os.Open(fName)
				if err != nil {
					log.Fatalf("can not open %s", fName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					outputs = append(outputs, kv)
				}
				file.Close()
			}

			sort.Slice(outputs, func(i, j int) bool {
				if outputs[i].Key < outputs[j].Key {
					return true
				}
				return false
			})

			oName := "mr-out-" + strconv.Itoa(reply.ReduceTaskNumber)
			file, err := ioutil.TempFile("", oName+"*")
			if err != nil {
				log.Fatalf("can not make temp file")
			}

			// reduce
			for i := 0; i < len(outputs); i++ {
				values := []string{outputs[i].Value}

				for i+1 < len(outputs) && outputs[i+1].Key == outputs[i].Key {
					i++
					values = append(values, outputs[i].Value)
				}

				fmt.Fprintf(file, "%s %s\n", outputs[i].Key, reducef(outputs[i].Key, values))
			}

			if err = os.Rename(file.Name(), oName); err != nil {
				log.Fatalf("rename temp file falied")
			}
			file.Close()

			for i := 0; i < reply.NMapTasks; i++ {
				fName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				if err := os.Remove(fName); err != nil {
					log.Fatalf("can not delete %s", fName)
				}
			}

			respArgs := &MRArgs{
				FinishedMapTaskNumber:    -1,
				FinishedReduceTaskNumber: reply.ReduceTaskNumber,
			}
			respReply := &MRReply{}
			call("Coordinator.FinishedReduceWork", respArgs, respReply)
		} else if reply.TaskType == WAIT_TASK {
			time.Sleep(time.Second)
		} else if reply.TaskType == INVALID_TASK {
			continue
		} else if reply.TaskType == FINISH_TASK {
			return
		} else {
			panic("unexpect task type")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
