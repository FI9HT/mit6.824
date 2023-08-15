package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KV

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for true {
		ret := CallFetchTask(mapf, reducef)
		if ret == 0 {
			continue
		} else if ret == -1 {
			time.Sleep(1 * time.Second)
			//fmt.Println("worker 进程等待1s后重新发起请求")
			continue
		} else if ret == -2 {
			//fmt.Println("worker 进程退出")
			break
		}
	}
}

func CallSubmitTask(taskSeq int, taskKind int, files []string) {
	args := SubmitTaskArgs{taskSeq, taskKind, files}
	reply := SubmitTaskArgs{}

	ok := call("Coordinator.SubmitTask", &args, &reply)
	if ok {
		//fmt.Printf("CallSubmitTask success. task [%d]\n", taskSeq)
	} else {
		//fmt.Println("CallSubmitTask error")
	}
}

// CallFetchTask
// return
//	-2:exit,
//	-1:waiting 1s and try again
//	0: process success

func CallFetchTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) int {

	args := FetchTaskArgs{}
	reply := FetchTaskReply{}

	ok := call("Coordinator.FetchTask", &args, &reply)
	if ok {
		//fmt.Printf("CallFetchTask success, result [%d] kind [%d] filename [%s]\n", reply.Result, reply.Kind, reply.Filename)
	} else {
		fmt.Println("CallFetchTask error")
		return 0
	}
	// 需要等待或者退出，直接返回
	if reply.Result == -1 || reply.Result == -2 {
		return reply.Result
	}

	if reply.Kind == 0 {
		// 处理map任务
		reduceFiles := processMapTask(reply.Result, reply.Filename[0], mapf)
		// 处理map任务完成，调用rpc返回结果
		CallSubmitTask(reply.Result, reply.Kind, reduceFiles)
	} else if reply.Kind == 1 {
		// 处理reduce任务
		processReduceTask(reply.Result, reply.Filename, reducef)
		// 完成
		CallSubmitTask(reply.Result, reply.Kind, nil)
	} else {
		//fmt.Println("unknown task kind")
		return -1
	}
	return 0
}

// 原始版本 速度慢
//func processMapTask(mapTaskSeq int, fileName string,
//	mapf func(string, string) []KeyValue) []string {
//
//	fmt.Printf("开始处理map任务 [%d]\n", mapTaskSeq)
//	startTime := time.Now()
//
//	file, err := os.Open(fileName)
//	if err != nil {
//		fmt.Println("os.Open error")
//	}
//	content, err := io.ReadAll(file)
//	if err != nil {
//		fmt.Println("io.ReadAll error")
//	}
//	err = file.Close()
//	if err != nil {
//		fmt.Println("file close error")
//	}
//	// 获取文件的所有词语kv
//	kva := mapf(fileName, string(content))
//
//	type FileEncode struct {
//		file    *os.File
//		encoder *json.Encoder
//	}
//
//	// K: 生成的文件名，V:FileEncode
//	//reduceFilesMap := make(map[string]FileEncode)
//
//	// 先把所有需要写入的文件统一open，然后统一close，而不是每个单词都打开一次文件，速度会很慢
//
//	var mrfileName string
//
//	//for _, kv := range kva {
//	//	reduceN := ihash(kv.Key) % 10 // nReduce
//	//	mrfileName = fmt.Sprintf("mr-%d-%d", mapTaskSeq, reduceN)
//	//	if reduceFilesMap[mrfileName].file == nil {
//	//		// 打开新文件名
//	//		file, err := os.OpenFile(mrfileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
//	//		if err != nil {
//	//			fmt.Println("open file error")
//	//		}
//	//		enc := json.NewEncoder(file)
//	//		reduceFilesMap[mrfileName] = FileEncode{file, enc}
//	//	}
//	//}
//	reduceFilesMap := make(map[string]bool)
//
//	for _, kv := range kva {
//		reduceN := ihash(kv.Key) % 10 // nReduce
//		mrfileName = fmt.Sprintf("mr-%d-%d", mapTaskSeq, reduceN)
//		file, err := os.OpenFile(mrfileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
//		enc := json.NewEncoder(file)
//		err = enc.Encode(kv)
//		//err = reduceFilesMap[mrfileName].encoder.Encode(kv)
//
//		if err != nil {
//			fmt.Println("enc.Encode KV ", kv, " error")
//		}
//		err = file.Close()
//		if err != nil {
//			fmt.Println("file close error")
//		}
//		if reduceFilesMap[mrfileName] == false {
//			reduceFilesMap[mrfileName] = true
//		}
//	}
//	//for k, v := range reduceFilesMap {
//	//	err = v.file.Close()
//	//	if err != nil {
//	//		fmt.Printf("Close file[%d] error\n", k)
//	//	}
//	//}
//
//	//fmt.Println("processMapTask finish file: ", mrfileName)
//	//time.Sleep(10 * time.Second)
//
//	var reduceFiles []string
//	for k, _ := range reduceFilesMap {
//		reduceFiles = append(reduceFiles, k)
//	}
//
//	endTime := time.Now()
//	duration := endTime.Sub(startTime).Seconds()
//	fmt.Printf("map任务[%d]处理完成，用时[%v]s\n", mapTaskSeq, duration)
//
//	return reduceFiles
//}

func processMapTask(mapTaskSeq int, fileName string,
	mapf func(string, string) []KeyValue) []string {

	//fmt.Printf("开始处理map任务 [%d]\n", mapTaskSeq)
	//startTime := time.Now()

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("os.Open error")
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("io.ReadAll error")
	}
	err = file.Close()
	if err != nil {
		fmt.Println("file close error")
	}
	// 获取文件的所有词语kv
	kva := mapf(fileName, string(content))

	type FileEncode struct {
		file    *os.File
		encoder *json.Encoder
	}

	// K: 生成的文件名，V:FileEncode
	reduceFilesMap := make(map[string]FileEncode)

	// 先把所有需要写入的文件统一open，然后统一close，而不是每个单词都打开一次文件，速度会很慢
	var mrfileName string

	for _, kv := range kva {
		reduceN := ihash(kv.Key) % 10 // nReduce
		mrfileName = fmt.Sprintf("mr-%d-%d", mapTaskSeq, reduceN)
		if reduceFilesMap[mrfileName].file == nil {
			// 打开新文件名
			//file, err := os.OpenFile(mrfileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			// 每次打开都清空，防止之前crash的worker写的残留内容
			file, err := os.OpenFile(mrfileName, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				fmt.Println("open file error")
			}
			enc := json.NewEncoder(file)
			reduceFilesMap[mrfileName] = FileEncode{file, enc}
		}
	}

	for _, kv := range kva {
		reduceN := ihash(kv.Key) % 10 // nReduce
		mrfileName = fmt.Sprintf("mr-%d-%d", mapTaskSeq, reduceN)
		err = reduceFilesMap[mrfileName].encoder.Encode(kv)

		if err != nil {
			fmt.Println("enc.Encode KV ", kv, " error")
		}
	}
	for k, v := range reduceFilesMap {
		err = v.file.Close()
		if err != nil {
			fmt.Printf("Close file[%d] error\n", k)
		}
	}

	var reduceFiles []string
	for k, _ := range reduceFilesMap {
		reduceFiles = append(reduceFiles, k)
	}

	//endTime := time.Now()
	//duration := endTime.Sub(startTime).Seconds()
	//fmt.Printf("map任务[%d]处理完成，用时[%v]s\n", mapTaskSeq, duration)

	return reduceFiles
}

//func decodeJsonFile(file string, kva *[]KV) {
//	openFile, err := os.OpenFile(file, os.O_RDONLY, 0644)
//	if err != nil {
//		fmt.Println("os.OpenFile error")
//	}
//	dec := json.NewDecoder(openFile)
//	for {
//		var kv KV
//		if err := dec.Decode(&kv); err != nil {
//			break
//		}
//		kva = append(kva, kv)
//	}
//	err = openFile.Close()
//	if err != nil {
//		fmt.Println("openFile close error")
//	}
//}

// 原始版本，速度慢
//func processReduceTask(reduceTaskSeq int, files []string,
//	reducef func(string, []string) string) {
//
//	fmt.Printf("开始处理reduce任务 [%d]\n", reduceTaskSeq)
//	startTime := time.Now()
//
//	// 读取所有文件的kv
//	kva := []KV{}
//	for _, file := range files {
//		openFile, err := os.OpenFile(file, os.O_RDONLY, 0644)
//		if err != nil {
//			fmt.Println("os.OpenFile error")
//		}
//		dec := json.NewDecoder(openFile)
//		for {
//			var kv KV
//			if err := dec.Decode(&kv); err != nil {
//				break
//			}
//			kva = append(kva, kv)
//		}
//		err = openFile.Close()
//		if err != nil {
//			fmt.Println("openFile close error")
//		}
//	}
//
//	endTime1 := time.Now()
//	duration := endTime1.Sub(startTime).Seconds()
//	fmt.Printf("reduce任务[%d]处理1/3，用时[%v]s\n", reduceTaskSeq, duration)
//
//	sort.Sort(ByKey(kva))
//	outputFileName := fmt.Sprintf("mr-out-%d", reduceTaskSeq)
//	openOutputFile, _ := os.Create(outputFileName)
//
//	endTime2 := time.Now()
//	duration = endTime2.Sub(endTime1).Seconds()
//	fmt.Printf("reduce任务[%d]处理2/3，用时[%v]s\n", reduceTaskSeq, duration)
//
//	i := 0
//	for i < len(kva) {
//		j := i + 1
//		for j < len(kva) && kva[j].Key == kva[i].Key {
//			j++
//		}
//		values := []string{}
//		for k := i; k < j; k++ {
//			values = append(values, kva[k].Value)
//		}
//		output := reducef(kva[i].Key, values)
//
//		// this is the correct format for each line of Reduce output.
//		fmt.Fprintf(openOutputFile, "%v %v\n", kva[i].Key, output)
//
//		i = j
//	}
//	endTime3 := time.Now()
//	duration = endTime3.Sub(endTime2).Seconds()
//	fmt.Printf("reduce任务[%d]处理3/3，用时[%v]s\n", reduceTaskSeq, duration)
//
//	openOutputFile.Close()
//}

func processReduceTask(reduceTaskSeq int, files []string,
	reducef func(string, []string) string) {

	//fmt.Printf("开始处理reduce任务 [%d]\n", reduceTaskSeq)
	//startTime := time.Now()

	// 读取所有文件的kv
	kva := []KV{}
	for _, file := range files {
		openFile, err := os.OpenFile(file, os.O_RDONLY, 0644)
		if err != nil {
			fmt.Println("os.OpenFile error")
		}
		dec := json.NewDecoder(openFile)
		for {
			var kv KV
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = openFile.Close()
		if err != nil {
			fmt.Println("openFile close error")
		}
	}

	//endTime1 := time.Now()
	//duration := endTime1.Sub(startTime).Seconds()
	//fmt.Printf("reduce任务[%d]处理1/3，用时[%v]s\n", reduceTaskSeq, duration)

	sort.Sort(ByKey(kva))
	outputFileName := fmt.Sprintf("mr-out-%d", reduceTaskSeq)
	// openOutputFile, _ := os.Create(outputFileName)
	openOutputFile, _ := os.CreateTemp("./", outputFileName+"_tmp_*")

	//endTime2 := time.Now()
	//duration = endTime2.Sub(endTime1).Seconds()
	//fmt.Printf("reduce任务[%d]处理2/3，用时[%v]s\n", reduceTaskSeq, duration)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(openOutputFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	//endTime3 := time.Now()
	//duration = endTime3.Sub(endTime2).Seconds()
	//fmt.Printf("reduce任务[%d]处理3/3，用时[%v]s\n", reduceTaskSeq, duration)

	err := openOutputFile.Close()
	if err != nil {
		fmt.Println("close file error")
	}

	// 写入完成，修改文件名字
	err = os.Rename(openOutputFile.Name(), outputFileName)
	if err != nil {
		fmt.Println("Rename error")
	}
}

// CallExample example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
