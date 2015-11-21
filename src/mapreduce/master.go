package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	idleWorder := make(chan string)
	mapChan := make(chan int)
	reduceChan := make(chan int)
	for i := 0; i < mr.nMap; i++ {
		go func(mapN int) {
			for {
				var worker string
				success := false
				select {
				case worker = <-mr.registerChannel:
					mr.Workers[worker] = &WorkerInfo{address: worker}
					jobArgs := DoJobArgs{File: mr.file, Operation: Map, JobNumber: mapN, NumOtherPhase: mr.nReduce}
					var reply DoJobReply
					success = call(worker, "Worker.DoJob", jobArgs, &reply)
				case worker = <-idleWorder:
					jobArgs := DoJobArgs{File: mr.file, Operation: Map, JobNumber: mapN, NumOtherPhase: mr.nReduce}
					var reply DoJobReply
					success = call(worker, "Worker.DoJob", jobArgs, &reply)
				}
				if success {
					mapChan <- mapN
					idleWorder <- worker
					return
				} else {
					delete(mr.Workers, worker)
				}
			}
		}(i)
	}
	for i := 0; i < mr.nMap; i++ {
		<-mapChan
	}
	for i := 0; i < mr.nReduce; i++ {
		go func(reduceN int) {
			for {
				var worker string
				success := false
				select {
				case worker = <-mr.registerChannel:
					mr.Workers[worker] = &WorkerInfo{address: worker}
					jobArgs := DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: reduceN, NumOtherPhase: mr.nMap}
					var reply DoJobReply
					success = call(worker, "Worker.DoJob", jobArgs, &reply)
				case worker = <-idleWorder:
					jobArgs := DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: reduceN, NumOtherPhase: mr.nMap}
					var reply DoJobReply
					success = call(worker, "Worker.DoJob", jobArgs, &reply)
				}
				if success {
					reduceChan <- reduceN
					idleWorder <- worker
					return
				} else {
					delete(mr.Workers, worker)
				}
			}
		}(i)
	}
	fmt.Println("waiting for reduce done!")
	for i := 0; i < mr.nReduce; i++ {
		<-reduceChan
	}
	fmt.Println("reduce done with living worker", len(mr.Workers))
	// consume idle workers...
	for i := 0; i < len(mr.Workers); i++ {
		fmt.Println(<-idleWorder)
	}
	return mr.KillWorkers()
}
