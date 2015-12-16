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
	mapDoneChannel := make(chan int, mr.nMap)
	reduceDoneChannel := make(chan int, mr.nReduce)
	
	for i := 0; i < mr.nMap; i++ {
		go func (jobNumber int)  {
			for {
				// get the idle  worker
				worker := <- mr.idleWorkerChannel
				
				// set the jobargs and reply
				jobArgs := &DoJobArgs{}
				jobReply := &DoJobReply{}
				jobArgs.NumOtherPhase = mr.nReduce
				jobArgs.Operation = Map
				jobArgs.File = mr.file
				jobArgs.JobNumber = jobNumber
				
				// call worker.DoJob
				ok := call(worker, "Worker.DoJob", jobArgs, jobReply)
				if ok == true {
					mr.idleWorkerChannel <- worker
					mapDoneChannel <- jobNumber
					return
				}
			}
		}(i)
	}
	
	for i := 0; i < mr.nMap; i++ {
		<- mapDoneChannel
	}
	
	for i := 0; i < mr.nReduce; i++ {
		go func (jobNumber int)  {
			for {
				// get the idle  worker
				worker := <- mr.idleWorkerChannel
				
				// set the jobargs and reply
				jobArgs := &DoJobArgs{}
				jobReply := &DoJobReply{}
				jobArgs.NumOtherPhase = mr.nMap
				jobArgs.Operation = Reduce
				jobArgs.File = mr.file
				jobArgs.JobNumber = jobNumber
				
				// call worker.DoJob
				ok := call(worker, "Worker.DoJob", jobArgs, jobReply)
				if ok == true {
					mr.idleWorkerChannel <- worker
					reduceDoneChannel <- jobNumber
					return
				}
			}
		}(i)
	}
	
	for i := 0; i < mr.nReduce; i++ {
		<- reduceDoneChannel
	}
	
	fmt.Println("Jobs are all done.")
	
	return mr.KillWorkers()
}
