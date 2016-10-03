package mapreduce

import "container/list"
import "fmt"
//import "sync"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	available bool
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
	DoJobRoutine := func(w *WorkerInfo, jobArgs *DoJobArgs) {
		var jobReply DoJobReply
		ok := call(w.address, "Worker.DoJob", jobArgs, &jobReply)
		if ok == false {
			fmt.Printf("DoJob %d error\n", jobArgs.JobNumber)
		}   else {
			fmt.Printf("DoJob %d done\n", jobArgs.JobNumber)
		}
		mr.nextAvailWorker <- w.address
	}

	fmt.Println("Start running master")

	// process new worker registration
	go func() {
		for {
			worker := <-mr.registerChannel
			mr.Workers[worker] = &WorkerInfo{address: worker, available: true}
			mr.nAvailable++
			fmt.Printf("Worker %v is registered\n", worker)
			mr.nextAvailWorker <- worker
			//mr.nextAvailWorker <- worker
		}
	}()


	// start the map phase
	for job := 0; job < mr.nMap; job++ {
		availWorker := <-mr.nextAvailWorker
			// do map job
		w := mr.Workers[availWorker]
		mapArgs := &DoJobArgs{mr.file, Map, job, mr.nReduce}
		go DoJobRoutine(w, mapArgs)
	}
	// start the reduce phase
	for job := 0; job < mr.nReduce; job++ {
        availWorker := <-mr.nextAvailWorker
            // do map job
        w := mr.Workers[availWorker]
        reduceArgs := &DoJobArgs{mr.file, Reduce, job, mr.nMap}
		go DoJobRoutine(w, reduceArgs)
	}
	return mr.KillWorkers()
}
