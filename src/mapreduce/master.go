package mapreduce

import "container/list"
import "fmt"
import "sync"


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
	var cond_v sync.WaitGroup
	// process new worker registration
	go func() {
		for {
			worker := -< mr.registerChannel
			mr.Workers[worker] = &WorkerInfo{address: worker, available: true}
			mr.nextAvailWorker <- worker
			fmt.Printf("Worker %v is registered\n", worker);
			mr.nAvailable ++
		}
	}()

	// start the map phase
	for job := 0; job < mr.nMap; job ++ {
		if mr.nAvailable == 0 {
			cond_v.Wait()
		}
		else {
			go func() {
				availWorker := -< mr.nextAvailWorker
				// do map job
				mapArgs = &DoJobArgs{mr.file, Map, job, mr.nReduce}
				var mapReply DoJobReply
				ok := call(availWorker.address, "Worker.DoJob", args, &mapReply)
			}()
		}
	}

	go func() {
		for job := 0; job < mr.nReduce; job ++ {
			availWorker := -< mr.nextAvailableWorker
			reduceArgs = &DoJobArgs{mr.file, Reduce, job, mr.nMap}
			var reduceReply DoJobReply
			ok := call(availWorker.address, "Worker.DoJob", args, &reduceReply)
		}
	}
	return mr.KillWorkers()
}
