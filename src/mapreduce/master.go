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
	fmt.Println("Start running master")
	var mutex sync.RWMutex

	// process new worker registration
	go func() {
		for {
			worker := <-mr.registerChannel
			mutex.Lock()
			mr.Workers[worker] = &WorkerInfo{address: worker, available: true}
			mr.nAvailable++
			fmt.Printf("Worker %v is registered\n", worker)
			mutex.Unlock()
			mr.nextAvailWorker <- worker
		}
	}()

	// start the map phase
	for job := 0; job < mr.nMap; job++ {
		for mr.nAvailable == 0 {
		}
		go func(job_id int) {
			availWorker := <-mr.nextAvailWorker
			// do map job
			mutex.Lock()
			mr.nAvailable--
			mutex.Unlock()
			w := mr.Workers[availWorker]
			mapArgs := &DoJobArgs{mr.file, Map, job_id, mr.nReduce}
			var mapReply DoJobReply
			ok := call(w.address, "Worker.DoJob", mapArgs, &mapReply)
			if ok == false {
				fmt.Printf("DoJob Map job %d error\n", job_id)
			}	else {
				fmt.Printf("DoJob Map job %d done\n", job_id)
			}
			mr.nextAvailWorker <- availWorker
			mutex.Lock()
			mr.nAvailable++
			mutex.Unlock()
		}(job)
	}
	// start the reduce phase
	for job := 0; job < mr.nReduce; job++ {
		for mr.nAvailable == 0 {
		}
		go func(job_id int) {
			availWorker := <-mr.nextAvailWorker
			mutex.Lock()
			mr.nAvailable--
			mutex.Unlock()
			w := mr.Workers[availWorker]
			reduceArgs := &DoJobArgs{mr.file, Reduce, job_id, mr.nMap}
			var reduceReply DoJobReply
			ok := call(w.address, "Worker.DoJob", reduceArgs, &reduceReply)
			if ok == false {
				fmt.Printf("DoJob Reduce job %d error\n", job_id)
			}	else {
				fmt.Printf("DoJob Reduce job %d done\n", job_id)
			}
			mr.nextAvailWorker <- availWorker
			mutex.Lock()
			mr.nAvailable++
			mutex.Unlock()
		}(job)
	}
	return mr.KillWorkers()
}
