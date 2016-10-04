package mapreduce

import "container/list"
import "fmt"
import "sync"

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
	var mutex sync.Mutex
	cond_v := sync.NewCond(&mutex)
	// Go routine for doing map/reduce jobs
	DoJobRoutine := func(w *WorkerInfo, jobArgs *DoJobArgs, cJobs int) {
		var jobReply DoJobReply
		ok := call(w.address, "Worker.DoJob", jobArgs, &jobReply)
		if ok == false || jobReply.OK == false{
			mr.unfinishedJobs <- jobArgs.JobNumber
		} else {
			mutex.Lock()
			mr.nCompleted++
			cond_v.Signal()
			mutex.Unlock()
			mr.nextAvailWorker <- w.address
		}
	}

	fmt.Println("Start running master")

	// process new worker registration
	go func() {
		for {
			worker := <-mr.registerChannel
			mr.Workers[worker] = &WorkerInfo{address: worker}
			fmt.Printf("Worker %v is registered\n", worker)
			mr.nextAvailWorker <- worker
		}
	}()

	var nextJob int
	// start the map phase
	for job := 0; job < mr.nMap+1; job++ {
		select {
		case nextJob = <-mr.unfinishedJobs:
			job--
		default:
			nextJob = job
		}
		if nextJob >= mr.nMap {
			fmt.Printf("Completed: %d, Required: %d\n", mr.nCompleted, mr.nMap)
			if mr.nCompleted == nMap {
				break
			} else {
				mutex.Lock()
				cond_v.Wait()
				mutex.Unlock()
				continue
			}
		}
		availWorker := <-mr.nextAvailWorker
		// do map job
		w := mr.Workers[availWorker]
		mapArgs := &DoJobArgs{mr.file, Map, nextJob, mr.nReduce}
		go DoJobRoutine(w, mapArgs, nMap)
	}

	mr.nCompleted = 0
	// start the reduce phase
	for job := 0; job < mr.nReduce+1; job++ {
		select {
		case nextJob = <-mr.unfinishedJobs:
			job--
		default:
			nextJob = job
		}
		if nextJob >= mr.nReduce {
			if mr.nCompleted == nReduce {
				break
			} else {
				mutex.Lock()
				cond_v.Wait()
				mutex.Unlock()
				continue
			}
		}
		availWorker := <-mr.nextAvailWorker
		// do reduce job
		w := mr.Workers[availWorker]
		reduceArgs := &DoJobArgs{mr.file, Reduce, nextJob, mr.nMap}
		go DoJobRoutine(w, reduceArgs, nReduce)
	}
	//wg.Wait()

	return mr.KillWorkers()
}
