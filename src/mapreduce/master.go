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
	idleWKChannel := make(chan string)
	jobChannel := make(chan *DoJobArgs)
	doneChannel := make(chan int)

	defer close(doneChannel)
	defer close(jobChannel)

	getNextWorkerAddr := func() string {
		var addr string
		select {
		case addr = <-mr.registerChannel:
			mr.Workers[addr] = &WorkerInfo{addr}
		case addr = <-idleWKChannel:
		}
		return addr
	}

	sendJob := func(workeraddr string, job *DoJobArgs) {
		var reply DoJobReply
		ok := call(workeraddr, "Worker.DoJob", job, &reply)
		if ok {
			// order of channel matters, doneChannel blocks main thread
			// while idleWKChannel blocks non-main ones
			doneChannel <- 1
			idleWKChannel <- workeraddr
		} else {
			jobChannel <- job
		}
	}
	// distribute jobs to available workers
	go func() {
		for job := range jobChannel {
			workeraddr := getNextWorkerAddr()
			go sendJob(workeraddr, job)
		}
	}()

	go func() {
		for i := 0; i < mr.nMap; i++ {
			job := &DoJobArgs{mr.file, Map, i, mr.nReduce}
			jobChannel <- job
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		<-doneChannel
	}

	go func() {
		for i := 0; i < mr.nReduce; i++ {
			job := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
			jobChannel <- job
		}
	}()

	for i := 0; i < mr.nReduce; i++ {
		<-doneChannel
	}

	return mr.KillWorkers()
}
