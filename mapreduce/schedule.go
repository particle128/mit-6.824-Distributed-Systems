package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	taskIds := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		taskIds <- i
	}

	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)
	done := 0
forloop:
	for {
		cond.L.Lock()
		select {
		case taskId := <-taskIds:
			cond.L.Unlock()
			worker := <-registerChan
			go func() {
				defer func() {
					// !!! Sending to unbuffered channel must be the last statement.
					// !!! Because it will block the goroutine if the channel is not read from the other side.
					// !!! This is the case for the last tasks
					registerChan <- worker
				}()
				var mapFile string
				switch phase {
				case mapPhase:
					mapFile = mapFiles[taskId]
				case reducePhase:
					mapFile = ""
				}
				ret := call(worker, "Worker.DoTask", &DoTaskArgs{jobName, mapFile, phase, taskId, n_other}, nil)
				cond.L.Lock()
				if ret {
					done++
				} else {
					taskIds <- taskId
				}
				cond.Signal()
				cond.L.Unlock()
			}()
		default:
			if done != ntasks {
				cond.Wait()
			}
			// !!! Return from Wait(),
			// !!! either done == ntasks, or taskIds has elements
			if done == ntasks {
				cond.L.Unlock()
				break forloop
			}
			cond.L.Unlock()
		}
	}

	/*
		// !!! Ignore worker failure

		var wg sync.WaitGroup
		for i := 0; i < ntasks; i++ {
			worker := <-registerChan
			wg.Add(1)
			go func(taskId int) {
				defer func() {
					wg.Done()
					registerChan <- worker
				}()
				var mapFile string
				switch phase {
				case mapPhase:
					mapFile = mapFiles[taskId]
				case reducePhase:
					mapFile = ""
				}
				var reply struct{}
				call(worker,
					"Worker.DoTask",
					&DoTaskArgs{jobName, mapFile, phase, taskId, n_other},
					&reply)
			}(i)
		}
		wg.Wait()
	*/

	fmt.Printf("Schedule: %v phase done\n", phase)
}
