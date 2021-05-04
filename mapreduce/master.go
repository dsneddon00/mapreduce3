package main

/*
Split the input file and start an HTTP server to serve source chunks to map workers.
Generate the full set of map tasks and reduce tasks. Note that reduce tasks will be incomplete initially, because they require a list of the hosts that handled each map task.
Create and start an RPC server to handle incoming client requests. Note that it can use the same HTTP server that shares static files.
Serve tasks to workers. The master should issue map tasks to workers that request jobs, then wait until all map tasks are complete before it begins issuing reduce jobs.
Wait until all jobs are complete.
Gather the reduce outputs and join them into a single output file.
Tell all workers to shut down, then shut down the master.
*/

import (
  "fmt"
  "io/fs"
  "log"
  "os"
  "path/filepath"
)

func startMaster(client Interface, inputPath, outputPath string) error {
  // Split the input file and start an HTTP server to serve source chunks to map workers.
  if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
    return fmt.Errorf("issue creating temp dir: %v", err)
  }
  // be sure to remove all of our temp files
  defer os.RemoveAll(tempdir)


  // start up the local server
  go localService(host, tempdir)


  _, err := splitDatabase(inputPath, tempdir, "map_%d_source.db", M)
  if err != nil {
    return fmt.Errorf("issue splitting: %v", err)
  }

  // Generate the full set of map tasks and reduce tasks.
  //Note that reduce tasks will be incomplete initially,
  //because they require a list of the hosts that handled each map task.
  mTasks := make([]MapTask, M)

  for i := 0; i < R; i++ {
    mTask[i] = MapTask {
      M:  M,
      R:  R,
      N:  i,
      SourceHost: host,
    }
  }

  rTasks := make([]ReduceTask, R)

  for i := 0; i < R; i++ {
    rTasks[i] = ReduceTask {
      M:  M,
      R:  R,
      N:  i,
      SourceHosts: make([]string, M),
    }
  }

  // Create and start an RPC server to handle incoming client requests.
  //Note that it can use the same HTTP server that shares static files.
  masterNode := Node {
    Phase:  Wait,
    NextJob:  0,
    DoneJobs: 0,
    MapTasks: mapTasks,
    ReduceTasks:  reduceTasks,
    Done: make(chan DoneJob, 10),
    // string struct of worker friends
    Workers:  make(map[string]struct{}),
  }
  // call the actor and start up the RPC server
  actor, err := masterNode.startRPC()
  if err != nil {
    return fmt.Errorf("Issue with RPC server: %v", err)
  }

  // Waiting phase

  if wait {
    log.Printf("Master waiting for user input to start.\n", host)
    fmt.Println("Push Enter to start the program...")

    // ignoring the first string
    var nothing string
    fmt.Scanln(&nothing)

    // master should by default start on map mode
    masterNode.Phase = Map

    fmt.Println("Starting up workers")

    for workerAddr := range masterNode.Workers {
      log.Printf("Starting worker: @[%s]", workerAddr)
      // err check the call
      if err := call(workerAddr, "NodeActor.Signal", struct{}{}, nil); err != nil {
        log.Printf("issue with worker @[%s]: %v\n", workerAddr, err)
      }

    }
  } else {
    // set to map phase
    masterNode.Phase = Map
    log.Printf("Master waiting for workers.\n", host)
  }

  // wait for jobs
  reduceHosts := actor.waitForJobs(masterNode.Finished)

  // create outputURLS

  outputURLs := make([]string, R)
  for i := 0; i < R; i++ {
    outputURLs[i] = makeURL(reduceHosts[i], reduceTasks[i].outputFile())
  }



}

// Document

func (a *NodeActor) waitForJobs(taskDone <-chan JobFinished) []stirng {
  // return a string slice

  mapHosts := make([]string, M)
  reduceHosts := make([]string R)

  // send tasks as they are completed

  for task := range taskDone {
    var current Phase
    // be sure to prevent data racing
    a.run(func(n *Node) {
      //check the phases
      switch {
        case n.Phase == Map || n.Phase == MapDone:

          log.Printf("Map task %d completed by [%s]\n", task.Number, task.Addr)
          // set each map host to the address of the task
          mapHosts[task.Number] = task.Addr
          n.DoneJobs++

          // if we have completed all the jobs (number M of jobs), we execute this
          if n.DoneJobs == M {
            for i := 0; i < R; i++ {
						  n.ReduceTasks[i].SourceHosts = mapHosts
					  }


  					log.Println("Map phase completed")

  					n.Phase = Reduce

  					n.NextJob = 0

  					n.DoneJobs = 0
          }

        case n.Phase == Reduce || n.Phase == ReduceDone:

          log.Printf("Reduce task %d completed by [%s]\n", task.Number, task.Addr)

          reduceHosts[task.Number] = task.Addr

          n.DoneJobs++

          // if we have completed all the jobs (number R of jobs), we execute this
          if n.DoneJobs == R {
            log.Println("Reducing phase complete")
            n.Phase = Merge
          }

        default:
          // ignore the tasks if the other cases don't fire
          log.Printf("Ignoring task completetion")
      }
      current = n.Phase
    })

    if current >= Merge {
      break
    }
  }
  return reduceHosts
}
