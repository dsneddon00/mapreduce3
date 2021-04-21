package mapreduce

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

func start(client Interface, inputPath, outputPath string) error {
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
    Workers:  make(map[string]struct{}),
  }
  // call the actor and start up the RPC server
  actor, err := masterNode.startRPC()
  if err != nil {
    return fmt.Errorf("Issue with RPC server: %v", err)
  }

}
