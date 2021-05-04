package main

// You are free to design and implement your RPC calls in a way that makes sense to you.
// The master and workers all share a single code base,
// so they can easily share data structures.

import (
  "log"
  "net/rpc"
)


type Node Struct {
  Phase Phase
  NextJob int
  FinishedJobs int
  MapTasks []MapTask
  ReduceTasks []ReduceTask
  Finished chan JobFinished
  Workers map[string]struct{}
}


type Job struct {
  Phase Phase
  Wait bool
  // important that we link it by reference to MapTask and ReduceTask
  MapTask *MapTask
  ReduceTask *ReduceTask
}

type JobFinished struct {
  Num int
  Address string
}

type Phase int


// A friend showed me that we could create some ENUMs to help us out with the RPC server

const (
  Wait Phase = iota
  Map
  MapDone
  Reduce
  ReduceDone
  Merge
  Finish
)

// Node method, returns next job on the list or sets the wait to true if there isn't one
func(n *Node) GetNextJob(workerAddress string) Job {
  job := Job { Phase: n.Phase, Wait: true}

  // switch case in this case asks if the current Phase is Map or Reduce, if it isn't, we wait
  switch n.Phase {
  case Map:
    // perform Map Job
    if n.NextJob < M {
      // assign next job to be a map task
      log.Printf("The map task %d has now been asigned to [$s]\n", n.NextJob, workerAddress)
      // actually assigning the right stuff
      job.MapTask = &n.MapTasks[n.NextJob]
      job.Wait = false
      // next iteration
      n.NextJob++

      if n.NextJob >= M {
        n.Phase = MapDone
      }
    }

  case Reduce:
    // perform Map Job
    if n.NextJob < R {
      // assign next job to be a map task
      log.Printf("The reduce task %d has now been asigned to [$s]\n", n.NextJob, workerAddress)
      // actually assigning the right stuff
      job.ReduceTask = &n.ReduceTasks[n.NextJob]
      job.Wait = false
      // next iteration
      n.NextJob++

      if n.NextJob >= M {
        n.Phase = ReduceDone
      }
    }
  }
  return job
}
// using actor method to start up the RPC server
func (n *Node) startRPC() (NodeActor, error) {
  actor := n.startActor()
	rpc.Register(actor)
	rpc.HandleHTTP()
	return actor, nil
}

func (n *Node) startActor() NodeActor {
  channel := make(chan handler)

  go func() {
    for evt := range channel {
      evt(n)
    }
  }()

  return ch
}
// nodeActor method
func (x NodeActor) run(h handler) {
  finished := make(chan struct{})
  x <- func(n *Node) {
    h(n)
    finished <- struct{}{}
  }
  <- finished
}

func (x NodeActor) Signal(_ struct{}, _ *struct{}) error {
  x.run(func(n *Node) {
    x.Finished <- JobFinished{}
  })
  return nil
}

func (x NodeActor) RequestJob(workerAddress struct{}, job *Job) error {
  var err error
  x.run(func(n *Node) {
    *job = n.GetNextJob(workerAddress)
  })
  return err
}

func (x NodeActor) FinishJob(job JobFinished, _ *struct{}) error {
  x.run(func(n *Node) {
    x.Finish <- job
  })
  return nil
}

// RPC calls and pings

func call(address string, method string, request interface{}, reply interface{}) error {

	client, err := rpc.DialHTTP("tcp", string(address))
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.Call(method, request, reply); err != nil {
		return err
	}

	return nil
}

func (a NodeActor) Ping(addr string, wait *bool) error {
	a.run(func(n *Node) {
		log.Printf("worker connected at %s\n", addr)
		n.Workers[addr] = struct{}{}
		if n.Phase == Wait {
			*wait = true
		} else {
			*wait = false
		}
	})
	return nil
}
