package main

// Part 2 instructions

//In this part you will write the code to handle map
//and reduce tasks, but everything will still run
//on a single machine. Write your code in a file
//called worker.go.


// Russ given code (:

//A Pair groups a single key/value pair. Individual client jobs
//(wordcount, grep, etc.) will use this type when feeding results back
//to your library code.
type Pair struct {
  Key   string
  Value string
}

//The Interface type represents the interface that a client job must implement
//to interface with the library. In Go, it is customary to consider the full
//name of a value when selecting its name. In this case, the client code will
//refer to this type as mapreduce.Interface, so its apparently vague
//name is appropriate.

type Interface interface {
  Map(key, value string, output chan<- Pair) error
  Reduce(key string, values <-chan string, output chan<- Pair) error
}
