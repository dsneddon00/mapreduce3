package main

import (
  "database/sql"
  "fmt"
  "log"
  "path/filepath"
)

// Russ given goodies
type ReduceTask struct {
    M, R        int      // total number of map and reduce tasks
    N           int      // reduce task number, 0-based
    SourceHosts []string // addresses of map workers
}

type KeySet struct {
  Key string
  Input <-chan string
}


func (task *ReduceTask) Process(tempdir string, client Interface) error {
  // This method processes a single reduce task. It must:


  // stores input files into a url slice of strings
  urls := make([]string, task.M)
  for i := 0; i < task.M; i++ {
    urls[i] = makeURL(task.SourceHosts[i], task.sprintMapInputFile(i))
  }


  // Create the input database by merging all of the appropriate output databases from the map phase
  inputDB, err := mergeDatabases(urls, filepath.Join(tempdir, task.sprintInputFile()), "tmp.db")
  // check the merge error
  if err != nil {
    return fmt.Errorf("issue merging database: %v", err)
  }

  defer inputDB.Close()

  // Create the output database
  outputDB, err := createDatabase(filepath.Join(tempdir, task.sprintOutputFile()))
  if err != nil {
    return fmt.Errorf("issue creating output database: %v", err)
  }
  defer outputDB.Close()

  // create the output statements
  outputStatements, err := outputDB.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
  if err != nil {
    return fmt.Errorf("issue with the prepare insert: %v", err)
  }
  defer outputStatements.Close()
  // Process all pairs in the correct order. This is trickier than in the map phase. Use this query:
  i, v, j := 0,0,0

  //It will sort all of the data and return it in the proper order.
  rows, err := inputDB.Query("SELECT key, value FROM pairs ORDER BY key, value")
  if err != nil {
    return fmt.Errorf("issue querying input db: %v", err)
  }
  defer rows.Close()

  KeySets := make(chan KeySet)
  readDone, writeDone := make(chan error), make(chan error)
  //As you loop over the key/value pairs,
  //take note whether the key for a new row is the same or
  //different from the key of the previous row.
  go readInput(rows, KeySets, readDone, &v)

  for set := range KeySets {
    i++
    reduceOutput := make(chan Pair, 100)

    go task.writeOutput(reduceOutput, writeDone, outputStatements, &j)
    if err := client.Reduce(set.Key, set.Input, reduceOutput); err != nil {
      return fmt.Errorf("issue with client reduce: %v", err)
    }
  }
  log.Printf("Map Task %d processed %d pairs and generated %d pairs\n", task.N, i, v, j)

  return nil
}










// Some more cool helpers, much like my other ones, however, we use ReduceTasks instead
// very similar to the map version, take away the hash part tho
func (task *ReduceTask) writeOutput(output <-chan Pair, finishedReduce chan<- error, stmt *sql.Stmt, count *int) {
  for pair := range output {
    *count++
    if _, err := stmt.Exec(pair.Key, pair.Value); err != nil {
      finishedReduce <- fmt.Errorf("issue inserting: %v", err)
      return
    }
  }
  finishedReduce <- nil
}

// send reading input database to the reduce function on the client

// a lot of this function is part of what we discussed with the lecture today
func readInput(rows *sql.Rows, setChannel chan<- KeySet, finishedReduce chan error, valueCount *int) {
  var erwar error
  var previousKey string
  var current chan string

  defer func() {
    close(setChannel)
    finishedReduce <- erwar
  }()

  for rows.Next() {
    // check for same and different keys through looping through them
    var key, value string
    if err := rows.Scan(&key, &value); err != nil {
      if previousKey != "" {
        close(current)
      }
      erwar = fmt.Errorf("issue reading a row from the input database: %v", err)
      return
    }

    // check if keys have changed
    if previousKey != key {
      if previousKey != "" {
        // Close call if it isn't the first key
        close(current)
        // wait for the output to finish
        if err := <-finishedReduce; err != nil {
          return
        }
      }
      // initiate a new key set
      current = make(chan string, 100)
      setChannel <- KeySet {
        Key: key,
        Input: current,
      }
    }
    *valueCount++
    current <- value
    previousKey = key
  }
  // check for errors in rows, just like how we did before
  if err := rows.Err(); err != nil {
		if previousKey != "" {
			close(current)
		}
		erwar = fmt.Errorf("issue on rows of downloaded db: %v", err)
	}

	// close last call
	close(current)

	// Wait for output to finish
	if err := <-finishedReduce; err != nil {
		return
	}
}


// reminder that Sprintf is a formating function, as opposed to simple printing
func(task *ReduceTask) sprintMapInputFile(taskNum int) string {
  return fmt.Sprintf("map_%d_output_%d.db", taskNum, task.N)
}

func(task *ReduceTask) sprintInputFile() string {
  return fmt.Sprintf("map_%d_source.db", task.N)
}

func(task *ReduceTask) sprintOutputFile() string {
  return fmt.Sprintf("map_%d_output_%d")
}

func(task *ReduceTask) sprintTempFile() string {
  return fmt.Sprintf("reduce_%d_temp.db", task.N)
}

func(task *ReduceTask) sprintPartialFile() string {
  return fmt.Sprintf("reduce_%d_partial.db", task.N)
}
