package main

import (
  "database/sql"
  "fmt"
  "hash/fnv"
  "log"
  "path/filepath"
)

// Even More Russ code (:

// the map task describes individual workers and their tasks
// we are going to plub specific test data chunks

type MapTask struct {
    M, R       int    // total number of map and reduce tasks
    N          int    // map task number, 0-based
    SourceHost string // address of host with map input file
}


func (task *MapTask) Process(tempdir string, client Interface) error {
  // First, we download the input file
  iFile := filepath.Join(tempdir, task.sprintInputFile())
  // check if download returned an error we don't like
  if err := download(makeURL(task.SourceHost, task.sprintSourceFile()), iFile); err != nil {
    return fmt.Errorf("download error: %v", err)
  }

  // output queries
  // stmt is a prepared statement in sequel and is safe for concurrent use in
  // multiple gorotines
  outputStatements := make([]*sql.Stmt, task.R)
  for i := 0; i < task.R; i++ {
    // creating the database and sending in the outputfile path to make the actual output file
    db, err := createDatabase(filepath.Join(tempdir, task.sprintOutputFile()))

    if err != nil {
      return fmt.Errorf("Issue Creating Output Files: %v", err)
    }
    /*
    Prepare creates a prepared statement for later queries or executions.
    Multiple queries or executions may be run concurrently from the returned statement.
    The caller must call the statement's Close method when the statement is no longer needed.
    */
    stmt, err := db.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
    // error check the sequel query
    if err != nil {
      return fmt.Errorf("issue on insert statements: %v", err)
    }

    outputStatements[i] = stmt

    // drop out defers to make sure we close the db and the statements
    defer stmt.Close()
    defer db.Close()
  }


  // open database
  db, err := openDatabase(iFile)

  if err != nil {
    return fmt.Errorf("Issue opening database: %v", err)
  }
  defer db.Close()

  rows, err := db.Query("SELECT key, value FROM pairs")
  if err != nil {
    return fmt.Errorf("issue querying input db: %v", err)
  }
  defer rows.Close()

  // counters
  i, j := 0, 0

  for rows.Next() {
    // increase the counter based on how many inputs we have processed\
    i++
    var key, value string
    if err := rows.Scan(&key, &value); err != nil {
      return fmt.Errorf("reading issue: %v", err)
    }

    // create map and gather the output
    outputMap := make(chan Pair, 200)
    finishedMap := make(chan error)

    // check for client map errors

    if err := client.Map(key, value, outputMap); err != nil {
      return fmt.Errorf("issue with client map: %v", err)
    }

    // Goroutine for writing intermediate key value pairs
    go task.writeOutput(outputMap, finishedMap, outputStatements, &j)

    //check if finished
    if err := <-finishedMap; err != nil {
      return fmt.Errorf("Issue writing output: %v", err)
    }
  }

  // check if rows contain errors
  if err := rows.Err(); err != nil {
    return fmt.Errorf("issue in rows: %v", err)
  }

  log.Printf("Map Task %d processed %d pairs and generated %d pairs\n", task.N, i, j)

  return nil
}

// we are going to write us a couple cool helper functions

func (task *MapTask) sprintSourceFile() string {
  return fmt.Sprintf("map_%d_source.db", task.N)
}

func (task *MapTask) sprintInputFile() string {
  return fmt.Sprintf("map_%d_source.db", task.N)
}

func (task *MapTask) sprintOutputFile() string {
  return fmt.Sprintf("map_%d_output_%d")
}

/* Be sure to use the Russ given code somewhere
hash := fnv.New32() // from the stdlib package hash/fnv
hash.Write([]byte(pair.Key))
r := int(hash.Sum32() % uint32(task.R))
*/

// this function will write the actual output to the Map Task
func (task *MapTask) writeOutput(output <-chan Pair, finishedMap chan<- error, outputStatements []*sql.Stmt, count *int) {
  for pair := range output {
    *count++
    // find the output file
    hash := fnv.New32() // from the stdlib package hash/fnv
    hash.Write([]byte(pair.Key))
    r := int(hash.Sum32() % uint32(task.R))

    if _, err := outputStatements[r].Exec(pair.Key, pair.Value); err != nil {
      finishedMap <- fmt.Errorf("issue inserting: %v", err)
      return
    }
  }
  finishedMap <- nil
}
