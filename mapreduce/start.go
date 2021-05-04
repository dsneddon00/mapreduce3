package main

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)


const host = "localhost:8080"

func main() {
  // we need to initiate a GOMAXPROCS because it limits the ammoung of operating system threads
  // that the user can execute simultaniously.
  runtime.GOMAXPROCS(1)
  log.SetFlags(log.Lshortfile)

  partTwo()
}

// part one and two of the assignment, calling the one we need based on whichever one we are demonstrating

func partOne() {
  tempdir := "temp"
  // calling our helper localService
  go localService(host, tempdir)
  // split the database into a couple different paths
  // spliting it into 5 chunks
  paths, err := splitDatabase("austen.db", tempdir, "output-%d.db", 5)

  // Then have your main function merge the pieces back into a single file.
  // func splitDatabase(source, outputDir, outputPattern string, m int) ([]string, error)
  // mergeDatabases(urls []string, path string, temp string) (*sql.DB, error)
  if err != nil {
    log.Fatal("Error spliting the db: %v", err)
  }

  outputdb, err := mergeDatabases(paths, "final.db", "temp.db")
  if err != nil {
    log.Fatalf("merging: %v", err)
  }

  // verification
  var total int
  	if err := outputdb.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
  		log.Fatalf("counting issue: %v", err)
  	}

  fmt.Printf("Databases complete!\n")
}

// part two

func partTwo() {
  tempdir := filepath.Join("tmp", fmt.Sprintf("mapreduce.%d", os.Getpid()))
  if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
    log.Fatalf("issue creating the temp dir: %v", err)
  }

  // let's call that local server

  go localService(host, tempdir)

  M, R := 9, 3
  // we are running 9 map tasks and 3 reduce tasks

  // splitting open the database
  _, err := splitDatabase("data/austin.db", tempdir, "map_%d_source.db", M)
  if err != nil {
    log.Fatalf("issue splitting: %v", err)
  }

  // creating a slice of hosts per Map request
  hosts:= make([]string, M)
  for i := range host {
    hosts[i] = host
  }

  // Mapping portion

  log.Println("Mapping operation is a go")
  for i := 0; i < M; i++ {
    curTask := MapTask{M: M, R: R, N: i, SourceHost: host,}
    // check on error
    if err := curTask.Process(tempdir, Client{}); err != nil {
      log.Fatalf("issue with map task: %v", err)
    }
  }

  // Reduce Portion
  log.Println("Reducing operation is a go")
  urls := make([]string, R)
  for i := 0; i < M; i++ {
    curTask := MapTask{M: M, R: R, N: i, SourceHost: host}
    // check on error
    if err := curTask.Process(tempdir, Client{}); err != nil {
      log.Fatalf("issue with reduce task: %v", err)
    }
    // append to urls string slice the output
    urls[i] = makeURL(host, curTask.sprintOutputFile())
  }

  // merge those DATABASES
  db, err := mergeDatabases(urls, "merged.db", "temp.db")
  // let's error check that niblet!!
  if err != nil {
    log.Fatal("Issue merging databases: %v", err)
  }
  defer db.Close()

  var total int
  	if err := db.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
  		log.Fatalf("counting issue: %v", err)
  	}

  fmt.Printf("Databases complete!\n")

}

// helpers, a good chunk of this was taken from part 1 but I modified it to fit my needs.

// we are going to call localService as a go routine, keep that in mind
func localService(host, tempdir string) {
  http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	log.Printf("Serving %s/* at %s", tempdir, makeURL(host, "*"))
	if err := http.ListenAndServe(host, nil); err != nil {
		log.Fatalf("Error in HTTP server for %s: %v", host, err)
	}

}

// helper function to split everything into a URL
func makeURL(host, file string) string {
	return fmt.Sprintf("http://%s/data/%s", host, file)
}
