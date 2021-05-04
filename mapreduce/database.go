package main

// this file takes all the database methods from part 1
// and puts it in here. I decided for part 2
// to switch to a multi-file structure instead
// of using one giant single file.
// toodles (:

import (
	"database/sql"
	"fmt"
  "errors"
  //"strings"
  //"bufio"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
  //"io/fs"
  //"io/ioutil"

	_ "github.com/mattn/go-sqlite3"
)
// moved to worker.go
/*
type Pair struct {
  Key string
  Value string
}
*/

//var tempdir string
//var host string

// I don't think we have to do anything else on this particular function, I think we are good.
func openDatabase(path string) (*sql.DB, error) {
  //path = "austen.db"
	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, _ := sql.Open("sqlite3", path+options)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Issue opening database %s: %v", path, err)
	}
	return db, nil
}

func createDatabase(path string) (*sql.DB, error) {
  // creates a new database and prepares it for use

  // if the database file already exists, delete it before continuing.
  /*
  if path exists {
    path.drop()
  }
  */
  // create table pairs (key text, value text)

  // it's BIG BRAIN TIME, let's just remove the database period whenever we try
  // to create one, then we don't even need to check (:
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("Issue removing existing database: %v", err)
		}
	}

	db, err := openDatabase(path)
	if err != nil {
		return nil, fmt.Errorf("Issue opening database: %v", err)
	}

  // create a new table based on the pairs
	_, err = db.Exec("CREATE TABLE pairs (key text, value text)")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("Issue creating the table: %v", err)
	}

	return db, nil
}

/*When the master first starts up,
it will need to partition the input data set for the individual
mapper tasks to access. This function is responsible for splitting a
single sqlite database into a fixed number of smaller databases.*/

// had to redo the split database function due to error I couldn't figure out
func splitDatabase(source, outputDir, outputPattern string, m int) ([]string, error) {
	// Open source database
  /*It is given the pathname of the input database,
and a pattern for creating pathnames for the output files.
The pattern is a format string for fmt.SFatalf to use,
so you might call it using:*/
  // we can start off by using our open database function to start
  /*
  var names []string
  db, err := openDatabase(source)
  if err != nil {
    log.Fatalf("split database:", err)
  }*/
  // defer will execute after we return, that's kinda what defer does,
  //is it delays the operation in defer from running until the function is finished
  //defer db.Close()
  /*
  count, err := rowCount(db)
  if err != nil {
    // for some reason we are hitting this a ton
    log.Fatalf("spliting database query into pairs", err)
  }
  */
  // debug line
  /*
  fmt.Println("Count: ", count)
  if count < m {
    log.Fatal("Can't have the count less than m")
  }
  */
	db, err := openDatabase(source)

	if err != nil {
		return nil, fmt.Errorf("Issue opening source database: %v", err)
	}
	defer db.Close()

	outputPaths := make([]string, m)

  // counting the pairs and stuff
	var total int
	if err := db.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
		return nil, fmt.Errorf("unable to get total size of data from source db: %v", err)
	}
	log.Printf("Size of data: %d", total)

  //log.Printf("we got here!")
// Fewer keys than map tasks
	if total < m {
		return nil, errors.New("Issue with fewer keys than acutal map tasks")
	}

  // have fun with our calculations for the ammount of partitions
	base := total / m
	r := total % m

	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return nil, fmt.Errorf("Issue with querying source database: %v", err)
	}
	defer rows.Close()

	count := 0

	for i := 0; i < m; i++ {
		size := base
		if i < r {
			size++
		}

		// Create out DB
		name := fmt.Sprintf(outputPattern, i)
		dir := filepath.Join(outputDir, name)
		db, err := createDatabase(dir)
		if err != nil {
			return nil, fmt.Errorf("Issue creating output database: %v", err)
		}
    // set each of our outputPaths to a directory by default
		outputPaths[i] = dir

		stmt, err := db.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
		if err != nil {
			return nil, fmt.Errorf("Issue with insert statement: %v", err)
		}

		for r := 0; r < size; r++ {
			rows.Next()
			count++
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				return nil, fmt.Errorf("Issue Reading a row from source database: %v", err)
			}
      // try a statement execute right here
			if _, err := stmt.Exec(key, value); err != nil {
				return nil, fmt.Errorf("Issue inserting into output database: %v", err)
			}
		}

    // be sure to close both our statements and our db's
		stmt.Close()
		db.Close()
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating over source db: %v", err)
	}

	// Verify all rows were processed
	if count != total {
		err := errors.New("wrong number of keys processed")
		return nil, fmt.Errorf("%v processed: %d, total: %d", err, count, total)
	}

	return outputPaths, nil
  /*
  var read = db.QueryRow("SELECT count(key) from pairs")
  var count int

  // pass in the reference of count, gets saved by the scan
  _ = read.Scan(&count)
  if count < m {
    return names, err
  }

  var length = count / m
  var rem = count - ((count / m) * m)

  if err != nil {
    return names, err
  }

  var databaseSlice []*sql.DB

  if err != nil {
    log.Fatalf("issue opening up the database for splitting: %v", err)
  }

  rows, err := db.Query("SELECT key, value FROM pairs")
  for i := 1; i <= m; i++ {
    // creates the path right here
    var file = filepath.Join(outputDir, fmt.Sprintf(outputPattern, i));

    // inform the user of the file creation
    fmt.Printf("path: %s\n", file)

    names = append(names, file)
    outputDB, err := createDatabase(file)

    if err != nil {
      log.Fatalf("Issue splitting and creating database: %v", err)
    }

    databaseSlice = append(databaseSlice, outputDB);

    var x = 0

    for x := 0; x < length; x++ {
      rows.Next()
      // create a key value pair
      var key, value string
      // like we did before passing by reference
      _ = rows.Scan(&key, &value)
      outputDB.Exec("INSERT INTO pairs(key, value) values(?, ?)", key, value)
    }

    if i == 50 { // distribute data remaining
      for x = 0; x < rem; x++ {
        rows.Next()
        var key, value string

        _ = rows.Scan(&key, &value)
        databaseSlice[x].Exec("INSERT INTO pairs(key, value) values(?, ?)", key, value)
      }
    }
  }*/

  // create a slice of databases
  // a := make([]int, 5)  // len(a)=5
  // set its length equal to the sql.DB's length
  //databaseSlice := make([]*sql.DB, 0)
  // name slice, nothing in by default
  //names := make([]string, 0)
  /*
  // spliting the database into a bunch of different files, as for the mapping stuff to happen
  for i := 0; i < m; i++ {
    // SFatalf(format string, a ...interface{}) string
    filename := filepath.Join(outputDir, fmt.Sprintf(outputPattern, i))

    // creating a new database to put into a directory
    outputdb, err := createDatabase(filename)

    if err != nil {
      log.Fatalf("Connection Error: %v", err)
    }

    defer outputdb.Close()

    // append database and names to their slices
    databaseSlice = append(databaseSlice, outputdb)
    names = append(names, filename)
  }

  // send in sql query to grab keys and pairs
  rows, err := db.Query("select key, value from pairs;")
  if err != nil {
    log.Fatalf("split databases selection:", err)
  }

  defer rows.Close()

  index := 0 // seting an index

  for rows.Next() {
    // get pair from rows
    pair, err := getPair(rows)
    if err != nil {
      log.Fatalf("split database%v", err)
    }
    // inserting the pairs into the seperate databases
    err = insertPair(pair, databaseSlice[index % m])
    if err != nil {
      log.Fatalf("split database%v", err)
    }
    index++
  }
  err = rows.Err()
  if err != nil {
    log.Fatal("end of split counting%v", err)
  }
  */
}

// Merge databases located trough urls into a destination local db, using temp as the temporary write file
func mergeDatabases(urls []string, dest string, temp string) (*sql.DB, error) {
  // first we need to actually create the database
	db, err := createDatabase(dest)
	if err != nil {
		return nil, fmt.Errorf("Issue with creating database: %v", err)
	}

  // For each URL in the urls list:
	for _, url := range urls {
    // Download the file at the given URL and store it in the local
    //file system at the path given by temp.
		if err := download(url, temp); err != nil {
			db.Close()
			return nil, fmt.Errorf("Issue with downloading db %s: %v", url, err)
		}
		// Merge and delete temp
		if err := gatherInto(db, temp); err != nil {
			db.Close()
			return nil, fmt.Errorf("Issue with merging db @(%s): %v", url, err)
		}
    // Delete the temporary file.
	}

	return db, nil
}

// Download a file over HTTP and store in dest path
func download(url, path string) error {
  //This function takes a URL and a pathname, and stores the file it downloads from the URL at the given location
  /*pathway, err := os.Create(path)
  if err != nil {
    log.Fatalf("Download create %s", err)
  }
  // always delay, but be sure we execute the close (:
  defer pathway.Close()

  // actually grabbing the body of the URL (:
  b, err := http.Get(url)
  if err != nil {
    log.Fatalf("Download get %s", err)
  }
  defer b.Body.Close()
  // always return an error code

  _, err = io.Copy(pathway, b.Body)
  if err != nil {
    log.Fatalf("Download opendb", err)
  }
  // utilize net/http

  // nil is generally considered "no error happened"
  return nil*/
	mpath, err := os.Create(path)
	if err != nil {
		log.Fatalf("%s: %s\n Download create", err)
	}
	defer mpath.Close()

	// get body from url
	res, err := http.Get(url)
	if err != nil {
		log.Fatalf("%s: %s\n Download get", err)
	}
	defer res.Body.Close()

	// read body into mpath
	_, err = io.Copy(mpath, res.Body)
	if err != nil {
		log.Fatalf("%s: %s\n Download copy", err)
	}

	return nil
}

// Merges db at path <in> into <out> db
func gatherInto(out *sql.DB, in string) error {
	if _, err := out.Exec(`ATTACH ? AS merge; INSERT INTO pairs SELECT * FROM merge.pairs; DETACH merge;`, in); err != nil {
		return fmt.Errorf("Issue merging db: %v", err)
	}

	// Delete input file
	if err := os.Remove(in); err != nil {
		return fmt.Errorf("Issue removing input file: %v", err)
	}

	return nil
}

/* main from part 1, gonna leave this here in case
I need it again

func main() {

  // I might need to ask a bunch of questions on this part, like where exactly do I start?

  //MapReduce will be a library, but for now it will be easier to test it if
  //you put it in package main and write a main function. Use the
  //Jane Austen example file and run your splitDatabase function
  //to break it into several pieces.
  //Count the number of rows in each piece from the command line using:
  //sqlite3 austen-0.db 'select count(1) from pairs'
  //result is 75811

  //Add the numbers up for each of the pieces and make sure the total
  //matches the total number of rows in the input file.
  outputdb, err := openDatabase("austen.db")
  // ask why the pairs are not being recognized
  // there is a second function called query row that return a single result
  /*rows, err := outputdb.Query("SELECT COUNT(1) FROM pairs")
  if err != nil {
    log.Fatalf("counting database error", err)
  }

  defer outputdb.Close()

  // Russ talked about with me during the office hours
  //that we can just use a single row as opposed to multiple
  var count int
  rows, err := outputdb.Query("SELECT COUNT(1) FROM pairs")

  for rows.Next() {
    if err := rows.Scan(&count); err != nil {
      log.Fatalf("Looping through database", err)
    }
  }
  fmt.Printf("After Split Count: %d\n", count)

  // %d is a decimal integer

  // Then have your main function merge the pieces back into a single file.
  // func splitDatabase(source, outputDir, outputPattern string, m int) ([]string, error)
  // mergeDatabases(urls []string, path string, temp string) (*sql.DB, error)

  _, err = splitDatabase("austen.db", "output", "output-%d.db", 4)
  if err != nil {
    log.Fatal("Error spliting the db: %v", err)
  }

  go func() {
       http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("/tmp/output/data"))))
       if err := http.ListenAndServe("localhost:8080", nil); err != nil {
           log.Printf("Error in HTTP server for %s: %v", "localhost:8080", err)
       }
   }()

  // reformat each of these into a url then pass them into mergeDatabases
  // data is a list of strings
  outputSlice := make([]string, 0)

  // walk through the filepath right here, adding each file and its path to the output slice
  erwar := filepath.Walk("output", func(path string, f os.FileInfo, err error) error {
    outputSlice = append(outputSlice, path)
    return err
  })

  if erwar != nil {
    // goes and checks for unexpected errors if we have to.
    panic(erwar)
  }

  var urls []string

  // appending the urls
  for i, item := range outputSlice {
    url := strings.Split(item, "/")
    // we drop the continue past the directory if we encounter a bad one
    if i == 0 {continue}
    // the actual append operation
    urls = append(urls, "http://localhost:8080/data/" + url[len(url)-1])
  }

  outputdb, err = mergeDatabases(urls, "final.db", "output-final")
  if err != nil {
    log.Fatalf("merging: %v", err)
  }
  fmt.Printf("Databases complete!\n")




  //mergeDatabases("localhost:8080/data/output-1")
  // Make sure it has the correct number of rows as well.

  // Since mergeDatabases expects to download data from the network,
  //you need to run a simple web server.
  // The following code will serve all files in the given directory:





  // In this code tempdir is the directory containing the files to be served,
  //and /data/ is the prefix path that each file should have in its URL.
  //address should be the network address to listen on.
  //You can use localhost:8080 to run it on a single machine.
}

*/
