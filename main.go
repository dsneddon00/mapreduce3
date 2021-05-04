package main

import (
  "log"
  "strconv"
  "strings"
  "unicode"
  // importing that "library" that I made through github
  "github.com/dsneddon00/mapreduce3/mapreduce"
)


func main() {
  // imported class from the library
  var client Client
  if err := mapreduce.Start(client); err != nil {
    log.Fatalf("Error with client %v", err)
  }
}


type Client struct{}


func (c Client) Map(key, value string, output chan<- mapreduce.Pair) error {
  // ensuring we close out the output channel no matter what
  defer close(output)
  lst := strings.Fields(value)

  for _, elt := range lst {
    // passing strings of map into word
    word := strings.Map(func(r rune) rune {
      if unicode.IsLetter(r) || unicode.IsDigit(r) {
        return unicode.ToLower(r)
      }
      return -1
    }, elt)
    if len(word) > 0 {
      // send a map key and value pair into the output
      output <- mapreduce.Pair{Key: word, Value: "1"}
    }
  }
  return nil
}
