package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// !!! Define ByKey
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// !!! Read input files from map tasks
	var keyValues []KeyValue
	for m := 0; m < nMap; m += 1 {
		fName := reduceName(jobName, m, reduceTaskNumber)
		f, err := os.Open(fName)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		for dec.More() {
			var kv KeyValue
			if dec.Decode(&kv) != nil {
				log.Fatal(err)
			}
			keyValues = append(keyValues, kv)
		}
	}

	// !!! Create reduce result file
	f, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// !!! Sort key value pairs by key, and group together values of same key
	sort.Sort(ByKey(keyValues))
	type KeyGroups struct {
		Key    string
		Values []string
	}
	var keyGroups []KeyGroups
	for i, kv := range keyValues {
		if i == 0 || kv.Key != keyValues[i-1].Key {
			keyGroups = append(keyGroups, KeyGroups{kv.Key, []string{kv.Value}})
		} else {
			j := len(keyGroups) - 1
			keyGroups[j].Values = append(keyGroups[j].Values, kv.Value)
		}
	}

	// !!! Run customized reduce function per key
	enc := json.NewEncoder(f)
	for _, kv := range keyGroups {
		if enc.Encode(KeyValue{kv.Key, reduceF(kv.Key, kv.Values)}) != nil {
			log.Fatal(err)
		}
	}
}
