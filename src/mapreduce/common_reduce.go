package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
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
	// Your code here (Part I).
	//
	var data Elem
	for m := 0; m < nMap; m++ {

		filename := reduceName(jobName, m, reduceTask)

		fmt.Println("[reduce file] 50", filename)

		f, err := os.Open(filename)
		if err != nil {
			log.Fatal("[reduce] open file err: ", err)
		}

		enc := json.NewDecoder(f)
		var kv KeyValue
		for {

			err := enc.Decode(&kv)
			if err != nil {
				break
			}
			data = append(data, kv)
		}
		f.Close()

	}

	//println("===========Sort==============")
	//arrary sort
	sort.Sort(data)

	kvm := make(map[string][]string)
	for _, kv := range data {
		k, v := kv.Key, kv.Value
		if vals, ok := kvm[k]; !ok {
			kvm[k] = append(vals, v)
		} else {
			var vals []string
			kvm[k] = append(vals, v)
		}
	}

	//println("===========write data of reduce==============")
	//fmt.Println("[reduce] 60",kvm)

	f2, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		log.Fatal("[reduce] create file err: ", err, outFile)
	}
	enc2 := json.NewEncoder(f2)

	for k, vs := range kvm {
		rs := KeyValue{k,reduceF(k, vs)}
		err := enc2.Encode(&rs)
		if err != nil {
			log.Fatal("[doMap] ncoder err: ", err)
		}
	}

	//reduceF()

}

type Elem []KeyValue

func (e Elem) Len() int           { return len(e) }
func (e Elem) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e Elem) Less(i, j int) bool { return e[i].Key < e[j].Key }
