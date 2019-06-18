package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

// URLCountMap is the map function in the first round
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	result := make(map[string]int, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		result[l] += 1
	}
	kvs := make([]KeyValue, 0, len(result))
	for k, v := range result {
		kvs = append(kvs, KeyValue{Key: k, Value: strconv.Itoa(v)})
	}
	return kvs
}

// URLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	sum := 0
	for _, v := range values {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		sum += n
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(sum))
}

// URLTop10Map is the map function in the first round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	cnts := make(map[string]int, len(lines))
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		tmp := strings.Split(l, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}
	us, cs := FastTopN(cnts, 10)

	kvs := make([]KeyValue, 0, 10)
	for i := range us {
		kvs = append(kvs, KeyValue{"", fmt.Sprintf("%s %d", us[i], cs[i])})
	}
	return kvs
}

// URLTop10Reduce is the reduce function in the second reound
func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := FastTopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
