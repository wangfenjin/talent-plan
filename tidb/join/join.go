package main

import (
	"encoding/csv"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/pingcap/tidb/util/mvmap"
)

// Join accepts a join query of two relations, and returns the sum of
// relation0.col0 in the final result.
// Input arguments:
//   f0: file name of the given relation0
//   f1: file name of the given relation1
//   offset0: offsets of which columns the given relation0 should be joined
//   offset1: offsets of which columns the given relation1 should be joined
// Output arguments:
//   sum: sum of relation0.col0 in the final result
func Join(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	// return SortMergeJoinUint64(f0, f1, offset0, offset1)
	// return SortMergeJoinString(f0, f1, offset0, offset1)
	// return FastHashJoin(f0, f1, offset0, offset1)
	return BroadcastHashJoin(f0, f1, offset0, offset1)
}

// https://blog.csdn.net/lp284558195/article/details/80717219

func FastHashJoin(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	tbl0, tbl1 := readCSVFileIntoTbl(f0), readCSVFileIntoTbl(f1)
	if len(tbl0) < len(tbl1) {
		hashtable := buildHashTable(tbl0, offset0)
		for _, row := range tbl1 {
			rowIDs := probe(hashtable, row, offset1)
			for _, id := range rowIDs {
				v, err := strconv.ParseUint(tbl0[id][0], 10, 64)
				if err != nil {
					panic("JoinExample panic\n" + err.Error())
				}
				sum += v
			}
		}
	} else {
		hashtable := buildHashTable(tbl1, offset1)
		for _, row := range tbl0 {
			rowIDs := probe(hashtable, row, offset0)
			v, err := strconv.ParseUint(row[0], 10, 64)
			if err != nil {
				panic("JoinExample panic\n" + err.Error())
			}
			sum += v * uint64(len(rowIDs))
		}
	}
	return sum
}

func SortMergeJoinUint64(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	tbl0, tbl1 := readCSVFileIntoTbl2(f0), readCSVFileIntoTbl2(f1)
	sort.Slice(tbl0, func(i, j int) bool {
		for _, off := range offset0 {
			if tbl0[i][off] == tbl0[j][off] {
				continue
			}
			return tbl0[i][off] < tbl0[j][off]
		}
		return true
	})
	sort.Slice(tbl1, func(i, j int) bool {
		for _, off := range offset1 {
			if tbl1[i][off] == tbl1[j][off] {
				continue
			}
			return tbl1[i][off] < tbl1[j][off]
		}
		return true
	})
	i, j := 1, 1
	for i <= len(tbl0) && j <= len(tbl1) {
		tmp := compare2(tbl0[i-1], tbl1[j-1], offset0, offset1)
		if tmp == 0 {
			innerSum := tbl0[i-1][0]
			nexti, nextj := i+1, j+1
			for nexti <= len(tbl0) {
				if compare2(tbl0[nexti-1], tbl1[j-1], offset0, offset1) == 0 {
					innerSum += tbl0[nexti-1][0]
					nexti++
				} else {
					break
				}
			}
			for nextj <= len(tbl1) {
				if compare2(tbl0[i-1], tbl1[nextj-1], offset0, offset1) == 0 {
					nextj++
				} else {
					break
				}
			}
			innerSum *= uint64(nextj - j)
			sum += innerSum
			i = nexti
			j = nextj
		} else if tmp == -1 {
			i++
		} else {
			j++
		}
	}
	return sum
}

func SortMergeJoinString(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	tbl0, tbl1 := readCSVFileIntoTbl(f0), readCSVFileIntoTbl(f1)
	sort.Slice(tbl0, func(i, j int) bool {
		for _, off := range offset0 {
			if tbl0[i][off] == tbl0[j][off] {
				continue
			}
			return tbl0[i][off] < tbl0[j][off]
		}
		return true
	})
	sort.Slice(tbl1, func(i, j int) bool {
		for _, off := range offset1 {
			if tbl1[i][off] == tbl1[j][off] {
				continue
			}
			return tbl1[i][off] < tbl1[j][off]
		}
		return true
	})
	i, j := 1, 1
	for i <= len(tbl0) && j <= len(tbl1) {
		tmp := compare(tbl0[i-1], tbl1[j-1], offset0, offset1)
		if tmp == 0 {
			innerSum := toUint64(tbl0[i-1][0])
			nexti, nextj := i+1, j+1
			for nexti <= len(tbl0) {
				if compare(tbl0[nexti-1], tbl1[j-1], offset0, offset1) == 0 {
					innerSum += toUint64(tbl0[nexti-1][0])
					nexti++
				} else {
					break
				}
			}
			for nextj <= len(tbl1) {
				if compare(tbl0[i-1], tbl1[nextj-1], offset0, offset1) == 0 {
					nextj++
				} else {
					break
				}
			}
			innerSum *= uint64(nextj - j)
			sum += innerSum
			i = nexti
			j = nextj
		} else if tmp == -1 {
			i++
		} else {
			j++
		}
	}
	return sum
}

func toUint64(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic("JoinExample panic\n" + err.Error())
	}
	return v
}

// equal: 0, smaller: -1, larger: 1
func compare(row0, row1 []string, offset0, offset1 []int) int {
	for i := range offset0 {
		if row0[offset0[i]] < row1[offset1[i]] {
			return -1
		} else if row0[offset0[i]] > row1[offset1[i]] {
			return 1
		}
	}
	return 0
}

// equal: 0, smaller: -1, larger: 1
func compare2(row0, row1 []uint64, offset0, offset1 []int) int {
	for i := range offset0 {
		if row0[offset0[i]] < row1[offset1[i]] {
			return -1
		} else if row0[offset0[i]] > row1[offset1[i]] {
			return 1
		}
	}
	return 0
}

func readCSVFileIntoTbl2(f string) (tbl [][]uint64) {
	csvFile, err := os.Open(f)
	if err != nil {
		panic("ReadFileIntoTbl " + f + " fail\n" + err.Error())
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic("ReadFileIntoTbl " + f + " fail\n" + err.Error())
		}
		r := make([]uint64, 0, len(row))
		for _, t := range row {
			v, err := strconv.ParseUint(t, 10, 64)
			if err != nil {
				panic("JoinExample panic\n" + err.Error())
			}
			r = append(r, v)
		}
		tbl = append(tbl, r)
	}
	return tbl
}

func BroadcastHashJoin(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	tbl0, tbl1 := readCSVFileIntoTbl(f0), readCSVFileIntoTbl(f1)
	hashtable := buildHashTable(tbl0, offset0)

	tableSize := 100 // bench to select the proper size
	partNums := len(tbl1)/tableSize + 1
	i := 0
	s := make(chan uint64, partNums)
	var wg sync.WaitGroup
	for i+tableSize < len(tbl1) {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			s <- joinTwoTable(hashtable, tbl0, tbl1[j:j+tableSize], offset0, offset1)
		}(i)
		i += tableSize
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s <- joinTwoTable(hashtable, tbl0, tbl1[i:], offset0, offset1)
	}()

	wg.Wait()
	close(s)
	for r := range s {
		sum += r
	}
	return sum
}

func joinTwoTable(hashtable *mvmap.MVMap, tbl0, tbl1 [][]string, offset0, offset1 []int) uint64 {
	sum := uint64(0)
	for _, row := range tbl1 {
		rowIDs := probe(hashtable, row, offset1)
		for _, id := range rowIDs {
			v, err := strconv.ParseUint(tbl0[id][0], 10, 64)
			if err != nil {
				panic("JoinExample panic\n" + err.Error())
			}
			sum += v
		}
	}
	return sum
}
