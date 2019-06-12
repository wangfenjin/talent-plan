package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func main() {
	benchmarkMergeSort2()
}

func benchmarkMergeSort2() {
	numElements := 16 << 20
	src := make([]int64, numElements)
	original := make([]int64, numElements)
	benchPrepare(original)

	for j := 256; j < 1000000000; j *= 2 {
		benchmarkFunc := func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				copy(src, original)
				b.StartTimer()
				benchParallelMergeSort(src, j)
			}
			b.StopTimer()
		}
		results := testing.Benchmark(benchmarkFunc)
		fmt.Println(results.String(), results.MemString(), "Parallel", j)
		benchmarkFunc = func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				copy(src, original)
				b.StartTimer()
				benchForkJoinMergeSort(src, j)
			}
			b.StopTimer()
		}
		results = testing.Benchmark(benchmarkFunc)
		fmt.Println(results.String(), results.MemString(), "ForkJoin", j)
		// os.Stdout.Sync()
	}
}

func benchPrepare(src []int64) {
	rand.Seed(time.Now().Unix())
	for i := range src {
		src[i] = rand.Int63()
	}
}

func benchForkJoinMergeSort(src []int64, size int) {
	tmp := make([]int64, len(src))
	parallelSize = size
	forkJoinMergeSort(src, tmp, 0, len(src))
}

func benchParallelMergeSort(src []int64, size int) {
	tmp := make([]int64, len(src))
	parallelSize = size
	forkJoinParallelMergeSort(src, tmp, 0, len(src))
}
