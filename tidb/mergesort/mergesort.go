package main

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	// NormalSort
	// BenchmarkNormalSort-12    	       1	2975116034 ns/op	      64 B/op	       2 allocs/op
	// BenchmarkNormalSort-12    	       1	3033260164 ns/op	      64 B/op	       2 allocs/op
	// BenchmarkNormalSort-12    	       1	3009185023 ns/op	      64 B/op	       2 allocs/op
	// BenchmarkNormalSort-12    	       1	2998787076 ns/op	      64 B/op	       2 allocs/op
	// BenchmarkNormalSort-12    	       1	2960347791 ns/op	      64 B/op	       2 allocs/op

	// naiveMergeSort(src, 0, len(src))
	// BenchmarkMergeSort-12     	       1	2931003333 ns/op	3221234912 B/op	16777235 allocs/op
	// BenchmarkMergeSort-12     	       1	2730617682 ns/op	3221225664 B/op	16777217 allocs/op
	// BenchmarkMergeSort-12     	       1	2679174528 ns/op	3221225760 B/op	16777218 allocs/op
	// BenchmarkMergeSort-12     	       1	2686156978 ns/op	3221226144 B/op	16777222 allocs/op
	// BenchmarkMergeSort-12     	       1	2682699670 ns/op	3221227360 B/op	16777219 allocs/op

	// tmp := make([]int64, len(src))
	// reduceAllocsMergeSort(src, tmp, 0, len(src))
	// BenchmarkMergeSort-12     	       1	2035406046 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	1965327742 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	1975974854 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	1962721425 ns/op	134217824 B/op	       2 allocs/op
	// BenchmarkMergeSort-12     	       1	2027160851 ns/op	134217728 B/op	       1 allocs/op

	// parallelSize = 1048576
	// tmp := make([]int64, len(src))
	// forkJoinMergeSort(src, tmp, 0, len(src))
	// BenchmarkMergeSort-12     	       3	 423988063 ns/op	134221600 B/op	      29 allocs/op
	// BenchmarkMergeSort-12     	       3	 440869802 ns/op	134220992 B/op	      24 allocs/op
	// BenchmarkMergeSort-12     	       3	 420740024 ns/op	134220864 B/op	      23 allocs/op
	// BenchmarkMergeSort-12     	       3	 443466346 ns/op	134220704 B/op	      21 allocs/op
	// BenchmarkMergeSort-12     	       3	 458170922 ns/op	134220448 B/op	      20 allocs/op

	parallelSize = 1048576
	tmp := make([]int64, len(src))
	forkJoinParallelMergeSort(src, tmp, 0, len(src))
	// BenchmarkMergeSort-12     	       3	 377076490 ns/op	134230293 B/op	     109 allocs/op
	// BenchmarkMergeSort-12     	       3	 364283067 ns/op	134229536 B/op	     111 allocs/op
	// BenchmarkMergeSort-12     	       3	 372533358 ns/op	134228106 B/op	     102 allocs/op
	// BenchmarkMergeSort-12     	       3	 371666103 ns/op	134229034 B/op	     111 allocs/op
	// BenchmarkMergeSort-12     	       3	 425077293 ns/op	134228426 B/op	     107 allocs/op
}

func naiveMergeSort(src []int64, low, high int) {
	if low+1 >= high {
		return
	}
	mid := low + (high-low)/2
	naiveMergeSort(src, low, mid)
	naiveMergeSort(src, mid, high)
	naiveMerge(src, low, mid, high)
}

func naiveMerge(src []int64, low, mid, high int) {
	if low >= mid || mid >= high {
		return
	}
	dest := make([]int64, high-low)
	ilow := low
	imid := mid
	i := 0
	for i < high-low && ilow < mid && imid < high {
		if src[ilow] <= src[imid] {
			dest[i] = src[ilow]
			ilow++
		} else {
			dest[i] = src[imid]
			imid++
		}
		i++
	}
	for i < high-low && ilow < mid {
		dest[i] = src[ilow]
		i++
		ilow++
	}
	for i < high-low && imid < high {
		dest[i] = src[imid]
		i++
		imid++
	}
	for j := low; j < high; j++ {
		src[j] = dest[j-low]
	}
}

func reduceAllocsMergeSort(src []int64, tmp []int64, low, high int) {
	if low+1 >= high {
		return
	}
	mid := low + (high-low)/2
	reduceAllocsMergeSort(src, tmp, low, mid)
	reduceAllocsMergeSort(src, tmp, mid, high)
	reduceAllocsMerge(src, tmp, low, mid, high)
}

func reduceAllocsMerge(src []int64, tmp []int64, low, mid, high int) {
	if low >= mid || mid >= high {
		return
	}
	// not work in bench, but should work in reality
	start := low
	if mid-low > 100 {
		start = binarySearch(src, src[mid], low, mid)
	}
	// copy src left part into tmp
	copy(tmp[start:], src[start:mid])
	ilow := start
	imid := mid
	i := start
	for i < high && ilow < mid && imid < high {
		if tmp[ilow] <= src[imid] {
			src[i] = tmp[ilow]
			ilow++
		} else {
			src[i] = src[imid]
			imid++
		}
		i++
	}
	// if element left in tmp, copy to src
	if i < high && ilow < mid {
		copy(src[i:], tmp[ilow:mid])
	}
}

var parallelSize int

func forkJoinMergeSort(src []int64, tmp []int64, low, high int) {
	// It will be slower if always go...
	// 10 is random choosed and not the optimized value
	// fallback to normal version
	if high-low <= parallelSize {
		reduceAllocsMergeSort(src, tmp, low, high)
		return
	}
	mid := low + (high-low)/2
	c := make(chan int)
	go func() {
		defer close(c)
		forkJoinMergeSort(src, tmp, low, mid)
	}()
	forkJoinMergeSort(src, tmp, mid, high)
	<-c
	reduceAllocsMerge(src, tmp, low, mid, high)
}

func forkJoinParallelMergeSort(src []int64, tmp []int64, low, high int) {
	if high-low <= parallelSize {
		reduceAllocsMergeSort(src, tmp, low, high)
		return
	}
	mid := low + (high-low)/2
	c := make(chan int)
	go func() {
		defer close(c)
		forkJoinParallelMergeSort(src, tmp, low, mid)
	}()
	forkJoinParallelMergeSort(src, tmp, mid, high)
	<-c
	parallelMerge(src, tmp, low, mid, high)
}

func parallelMerge(src, tmp []int64, low, mid, high int) {
	internalParallelMerge(src, low, mid, mid, high, tmp, low, high)
	copy(src[low:], tmp[low:high])
}

func internalParallelMerge(src []int64, leftLow, leftHigh, rightLow, rightHigh int, tmp []int64, low, high int) {
	if high-low < parallelSize || leftHigh-leftLow < parallelSize/4 || rightHigh-rightLow < parallelSize/4 {
		naiveMergeToTmp(src, leftLow, leftHigh, rightLow, rightHigh, tmp, low, high)
		return
	}
	left := leftLow + (leftHigh-leftLow)/2
	partitionValue := src[left]
	right := binarySearch(src, partitionValue, rightLow, rightHigh)
	tmpPartitionIndex := low + (left - leftLow) + (right - rightLow)
	tmp[tmpPartitionIndex] = partitionValue
	c := make(chan int)
	go func() {
		defer close(c)
		internalParallelMerge(src, leftLow, left, rightLow, right, tmp, low, tmpPartitionIndex)
	}()
	internalParallelMerge(src, left+1, leftHigh, right, rightHigh, tmp, tmpPartitionIndex+1, high)
	<-c
}

// binarySearch find the index in src[low, high) which is greater than or equal to target
func binarySearch(src []int64, target int64, low, high int) int {
	mid := low + (high-low)/2
	for low < high && mid > low && mid < high {
		if src[mid] == target {
			return mid
		} else if src[mid] > target {
			high = mid
		} else {
			low = mid + 1
		}
		mid = low + (high-low)/2
	}
	if mid < high && target > src[mid] {
		return mid + 1
	}
	return mid
}

func naiveMergeToTmp(src []int64, leftLow, leftHigh, rightLow, rightHigh int, tmp []int64, low, high int) {
	for leftLow < leftHigh && rightLow < rightHigh && low < high {
		if src[leftLow] < src[rightLow] {
			tmp[low] = src[leftLow]
			leftLow++
		} else {
			tmp[low] = src[rightLow]
			rightLow++
		}
		low++
	}
	// only 1 part can left elements
	if leftLow < leftHigh && low < high {
		copy(tmp[low:], src[leftLow:leftHigh])
	} else if rightLow < rightHigh && low < high {
		copy(tmp[low:], src[rightLow:rightHigh])
	}
}
