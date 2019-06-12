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
	// BenchmarkMergeSort-12     	       1	2191147198 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	2070902923 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	2068486601 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	2164531992 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	2110035179 ns/op	134217728 B/op	       1 allocs/op

	// parallelSize = 1048576
	// tmp := make([]int64, len(src))
	// forkJoinMergeSort(src, tmp, 0, len(src))
	// BenchmarkMergeSort-12     	       3	 478334197 ns/op	134221248 B/op	      27 allocs/op
	// BenchmarkMergeSort-12     	       3	 479179199 ns/op	134221440 B/op	      27 allocs/op
	// BenchmarkMergeSort-12     	       3	 479248880 ns/op	134220085 B/op	      20 allocs/op
	// BenchmarkMergeSort-12     	       3	 474184832 ns/op	134220853 B/op	      22 allocs/op
	// BenchmarkMergeSort-12     	       3	 491231127 ns/op	134220224 B/op	      20 allocs/op

	parallelSize = 1048576
	tmp := make([]int64, len(src))
	forkJoinParallelMergeSort(src, tmp, 0, len(src))
	// BenchmarkMergeSort-12     	       3	 412101605 ns/op	134234805 B/op	     125 allocs/op
	// BenchmarkMergeSort-12     	       3	 402700463 ns/op	134228288 B/op	     100 allocs/op
	// BenchmarkMergeSort-12     	       3	 417370068 ns/op	134229141 B/op	     108 allocs/op
	// BenchmarkMergeSort-12     	       3	 399888826 ns/op	134231093 B/op	     107 allocs/op
	// BenchmarkMergeSort-12     	       3	 419092391 ns/op	134229386 B/op	     110 allocs/op
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
	ilow := low
	imid := mid
	i := low
	for i < high && ilow < mid && imid < high {
		if src[ilow] <= src[imid] {
			tmp[i] = src[ilow]
			ilow++
		} else {
			tmp[i] = src[imid]
			imid++
		}
		i++
	}
	for i < high && ilow < mid {
		tmp[i] = src[ilow]
		i++
		ilow++
	}
	for i < high && imid < high {
		tmp[i] = src[imid]
		i++
		imid++
	}
	for j := low; j < high; j++ {
		src[j] = tmp[j]
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
	for j := low; j < high; j++ {
		src[j] = tmp[j]
	}
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
	for leftLow < leftHigh && low < high {
		tmp[low] = src[leftLow]
		leftLow++
		low++
	}
	for rightLow < rightHigh && low < high {
		tmp[low] = src[rightLow]
		rightLow++
		low++
	}
}
