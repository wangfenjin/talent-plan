package main

import (
	"errors"

	"github.com/psilva261/timsort"
)

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
	// BenchmarkMergeSort-12     	       1	1867831612 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	1811441913 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	1826893286 ns/op	134217728 B/op	       1 allocs/op
	// BenchmarkMergeSort-12     	       1	1823869470 ns/op	134217824 B/op	       2 allocs/op
	// BenchmarkMergeSort-12     	       1	1830547453 ns/op	134217728 B/op	       1 allocs/op

	// NOTE: very slow, slower than NormalSort
	// timSort(src, 0, len(src))
	// BenchmarkMergeSort-12     	       1	12127359432 ns/op	268434080 B/op	      20 allocs/op
	// BenchmarkMergeSort-12     	       1	12242139117 ns/op	257778272 B/op	      20 allocs/op
	// BenchmarkMergeSort-12     	       1	12049227596 ns/op	250469024 B/op	      16 allocs/op
	// BenchmarkMergeSort-12     	       1	11900753235 ns/op	234217888 B/op	      19 allocs/op
	// BenchmarkMergeSort-12     	       1	11539638930 ns/op	268434080 B/op	      20 allocs/op

	// parallelSize = 65536
	// tmp := make([]int64, len(src))
	// forkJoinMergeSort(src, tmp, 0, len(src))
	// BenchmarkMergeSort-12     	       3	 391885365 ns/op	134252778 B/op	     302 allocs/op
	// BenchmarkMergeSort-12     	       3	 388743634 ns/op	134251349 B/op	     299 allocs/op
	// BenchmarkMergeSort-12     	       3	 384621605 ns/op	134247552 B/op	     274 allocs/op
	// BenchmarkMergeSort-12     	       3	 393098988 ns/op	134245962 B/op	     278 allocs/op
	// BenchmarkMergeSort-12     	       3	 385350466 ns/op	134243466 B/op	     262 allocs/op

	parallelSize = 65536
	tmp := make([]int64, len(src))
	forkJoinParallelMergeSort(src, tmp, 0, len(src))
	// BenchmarkMergeSort-12     	       5	 322563114 ns/op	134521555 B/op	    3124 allocs/op
	// BenchmarkMergeSort-12     	       5	 324330249 ns/op	134515097 B/op	    3071 allocs/op
	// BenchmarkMergeSort-12     	       5	 347284114 ns/op	134515116 B/op	    3104 allocs/op
	// BenchmarkMergeSort-12     	       5	 334181742 ns/op	134510028 B/op	    3048 allocs/op
	// BenchmarkMergeSort-12     	       3	 350602169 ns/op	134517301 B/op	    3126 allocs/op
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
	if high-low < 32 {
		_ = binarySort(src, low, high, low)
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

// Copy from https://github.com/psilva261/timsort/blob/master/timsort.go#L233
func binarySort(a []int64, lo, hi, start int) (err error) {
	if lo > start || start > hi {
		return errors.New("lo <= start && start <= hi")
	}

	if start == lo {
		start++
	}

	for ; start < hi; start++ {
		pivot := a[start]

		// Set left (and right) to the index where a[start] (pivot) belongs
		left := lo
		right := start

		if left > right {
			return errors.New("left <= right")
		}

		/*
		 * Invariants:
		 *   pivot >= all in [lo, left).
		 *   pivot <  all in [right, start).
		 */
		for left < right {
			mid := int(uint(left+right) >> 1)
			if pivot < a[mid] {
				right = mid
			} else {
				left = mid + 1
			}
		}

		if left != right {
			return errors.New("left == right")
		}

		/*
		 * The invariants still hold: pivot >= all in [lo, left) and
		 * pivot < all in [left, start), so pivot belongs at left.  Note
		 * that if there are elements equal to pivot, left points to the
		 * first slot after them -- that's why this sort is stable.
		 * Slide elements over to make room to make room for pivot.
		 */
		n := start - left // The number of elements to move
		// just an optimization for copy in default case
		if n <= 2 {
			if n == 2 {
				a[left+2] = a[left+1]
			}
			if n > 0 {
				a[left+1] = a[left]
			}
		} else {
			copy(a[left+1:], a[left:left+n])
		}
		a[left] = pivot
	}
	return
}

// Bench timsort
// IntSlice attaches the methods of Interface to []int64, sorting in increasing order.
type IntSlice []int64

func (p IntSlice) Len() int           { return len(p) }
func (p IntSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p IntSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func timSort(src []int64, low, high int) {
	timsort.TimSort(IntSlice(src[low:high]))
}
