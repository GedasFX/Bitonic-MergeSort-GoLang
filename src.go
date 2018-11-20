package main

import "sync"
import "math/rand"
import "time"
import "fmt"
import "os"

/* Bitonic sort algorithm from https://www.geeksforgeeks.org/bitonic-sort/ */
/* Modified to work on parallel and trensfered to GO */

/*The parameter dir indicates the sorting direction, ASCENDING
or DESCENDING; if (a[i] > a[j]) agrees with the direction,
then a[i] and a[j] are interchanged.*/
func cmpAndSwap(a[] int, i int, j int, dir bool) {
	if (dir == (a[i] > a[j])) {
		a[i], a[j] = a[j], a[i];
	}
}

/*It recursively sorts a bitonic sequence in ascending order,
	if dir = 1, and in descending order otherwise (means dir=0).
	The sequence to be sorted starts at index position low,
	the parameter cnt is the number of elements to be sorted.*/
func bitonicMerge(a[] int, low int, cnt int, dir bool) {
	if (cnt > 1) {
		var k = cnt / 2;
		for i := low; i < low + k; i++ {
			cmpAndSwap(a, i, i + k, dir);
		}
		bitonicMerge(a, low, k, dir);
		bitonicMerge(a, low + k, k, dir);
	}
}

/* This function first produces a bitonic sequence by recursively
	sorting its two halves in opposite sorting orders, and then
	calls bitonicMerge to make them in the same order */
func bitonicSort(a[] int, low int, cnt int, dir bool) {
	if (cnt > 1) {
		var k = cnt / 2;
		// sort in ascending order since dir here is 1
		bitonicSort(a, low, k, true);
		// sort in descending order since dir here is 0 
		bitonicSort(a, low + k, k, false);

		// Merges entire equence in DIR order
		bitonicMerge(a, low, cnt, dir);
	}
}

/* Sorts threads parallely effectivelly - does not split more than it needs to */
func bitonicSortParallel(a[] int, low int, cnt int, dir bool, availableThreads int, owg *sync.WaitGroup) {
	defer owg.Done();
	// Cannot split up the process parralel more than it currently is. Continue operations as single thread.
	if (availableThreads == 1) {
		bitonicSort(a, low, cnt, dir);
	} else {
		var k = cnt / 2;
		var wg sync.WaitGroup; wg.Add(2);
		go bitonicSortParallel(a, low, k, true, availableThreads >> 1, &wg);
		bitonicSortParallel(a, low + k, k, false, availableThreads >> 1, &wg);
		wg.Wait();

		// Dont need to split this up as method is already split up to num of threads.
		bitonicMerge(a, low, cnt, dir);
	}
}

/* Caller of bitonicSort for sorting the entire array of
	length N in ASCENDING order */
func sort(a[] int, N int, up bool) {
	bitonicSort(a, 0, N, up);
}

/* Parallel sorter, to split work among N threads */
func sortParallel(a[] int, n int, threadCount int) {
	var wg sync.WaitGroup; wg.Add(1);
	bitonicSortParallel(a, 0, n, true, threadCount, &wg);
	wg.Wait();
}

/* One test with a given thread count, input data and required accuracy */
func testCase(threadCount int, n int, accuracy int) int64 {
	var seq = make([]int, n);
	var timeTaken time.Duration;

	rand := rand.New(rand.NewSource(99))

	for j := 0; j < accuracy; j++ {
		for i := 0; i < n; i++ {
			seq[i] = int(rand.Int31());
		}

		// Sort array ascending.
		var t1 = time.Now();
		sortParallel(seq, n, threadCount);
		var t2 = time.Now();
		timeTaken += t2.Sub(t1) / time.Millisecond;

		// Check is result fully sorted. Confimation test, mostly for debug purposes.
		/*
		for i := 1; i < n; i++ {
			if (seq[i - 1] > seq[i]) {
				fmt.Println("Cycle invariant was broken!");
				panic(1);
			}
		}
		*/
	}

	return int64(timeTaken) / int64(accuracy);
}

func main() {
	// Truncates the file for clean writing
	f, _ := os.OpenFile("out.csv", os.O_WRONLY, 0644); f.Truncate(0); f.Close();
	for sampleSize := 1 << 4; sampleSize <= 1 << 24; sampleSize = sampleSize << 1 {
		for threadCount := 1; threadCount <= 1 << 3; threadCount = threadCount << 1 {
			fmt.Printf("Working on: %d %d\n", threadCount, sampleSize);
			f, _ := os.OpenFile("out.csv", os.O_APPEND|os.O_WRONLY, 0644)
			var calc = testCase(threadCount, sampleSize, 10);
			f.WriteString(fmt.Sprintf("%d,%d,%d\n", threadCount, sampleSize, calc)); f.Close();
		}
	}
}