package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"testing"
	"time"
)

func testDataScale() ([]DataSize, []int) {
	dataSize := []DataSize{1 * MB, 10 * MB, 100 * MB, 500 * MB, 1 * GB}
	nMapFiles := []int{5, 10, 20, 40, 60}
	return dataSize, nMapFiles
}

const (
	dataDir = "/tmp/mr_homework"
)

func dataPrefix(i int, ds DataSize, nMap int) string {
	return path.Join(dataDir, fmt.Sprintf("case%d-%s-%d", i, ds, nMap))
}

func TestGenData(t *testing.T) {
	gens := AllCaseGenFs()
	dataSize, nMapFiles := testDataScale()
	for k := range dataSize {
		for i, gen := range gens {
			fmt.Printf("generate data file for cast%d, dataSize=%v, nMap=%v\n", i, dataSize[k], nMapFiles[k])
			prefix := dataPrefix(i, dataSize[k], nMapFiles[k])
			gen(prefix, int(dataSize[k]), nMapFiles[k])
		}
	}
}

func TestCleanData(t *testing.T) {
	if err := os.RemoveAll(dataDir); err != nil {
		log.Fatal(err)
	}
}

// Case0 PASS, dataSize=1MB, nMapFiles=5, cost=106.282393ms
// Case1 PASS, dataSize=1MB, nMapFiles=5, cost=91.340113ms
// Case2 PASS, dataSize=1MB, nMapFiles=5, cost=94.377719ms
// Case3 PASS, dataSize=1MB, nMapFiles=5, cost=86.51286ms
// Case4 PASS, dataSize=1MB, nMapFiles=5, cost=89.557713ms
// Case5 PASS, dataSize=1MB, nMapFiles=5, cost=47.609539ms
// Case6 PASS, dataSize=1MB, nMapFiles=5, cost=36.832559ms
// Case7 PASS, dataSize=1MB, nMapFiles=5, cost=37.747296ms
// Case8 PASS, dataSize=1MB, nMapFiles=5, cost=36.789409ms
// Case9 PASS, dataSize=1MB, nMapFiles=5, cost=46.509214ms
// Case10 PASS, dataSize=1MB, nMapFiles=5, cost=25.964241ms
// Case0 PASS, dataSize=10MB, nMapFiles=10, cost=832.391055ms
// Case1 PASS, dataSize=10MB, nMapFiles=10, cost=878.999529ms
// Case2 PASS, dataSize=10MB, nMapFiles=10, cost=783.95106ms
// Case3 PASS, dataSize=10MB, nMapFiles=10, cost=805.207193ms
// Case4 PASS, dataSize=10MB, nMapFiles=10, cost=780.166877ms
// Case5 PASS, dataSize=10MB, nMapFiles=10, cost=300.443351ms
// Case6 PASS, dataSize=10MB, nMapFiles=10, cost=212.160914ms
// Case7 PASS, dataSize=10MB, nMapFiles=10, cost=210.041604ms
// Case8 PASS, dataSize=10MB, nMapFiles=10, cost=211.253447ms
// Case9 PASS, dataSize=10MB, nMapFiles=10, cost=308.702113ms
// Case10 PASS, dataSize=10MB, nMapFiles=10, cost=211.542319ms
// Case0 PASS, dataSize=100MB, nMapFiles=20, cost=4.504170172s
// Case1 PASS, dataSize=100MB, nMapFiles=20, cost=4.238113164s
// Case2 PASS, dataSize=100MB, nMapFiles=20, cost=4.360956915s
// Case3 PASS, dataSize=100MB, nMapFiles=20, cost=4.832005657s
// Case4 PASS, dataSize=100MB, nMapFiles=20, cost=4.355993415s
// Case5 PASS, dataSize=100MB, nMapFiles=20, cost=1.816069835s
// Case6 PASS, dataSize=100MB, nMapFiles=20, cost=1.890094392s
// Case7 PASS, dataSize=100MB, nMapFiles=20, cost=2.944618161s
// Case8 PASS, dataSize=100MB, nMapFiles=20, cost=1.837585121s
// Case9 PASS, dataSize=100MB, nMapFiles=20, cost=1.887453635s
// Case10 PASS, dataSize=100MB, nMapFiles=20, cost=1.671013082s
// Case0 PASS, dataSize=500MB, nMapFiles=40, cost=12.055140416s
// Case1 PASS, dataSize=500MB, nMapFiles=40, cost=11.901577391s
// Case2 PASS, dataSize=500MB, nMapFiles=40, cost=11.48302667s
// Case3 PASS, dataSize=500MB, nMapFiles=40, cost=11.319804316s
// Case4 PASS, dataSize=500MB, nMapFiles=40, cost=13.376146597s
// Case5 PASS, dataSize=500MB, nMapFiles=40, cost=12.643473894s
// Case6 PASS, dataSize=500MB, nMapFiles=40, cost=12.120827798s
// Case7 PASS, dataSize=500MB, nMapFiles=40, cost=9.978886647s
// Case8 PASS, dataSize=500MB, nMapFiles=40, cost=9.500893958s
// Case9 PASS, dataSize=500MB, nMapFiles=40, cost=15.022160532s
// Case10 PASS, dataSize=500MB, nMapFiles=40, cost=6.994823044s
// Case0 PASS, dataSize=1GB, nMapFiles=60, cost=20.813217786s
// Case1 PASS, dataSize=1GB, nMapFiles=60, cost=21.93183097s
// Case2 PASS, dataSize=1GB, nMapFiles=60, cost=23.55116378s
// Case3 PASS, dataSize=1GB, nMapFiles=60, cost=22.717625646s
// Case4 PASS, dataSize=1GB, nMapFiles=60, cost=21.267609407s
// Case5 PASS, dataSize=1GB, nMapFiles=60, cost=22.073574437s
// Case6 PASS, dataSize=1GB, nMapFiles=60, cost=22.939903923s
// Case7 PASS, dataSize=1GB, nMapFiles=60, cost=22.341838251s
// Case8 PASS, dataSize=1GB, nMapFiles=60, cost=23.974317627s
// Case9 PASS, dataSize=1GB, nMapFiles=60, cost=20.550540869s
// Case10 PASS, dataSize=1GB, nMapFiles=60, cost=16.145749729s
func TestExampleURLTop(t *testing.T) {
	rounds := ExampleURLTop10(GetMRCluster().NWorkers())
	testURLTop(t, rounds)
}

// Case0 PASS, dataSize=1MB, nMapFiles=5, cost=37.443645ms
// Case1 PASS, dataSize=1MB, nMapFiles=5, cost=42.912878ms
// Case2 PASS, dataSize=1MB, nMapFiles=5, cost=31.449136ms
// Case3 PASS, dataSize=1MB, nMapFiles=5, cost=32.946349ms
// Case4 PASS, dataSize=1MB, nMapFiles=5, cost=35.634311ms
// Case5 PASS, dataSize=1MB, nMapFiles=5, cost=18.529656ms
// Case6 PASS, dataSize=1MB, nMapFiles=5, cost=17.708549ms
// Case7 PASS, dataSize=1MB, nMapFiles=5, cost=15.154164ms
// Case8 PASS, dataSize=1MB, nMapFiles=5, cost=15.983723ms
// Case9 PASS, dataSize=1MB, nMapFiles=5, cost=15.559892ms
// Case10 PASS, dataSize=1MB, nMapFiles=5, cost=10.801945ms
// Case0 PASS, dataSize=10MB, nMapFiles=10, cost=241.608224ms
// Case1 PASS, dataSize=10MB, nMapFiles=10, cost=247.527744ms
// Case2 PASS, dataSize=10MB, nMapFiles=10, cost=227.834371ms
// Case3 PASS, dataSize=10MB, nMapFiles=10, cost=243.058787ms
// Case4 PASS, dataSize=10MB, nMapFiles=10, cost=234.960711ms
// Case5 PASS, dataSize=10MB, nMapFiles=10, cost=42.814682ms
// Case6 PASS, dataSize=10MB, nMapFiles=10, cost=43.566608ms
// Case7 PASS, dataSize=10MB, nMapFiles=10, cost=44.588388ms
// Case8 PASS, dataSize=10MB, nMapFiles=10, cost=41.045534ms
// Case9 PASS, dataSize=10MB, nMapFiles=10, cost=39.024979ms
// Case10 PASS, dataSize=10MB, nMapFiles=10, cost=26.075548ms
// Case0 PASS, dataSize=100MB, nMapFiles=20, cost=1.920594255s
// Case1 PASS, dataSize=100MB, nMapFiles=20, cost=2.012244072s
// Case2 PASS, dataSize=100MB, nMapFiles=20, cost=1.829078348s
// Case3 PASS, dataSize=100MB, nMapFiles=20, cost=1.866046963s
// Case4 PASS, dataSize=100MB, nMapFiles=20, cost=2.00154587s
// Case5 PASS, dataSize=100MB, nMapFiles=20, cost=210.150545ms
// Case6 PASS, dataSize=100MB, nMapFiles=20, cost=210.231311ms
// Case7 PASS, dataSize=100MB, nMapFiles=20, cost=192.77724ms
// Case8 PASS, dataSize=100MB, nMapFiles=20, cost=192.382181ms
// Case9 PASS, dataSize=100MB, nMapFiles=20, cost=187.579803ms
// Case10 PASS, dataSize=100MB, nMapFiles=20, cost=125.577757ms
// Case0 PASS, dataSize=500MB, nMapFiles=40, cost=7.341428423s
// Case1 PASS, dataSize=500MB, nMapFiles=40, cost=7.732061074s
// Case2 PASS, dataSize=500MB, nMapFiles=40, cost=8.11600751s
// Case3 PASS, dataSize=500MB, nMapFiles=40, cost=7.954307758s
// Case4 PASS, dataSize=500MB, nMapFiles=40, cost=8.197992107s
// Case5 PASS, dataSize=500MB, nMapFiles=40, cost=1.139405179s
// Case6 PASS, dataSize=500MB, nMapFiles=40, cost=983.625288ms
// Case7 PASS, dataSize=500MB, nMapFiles=40, cost=878.914657ms
// Case8 PASS, dataSize=500MB, nMapFiles=40, cost=787.637334ms
// Case9 PASS, dataSize=500MB, nMapFiles=40, cost=947.469785ms
// Case10 PASS, dataSize=500MB, nMapFiles=40, cost=757.972149ms
// Case0 PASS, dataSize=1GB, nMapFiles=60, cost=16.564670366s
// Case1 PASS, dataSize=1GB, nMapFiles=60, cost=18.416545178s
// Case2 PASS, dataSize=1GB, nMapFiles=60, cost=14.948088631s
// Case3 PASS, dataSize=1GB, nMapFiles=60, cost=15.022978494s
// Case4 PASS, dataSize=1GB, nMapFiles=60, cost=15.425586216s
// Case5 PASS, dataSize=1GB, nMapFiles=60, cost=1.687496475s
// Case6 PASS, dataSize=1GB, nMapFiles=60, cost=2.222470047s
// Case7 PASS, dataSize=1GB, nMapFiles=60, cost=2.684959123s
// Case8 PASS, dataSize=1GB, nMapFiles=60, cost=2.289995457s
// Case9 PASS, dataSize=1GB, nMapFiles=60, cost=1.835699471s
// Case10 PASS, dataSize=1GB, nMapFiles=60, cost=1.166581338s
// --- PASS: TestURLTop (151.37s)
func TestURLTop(t *testing.T) {
	rounds := URLTop10(GetMRCluster().NWorkers())
	testURLTop(t, rounds)
}

func testURLTop(t *testing.T, rounds RoundsArgs) {
	if len(rounds) == 0 {
		t.Fatalf("no rounds arguments, please finish your code")
	}
	mr := GetMRCluster()

	// run all cases
	gens := AllCaseGenFs()
	dataSize, nMapFiles := testDataScale()
	for k := range dataSize {
		for i, gen := range gens {
			// generate data
			prefix := dataPrefix(i, dataSize[k], nMapFiles[k])
			c := gen(prefix, int(dataSize[k]), nMapFiles[k])

			runtime.GC()

			// run map-reduce rounds
			begin := time.Now()
			inputFiles := c.MapFiles
			for idx, r := range rounds {
				jobName := fmt.Sprintf("Case%d-Round%d", i, idx)
				ch := mr.Submit(jobName, prefix, r.MapFunc, r.ReduceFunc, inputFiles, r.NReduce)
				inputFiles = <-ch
			}
			cost := time.Since(begin)

			// check result
			if len(inputFiles) != 1 {
				panic("the length of result file list should be 1")
			}
			result := inputFiles[0]

			if errMsg, ok := CheckFile(c.ResultFile, result); !ok {
				t.Fatalf("Case%d FAIL, dataSize=%v, nMapFiles=%v, cost=%v\n%v\n", i, dataSize[k], nMapFiles[k], cost, errMsg)
			} else {
				fmt.Printf("Case%d PASS, dataSize=%v, nMapFiles=%v, cost=%v\n", i, dataSize[k], nMapFiles[k], cost)
			}
		}
	}
}
