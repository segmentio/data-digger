package stats

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/olekukonko/tablewriter"
)

const (
	// AllValue is a special bucket key that represents all possible values.
	AllValue = "__all__"

	// InvalidValue is a special bucket key that represents an invalid message.
	InvalidValue = "__invalid__"

	// MissingValue is a special bucket key that represents a missing value in a message.
	MissingValue = "__missing__"

	// DimSeparator is a special string that's used to separate dimensions in multi-dimensional
	// bucket keys.
	DimSeparator = "∪∪"
)

// TopKCounter is a counter that keeps stats on the top K keys seen so far. It uses a BucketsHeap
// behind the scenes to do this in a memory-efficient way.
type TopKCounter struct {
	sync.Mutex

	k            int
	bucketsHeap  *BucketsHeap
	bucketsMap   map[string]*Bucket
	totalAdded   int
	totalRemoved int
	totalMissing int
	totalInvalid int
}

// TopKCounterSummary is a summary of the current top K state. It's used for the progress
// display when the digger is running.
type TopKCounterSummary struct {
	TotalAdded    int
	TotalRemoved  int
	TotalMissing  int
	TotalInvalid  int
	NumCategories int
}

// NewTopKCounter creates a new TopKCounter instance for the argument k value.
func NewTopKCounter(k int) *TopKCounter {
	counter := &TopKCounter{
		k:           k,
		bucketsHeap: &BucketsHeap{},
		bucketsMap:  map[string]*Bucket{},
	}

	heap.Init(counter.bucketsHeap)
	return counter
}

// Add updates the counter state for the argument key and value. If the key is not currently
// in the heap, then a bucket is created for it.
func (t *TopKCounter) Add(key string, value float64) error {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	t.totalAdded++

	if key == MissingValue {
		t.totalMissing++
	} else if key == InvalidValue {
		t.totalInvalid++
	}

	bucket, ok := t.bucketsMap[key]
	if !ok {
		newBucket := NewBucket(key, value)
		t.bucketsMap[key] = newBucket

		heap.Push(
			t.bucketsHeap,
			newBucket,
		)
	} else {
		bucket.Update(value)
		heap.Fix(t.bucketsHeap, bucket.Index)
	}

	// Clean down to 100k instead of k so that we can get a better approximation
	// of values
	if len(*t.bucketsHeap) > 200*t.k {
		t.Clean(100 * t.k)
	}

	return nil
}

// Clean removes items from the heap to get the size down to the argument limit.
func (t *TopKCounter) Clean(limit int) {
	if len(*t.bucketsHeap) < limit {
		return
	}

	for i := 0; i < len(*t.bucketsHeap)-limit; i++ {
		bucket := heap.Remove(t.bucketsHeap, len(*t.bucketsHeap)-1).(*Bucket)

		t.totalRemoved += bucket.Count
		delete(t.bucketsMap, bucket.Key)
	}
}

// Buckets returns all of the buckets currently in the heap, sorted by count and key.
func (t *TopKCounter) Buckets(limit int, sortByName bool) []Bucket {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	buckets := []Bucket{}

	for _, bucket := range *t.bucketsHeap {
		buckets = append(buckets, *bucket)
	}

	sort.Slice(buckets, func(a, b int) bool {
		return buckets[a].Count > buckets[b].Count ||
			(buckets[a].Count == buckets[b].Count && buckets[a].Key < buckets[b].Key)
	})

	if len(buckets) > limit {
		buckets = buckets[0:limit]
	}

	if sortByName {
		// Do this sort after the value sort so we still get the top k by value
		sort.Slice(buckets, func(a, b int) bool {
			return buckets[a].Key < buckets[b].Key
		})
	}

	return buckets
}

// Summary returns a summary of the current state of this counter instance.
func (t *TopKCounter) Summary() TopKCounterSummary {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	return TopKCounterSummary{
		TotalAdded:    t.totalAdded,
		TotalRemoved:  t.totalRemoved,
		TotalMissing:  t.totalMissing,
		TotalInvalid:  t.totalInvalid,
		NumCategories: len(t.bucketsMap),
	}
}

// PrettyTable returns a pretty table that summarizes the stats for the top k
// values in this counter instance.
func (t *TopKCounter) PrettyTable(n int, numeric bool, sortByName bool) string {
	if numeric && n > 1 {
		// Don't create column for the numeric dimension
		n--
	}

	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	header := []string{
		"Rank",
	}

	if n == 1 {
		header = append(header, "Bucket")
	} else {
		for i := 0; i < n; i++ {
			header = append(header, fmt.Sprintf("Dim %d", i+1))
		}
	}

	if numeric {
		header = append(
			header,
			"Min",
			"Avg",
			"Max",
		)
	}

	header = append(
		header,
		"Count",
		"Percent",
		"Cumulative",
	)

	table.SetHeader(header)

	table.SetAutoWrapText(false)

	columnAlignments := []int{
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
	}

	for i := 0; i < n; i++ {
		columnAlignments = append(columnAlignments, tablewriter.ALIGN_LEFT)
	}

	table.SetColumnAlignment(columnAlignments)
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    true,
			Right:  false,
			Bottom: true,
		},
	)

	cumlPercent := 0.0

	buckets := t.Buckets(t.k, sortByName)
	for i, bucket := range buckets {
		percent := float64(bucket.Count) / float64(t.totalAdded-t.totalRemoved) * 100.0
		cumlPercent += percent

		bucketComponents := strings.SplitN(bucket.Key, DimSeparator, n)

		row := []string{
			fmt.Sprintf("%d", i+1),
		}

		for _, bucketComponent := range bucketComponents {
			row = append(row, bucketComponent)
		}

		if len(bucketComponents) < n {
			for j := 0; j < n-len(bucketComponents); j++ {
				row = append(row, "")
			}
		}

		if numeric {
			if bucket.Key == MissingValue || bucket.Key == InvalidValue {
				row = append(row, "", "", "")
			} else {
				row = append(
					row,
					fmt.Sprintf("%f", bucket.Min),
					fmt.Sprintf("%f", bucket.Avg()),
					fmt.Sprintf("%f", bucket.Max),
				)
			}
		}

		row = append(
			row,
			fmt.Sprintf("%d", bucket.Count),
			fmt.Sprintf("%0.2f%%", percent),
			fmt.Sprintf("%0.2f%%", cumlPercent),
		)

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}
