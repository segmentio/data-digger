package stats

// Bucket is a collection that we keep counts for in a heap.
type Bucket struct {
	Key   string
	Count int
	Index int

	// Statistics about the values in this bucket
	Min float64
	Max float64
	Sum float64
}

// NewBucket creates a new Bucket instance for the argument key and value.
func NewBucket(key string, value float64) *Bucket {
	return &Bucket{
		Key:   key,
		Count: 1,
		Min:   value,
		Max:   value,
		Sum:   value,
	}
}

// Update updates the counts and stats for this bucket for the argument value.
func (i *Bucket) Update(value float64) {
	i.Count++
	i.Sum += value
	if value < i.Min {
		i.Min = value
	}
	if value > i.Max {
		i.Max = value
	}
}

// Avg returns the average value for this bucket.
func (i *Bucket) Avg() float64 {
	return i.Sum / float64(i.Count)
}

// BucketsHeap is a heap.Interface implementation that holds Buckets.
// Based on example in https://golang.org/pkg/container/heap/#example__priorityQueue.
type BucketsHeap []*Bucket

// Len returns the number of buckets in the heap.
func (h BucketsHeap) Len() int { return len(h) }

// Less returns whether the ith element in the heap is less than the jth one.
func (h BucketsHeap) Less(i, j int) bool {
	return h[i].Count > h[j].Count ||
		(h[i].Count == h[j].Count && h[i].Key < h[j].Key)
}

// Swap swaps the ith and jth elements and updates the indices for each.
func (h BucketsHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

// Push adds a new item to the heap.
func (h *BucketsHeap) Push(x interface{}) {
	n := len(*h)
	bucket := x.(*Bucket)
	bucket.Index = n
	*h = append(*h, bucket)
}

// Pop removes the smallest item from the heap.
func (h *BucketsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	bucket := old[n-1]
	old[n-1] = nil
	bucket.Index = -1
	*h = old[0 : n-1]
	return bucket
}
