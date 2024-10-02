// Package batchproc offers utility functions which ease processing of indexable (array-like) collections.
package batchproc

import (
	"context"
	"runtime"

	"golang.org/x/sync/errgroup"
)

// Worker function type used for processing a batch of an array-like collection.
type Worker func(context.Context, int, int) error

// Process splits the collection, provided implicitly through collectionSize into numWorkers amount of batches,
// and calls the Worker on the batches. Batches are processed concurrently.
// Blocks until all the batches are processed and returns the first non-nil error (if any).
// The context passed to the Worker is cancelled if any worker finishes with an error.
func Process(ctx context.Context, collectionSize, numWorkers int, worker Worker) error {
	if collectionSize < numWorkers || numWorkers == 1 {
		return worker(ctx, 0, collectionSize)
	}

	batchSize := collectionSize / numWorkers
	lastWorker := numWorkers - 1

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return worker(ctx, lastWorker*batchSize, collectionSize)
	})
	for i := 0; i < lastWorker; i++ {
		start, end := i*batchSize, (i+1)*batchSize
		eg.Go(func() error {
			return worker(ctx, start, end)
		})
	}
	return eg.Wait()
}

// Parallel wraps Process with numWorkers set to the return value of runtime.GOMAXPROCS
func Parallel(ctx context.Context, collectionSize int, worker Worker) error {
	return Process(ctx, collectionSize, runtime.GOMAXPROCS(0), worker)
}
