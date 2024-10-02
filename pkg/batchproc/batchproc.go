// Copyright 2024 Syntio Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
// The context passed to the Worker is canceled if any worker finishes with an error.
func Process(ctx context.Context, collectionSize, numWorkers int, worker Worker) error {
	if collectionSize < numWorkers || numWorkers == 1 {
		return worker(ctx, 0, collectionSize)
	}

	batchSize := collectionSize / numWorkers
	lastWorker := numWorkers - 1

	errorGroup, ctx := errgroup.WithContext(ctx)
	errorGroup.Go(func() error {
		return worker(ctx, lastWorker*batchSize, collectionSize)
	})

	for i := 0; i < lastWorker; i++ {
		start, end := i*batchSize, (i+1)*batchSize

		errorGroup.Go(func() error {
			return worker(ctx, start, end)
		})
	}

	return errorGroup.Wait()
}

// Parallel wraps Process with numWorkers set to the return value of runtime.GOMAXPROCS.
func Parallel(ctx context.Context, collectionSize int, worker Worker) error {
	return Process(ctx, collectionSize, runtime.GOMAXPROCS(0), worker)
}
