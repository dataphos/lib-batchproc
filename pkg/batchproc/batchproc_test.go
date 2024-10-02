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

package batchproc_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/dataphos/lib-batchproc/pkg/batchproc"
)

func TestProcess(t *testing.T) {
	type batch struct {
		start int
		end   int
	}

	type testCase struct {
		collectionSize int
		numWorkers     int
		// expected       map[Batch]int.
		actual map[batch]int
		mtx    *sync.Mutex
	}

	cases := []testCase{
		// basic Batch test.
		{
			collectionSize: 6,
			numWorkers:     3,
			/*expected: map[Batch]int{
				{0, 2}: 1,
				{2, 4}: 1,
				{4, 6}: 1,
			},*/
			actual: make(map[batch]int),
			mtx:    &sync.Mutex{},
		},
		// single worker.
		{
			collectionSize: 100,
			numWorkers:     1,
			/*expected: map[Batch]int{
				{0, 100}: 1,
			},*/
			actual: make(map[batch]int),
			mtx:    &sync.Mutex{},
		},
		// single worker forced through collectionSize being less than numWorker.
		{
			collectionSize: 2,
			numWorkers:     3,
			/*expected: map[Batch]int{
				{0, 2}: 1,
			},*/
			actual: make(map[batch]int),
			mtx:    &sync.Mutex{},
		},
		// single, no-op worker since collectionSize is 0.
		{
			collectionSize: 0,
			numWorkers:     3,
			/*expected: map[Batch]int{
				{0, 0}: 1,
			},*/
			actual: make(map[batch]int),
			mtx:    &sync.Mutex{},
		},
		// collectionSize the same as numWorkers.
		{
			collectionSize: 3,
			numWorkers:     3,
			/*expected: map[Batch]int{
				{0, 1}: 1,
				{1, 2}: 1,
				{2, 3}: 1,
			},*/
			actual: make(map[batch]int),
			mtx:    &sync.Mutex{},
		},
		// last Batch of a different size.
		{
			collectionSize: 51,
			numWorkers:     4,
			/*expected: map[Batch]int{
				{0, 12}:  1,
				{12, 24}: 1,
				{24, 36}: 1,
				{36, 51}: 1,
			},*/
			actual: make(map[batch]int),
			mtx:    &sync.Mutex{},
		},
		// last Batch of a different size.
		{
			collectionSize: 7,
			numWorkers:     2,
			/*expected: map[Batch]int{
				{0, 3}: 1,
				{3, 7}: 1,
			},*/
			actual: make(map[batch]int),
			mtx:    &sync.Mutex{},
		},
	}

	test := func(testCase testCase) error {
		worker := func(ctx context.Context, start, end int) error {
			testCase.mtx.Lock()
			defer testCase.mtx.Unlock()

			key := batch{start: start, end: end}
			count := testCase.actual[key]
			count++
			testCase.actual[key] = count

			return nil
		}

		return batchproc.Process(context.Background(), testCase.collectionSize, testCase.numWorkers, worker)
	}

	for i, currentCase := range cases {
		currentTestCase := currentCase

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if err := test(currentTestCase); err != nil {
				t.Fatal(err)
			}
		})
	}
}
