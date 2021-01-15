// Copyright 2021 Toolchain Labs, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retry

import (
	"errors"
	"math/rand"
	"time"
)

var RetriesExceededError = errors.New("exceeded maximum number of retries")

// Retry a function with exponential backoff.
func ExpBackoff(
	n int,
	interval time.Duration,
	maxBackoffTime time.Duration,
	f func() (interface{}, error),
	isRetryable func(error) bool,
) (interface{}, error) {
	maxIntervals := 1
	attempt := 0

	for {
		result, err := f()
		if err == nil {
			return result, nil
		}

		if !isRetryable(err) {
			return nil, err
		}

		attempt += 1
		if attempt > n {
			return nil, RetriesExceededError
		}

		backoffTimeNanos := int64(1+rand.Int()%maxIntervals) * int64(interval)
		if backoffTimeNanos > int64(maxBackoffTime) {
			backoffTimeNanos = int64(maxBackoffTime)
		}
		time.Sleep(time.Duration(backoffTimeNanos))

		maxIntervals *= 2
	}
}
