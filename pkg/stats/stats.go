// Copyright 2020 Toolchain Labs, Inc. All rights reserved.
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

package stats

import (
	"fmt"
	"sort"
	"time"
)

func PrintTimingStats(times []time.Duration) {
	sort.Slice(times, func(i, j int) bool {
		return times[i] < times[j]
	})

	total := time.Duration(0)
	for _, t := range times {
		total += t
	}

	average := time.Duration(float64(total) / float64(len(times)))

	fmt.Printf("  elapsed - total: %v\n  elapsed - average: %v\n  elapsed - min: %v\n  elapsed - max: %v\n",
		total,
		average,
		times[0],
		times[len(times)])
}
