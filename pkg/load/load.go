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

package load

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
)

// This action will load a set of digests from a file into the "known digests" of this run's action context.
// This allows using a known set of digests in a load test (e.g., digests saved from a previous run via the
// save digests action).

type loadDigestsAction struct {
	filename string
}

func (self *loadDigestsAction) RunAction(actionContext *ActionContext) error {
	f, err := os.Open(self.filename)
	if err != nil {
		return fmt.Errorf("failed to load digests from file %s: %s", self.filename, err)
	}
	defer f.Close()

	lineNum := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lineNum += 1
		line := scanner.Text()

		parts := strings.Split(line, ",")
		if len(parts) != 3 {
			return fmt.Errorf("invalid digest specifier (%s:%d)", self.filename, lineNum)
		}

		sizeBytes, err := strconv.Atoi(parts[1])
		if err != nil {
			return fmt.Errorf("invalid digest size (%s:%d)", self.filename, lineNum)
		}

		present, err := strconv.ParseBool(parts[2])
		if err != nil {
			return fmt.Errorf("invalid presennce specifier (%s:%d)", self.filename, lineNum)
		}

		digest := remote_pb.Digest{
			Hash:      parts[0],
			SizeBytes: int64(sizeBytes),
		}

		actionContext.AddKnownDigest(&digest, present)
	}

	return nil
}

func ParseLoadDigestsAction(args []string) (Action, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("filename to load must be specified")
	}

	action := loadDigestsAction{
		filename: args[0],
	}

	return &action, nil
}
