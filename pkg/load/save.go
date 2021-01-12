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
	"fmt"
	"os"
	"strconv"
	"strings"
)

// This action will save the "known digests" of a run to a file. This allows reuse of those digests in later
// load tests.s action).

type saveDigestsAction struct {
	filename string
}

func (self *saveDigestsAction) RunAction(actionContext *ActionContext) error {
	f, err := os.Create(self.filename)
	if err != nil {
		return fmt.Errorf("failed to load digests from file %s: %s", self.filename, err)
	}
	defer f.Close()

	for digest, present := range actionContext.KnownDigests {
		parts := strings.Split(digest, "-")
		if len(parts) != 2 {
			return fmt.Errorf("illegal state: known digests key was not parsed correctly")
		}

		line := fmt.Sprintf("%s,%s,%s\n", parts[0], parts[1], strconv.FormatBool(present))
		_, err = f.WriteString(line)
		if err != nil {
			return fmt.Errorf("failed to write to file: %s", err)
		}
	}

	return nil
}

func ParseSaveDigestsAction(args []string) (Action, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("filename to save to must be specified")
	}

	action := saveDigestsAction{
		filename: args[0],
	}

	return &action, nil
}
