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
	log "github.com/sirupsen/logrus"
	"github.com/toolchainlabs/remote-api-tools/pkg/casutil"
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math/rand"
	"strconv"
	"time"
)

// This action uses the known digests of the current run to schedule read requests to the CAS.
// This is useful to verify CAS contents after generating known digests or loading a set of known digests.
type readAction struct {
	numDigestsToRead  int
	numReadsPerDigest int
	concurrency       int
}

func (self *readAction) String() string {
	return fmt.Sprintf("read %d:%d:%d", self.numDigestsToRead, self.numReadsPerDigest, self.concurrency)
}

type readResult struct {
	startTime time.Time
	endTime   time.Time
	success   int
	errors    int
}

type readWorkItem struct {
	actionContext *ActionContext
	digest        *remote_pb.Digest
}

type readWorkResult struct {
	digest *remote_pb.Digest
	err    error
}

func readWorker(workChan <-chan *readWorkItem, resultChan chan<- *readWorkResult) {
	log.Debug("readWorker started")
	for wi := range workChan {
		resultChan <- processReadWorkItem(wi)
	}
	log.Debug("readWorker stopped")
}

func processReadWorkItem(wi *readWorkItem) *readWorkResult {
	// TODO: This should use bytestream if size execeeds the server's batch RPC limit.

	request := remote_pb.BatchReadBlobsRequest{
		InstanceName: wi.actionContext.InstanceName,
		Digests: []*remote_pb.Digest{
			wi.digest,
		},
	}

	response, err := wi.actionContext.CasClient.BatchReadBlobs(wi.actionContext.Ctx, &request)
	if err == nil {
		s := status.FromProto(response.Responses[0].Status)
		if s.Code() != codes.OK {
			return &readWorkResult{
				digest: wi.digest,
				err:    s.Err(),
			}
		}

		data := response.Responses[0].Data
		if int64(len(data)) != wi.digest.SizeBytes {
			return &readWorkResult{
				digest: wi.digest,
				err:    fmt.Errorf("size mismatch, expected %d, actual %d", wi.digest.SizeBytes, len(data)),
			}
		}

		computedDigest := casutil.ComputeDigest(data)
		if computedDigest.Hash != wi.digest.Hash {
			return &readWorkResult{
				digest: wi.digest,
				err:    fmt.Errorf("digest hash mismatch"),
			}
		}
	} else {
		return &readWorkResult{
			digest: wi.digest,
			err:    err,
		}
	}

	return &readWorkResult{
		digest: wi.digest,
		err:    nil,
	}
}

func (self *readAction) RunAction(actionContext *ActionContext) error {
	result := readResult{
		startTime: time.Now(),
	}

	workChan := make(chan *readWorkItem)
	resultChan := make(chan *readWorkResult)

	for c := 0; c < self.concurrency; c++ {
		go readWorker(workChan, resultChan)
	}

	knownDigests := actionContext.GetKnownDigests(true)

	var workItems []*readWorkItem
	for i := 0; i < self.numDigestsToRead; i++ {
		index := rand.Int() % len(knownDigests)
		digest := knownDigests[index]
		for j := 0; j < self.numReadsPerDigest; j++ {
			workItem := &readWorkItem{
				actionContext: actionContext,
				digest:        digest,
			}
			workItems = append(workItems, workItem)
		}
	}

	// Shuffle the work items.
	rand.Shuffle(len(workItems), func(i, j int) {
		tmp := workItems[i]
		workItems[i] = workItems[j]
		workItems[j] = tmp
	})

	// Inject the work items into the channel.
	go func() {
		for _, workItem := range workItems {
			workChan <- workItem
		}

		close(workChan)
	}()

	// Wait for results.
	for i := 0; i < len(workItems); i++ {
		r := <-resultChan
		if r.err == nil {
			result.success += 1
		} else {
			result.errors += 1
			log.WithFields(log.Fields{
				"digest": r.digest,
				"err":    r.err.Error(),
			}).Error("request error")
		}

		if i%10 == 0 {
			log.Infof("progress: %d / %d", i, len(workItems))
		}
	}

	result.endTime = time.Now()

	close(resultChan)

	fmt.Printf("program: %s\n  startTime: %s\n  endTime: %s\n  success: %d\n  errors: %d\n",
		self.String(),
		result.startTime.String(),
		result.endTime.String(),
		result.success,
		result.errors,
	)

	return nil
}

func ParseReadAction(args []string) (Action, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("unable to parse program: expected 2-3 fields, got %d fields", len(args))
	}

	numDigestsToRead, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse number of digests to read from: %s: %s", args[0], err)
	}

	numReadsPerDigest, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, fmt.Errorf("unable to parse number of reads per digest: %s: %s", args[1], err)
	}

	concurrency := 50
	if len(args) >= 4 {
		c, err := strconv.Atoi(args[3])
		if err != nil {
			return nil, fmt.Errorf("unable to parse concurrency: %s: %s", args[3], err)
		}
		concurrency = c
	}

	action := readAction{
		numDigestsToRead:  numDigestsToRead,
		numReadsPerDigest: numReadsPerDigest,
		concurrency:       concurrency,
	}

	return &action, nil
}
