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
	"math/rand"
	"strconv"
	"time"

	"github.com/toolchainlabs/remote-api-tools/pkg/casutil"
	"github.com/toolchainlabs/remote-api-tools/pkg/stats"
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	startTime       time.Time
	endTime         time.Time
	success         int
	errors          int
	byteStreamReads int
	blobReads       int
}

type readWorkItem struct {
	actionContext *ActionContext
	digest        *remote_pb.Digest
}

type readWorkResult struct {
	digest  *remote_pb.Digest
	err     error
	elapsed time.Duration
}

func readWorker(workChan <-chan *readWorkItem, resultChan chan<- *readWorkResult) {
	log.Debug("readWorker started")
	for wi := range workChan {
		startTime := time.Now()
		result := processReadWorkItem(wi)
		result.elapsed = time.Now().Sub(startTime)
		resultChan <- result
	}
	log.Debug("readWorker stopped")
}

func readBlobBatch(actionContext *ActionContext, digest *remote_pb.Digest) ([]byte, error) {
	request := remote_pb.BatchReadBlobsRequest{
		InstanceName: actionContext.InstanceName,
		Digests: []*remote_pb.Digest{
			digest,
		},
	}

	response, err := actionContext.CasClient.BatchReadBlobs(actionContext.Ctx, &request)
	if err != nil {
		return nil, err
	}

	s := status.FromProto(response.Responses[0].Status)
	if s.Code() != codes.OK {
		return nil, s.Err()
	}

	if len(response.Responses) != 1 {
		return nil, fmt.Errorf("expected 1 response in RPC call, got %d responses instead", len(response.Responses))
	}

	digestResponse := response.Responses[0]

	if digestResponse.Digest.Hash != digest.Hash {
		return nil, fmt.Errorf("expected digest %s in RPC call, got digest %s instead", digest.Hash, digestResponse.Digest.Hash)
	}

	if digestResponse.Digest.SizeBytes != digest.SizeBytes {
		return nil, fmt.Errorf("expected size %d in RPC call, got size %d instead", digest.SizeBytes, digestResponse.Digest.SizeBytes)
	}

	return response.Responses[0].Data, nil
}

func readBlobStream(actionContext *ActionContext, digest *remote_pb.Digest) ([]byte, error) {
	data, err := casutil.GetBytesStream(actionContext.Ctx, actionContext.BytestreamClient, digest, actionContext.InstanceName)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob (bytestream): %s", err)
	}

	return data, nil
}

func processReadWorkItem(wi *readWorkItem) *readWorkResult {
	var data []byte
	var err error

	if wi.digest.SizeBytes < wi.actionContext.MaxBatchBlobSize {
		data, err = readBlobBatch(wi.actionContext, wi.digest)
	} else {
		data, err = readBlobStream(wi.actionContext, wi.digest)
	}
	if err != nil {
		return &readWorkResult{
			digest: wi.digest,
			err:    err,
		}
	}

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
			err:    fmt.Errorf("digest hash mismatch, expected %s, actual %s", wi.digest, computedDigest.Hash),
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

	elapsedTimes := make([]time.Duration, len(workItems))

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
		elapsedTimes[i] = r.elapsed
		if int64(r.digest.SizeBytes) < actionContext.MaxBatchBlobSize {
			result.blobReads += 1
		} else {
			result.byteStreamReads += 1
		}

		if i%100 == 0 {
			log.Debugf("progress: %d / %d", i, len(workItems))
		}
	}

	result.endTime = time.Now()

	close(resultChan)

	fmt.Printf("program: %s\n  startTime: %s\n  endTime: %s\n  success: %d\n  errors: %d\n  byteStreamReads: %d\n  blobReads: %d\n",
		self.String(),
		result.startTime.String(),
		result.endTime.String(),
		result.success,
		result.errors,
		result.byteStreamReads,
		result.blobReads,
	)

	stats.PrintTimingStats(elapsedTimes)

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
