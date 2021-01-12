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
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type generateAction struct {
	numRequests int
	minBlobSize int
	maxBlobSize int
	concurrency int
}

func (g *generateAction) String() string {
	return fmt.Sprintf("%d:%d:%d", g.numRequests, g.minBlobSize, g.maxBlobSize)
}

type generateResult struct {
	startTime time.Time
	endTime   time.Time
	success   int
	errors    int
}

type generateWorkItem struct {
	actionContext *ActionContext
	blobSize      int
}

type generateWorkResult struct {
	blobSize int
	err      error
}

func generateWorker(workChan <-chan *generateWorkItem, resultChan chan<- *generateWorkResult) {
	log.Debug("generateWorker started")
	for wi := range workChan {
		resultChan <- processWorkItem(wi)
	}
	log.Debug("generateWorker stopped")
}

func processWorkItem(wi *generateWorkItem) *generateWorkResult {
	buf := make([]byte, wi.blobSize)
	n, err := rand.Read(buf)
	if err != nil {
		log.Errorf("rand failed: %s", err)
		return &generateWorkResult{
			err: err,
		}
	}
	if n != wi.blobSize {
		log.Errorf("rand gave less than expected")
		return &generateWorkResult{
			err: err,
		}
	}
	digest := casutil.ComputeDigest(buf)

	request := remote_pb.BatchUpdateBlobsRequest{
		InstanceName: wi.actionContext.InstanceName,
		Requests: []*remote_pb.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest,
				Data:   buf,
			},
		},
	}

	response, err := wi.actionContext.CasClient.BatchUpdateBlobs(wi.actionContext.Ctx, &request)
	if err == nil {
		s := status.FromProto(response.Responses[0].Status)
		if s.Code() != codes.OK {
			return &generateWorkResult{
				blobSize: wi.blobSize,
				err:      s.Err(),
			}
		}
	} else {
		return &generateWorkResult{
			blobSize: wi.blobSize,
			err:      err,
		}
	}

	missingBlobs, err := casutil.FindMissingBlobs(wi.actionContext.Ctx, wi.actionContext.CasClient, []*remote_pb.Digest{digest},
		wi.actionContext.InstanceName)
	if err != nil {
		return &generateWorkResult{
			blobSize: wi.blobSize,
			err:      err,
		}
	}

	if len(missingBlobs) > 0 {
		return &generateWorkResult{
			blobSize: wi.blobSize,
			err:      fmt.Errorf("Just-written blob is reported as not present in the CAS."),
		}
	}

	wi.actionContext.AddKnownDigest(digest, true)

	return &generateWorkResult{
		blobSize: wi.blobSize,
		err:      nil,
	}
}

func (g *generateAction) RunAction(actionContext *ActionContext) error {
	result := generateResult{
		startTime: time.Now(),
	}

	workChan := make(chan *generateWorkItem)
	resultChan := make(chan *generateWorkResult)

	for c := 0; c < g.concurrency; c++ {
		go generateWorker(workChan, resultChan)
	}

	go func() {
		for i := 0; i < g.numRequests; i++ {
			wi := generateWorkItem{
				actionContext: actionContext,
				blobSize:      rand.Intn(g.maxBlobSize-g.minBlobSize) + g.minBlobSize,
			}
			workChan <- &wi
		}

		close(workChan)
	}()

	for i := 0; i < g.numRequests; i++ {
		r := <-resultChan
		if r.err == nil {
			result.success += 1
		} else {
			result.errors += 1
			log.WithFields(log.Fields{
				"size": r.blobSize,
				"err":  r.err.Error(),
			}).Error("request error")
		}

		if i%10 == 0 {
			log.Infof("progress: %d / %d", i, g.numRequests)
		}
	}

	result.endTime = time.Now()

	close(resultChan)

	fmt.Printf("program: %s\n  startTime: %s\n  endTime: %s\n  success: %d\n  errors: %d\n",
		g.String(),
		result.startTime.String(),
		result.endTime.String(),
		result.success,
		result.errors,
	)

	return nil
}

func ParseGenerateAction(args []string) (Action, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, fmt.Errorf("unable to parse program: expected 3-4 fields, got %d fields", len(args))
	}

	numRequests, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse number of requests: %s: %s", args[0], err)
	}

	minBlobSize, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, fmt.Errorf("unable to parse min blob size: %s: %s", args[1], err)
	}

	maxBlobSize, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, fmt.Errorf("unable to parse max blob size: %s: %s", args[2], err)
	}

	concurrency := 50
	if len(args) >= 4 {
		c, err := strconv.Atoi(args[3])
		if err != nil {
			return nil, fmt.Errorf("unable to parse concurrency: %s: %s", args[3], err)
		}
		concurrency = c
	}

	action := generateAction{
		numRequests: numRequests,
		minBlobSize: minBlobSize,
		maxBlobSize: maxBlobSize,
		concurrency: concurrency,
	}

	return &action, nil
}
