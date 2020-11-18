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

package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math/rand"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/toolchainlabs/remote-api-tools/pkg/casutil"
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
)

type loadProgram struct {
	numRequests int
	minBlobSize int
	maxBlobSize int
	concurrency int
}

type loadResult struct {
	startTime time.Time
	endTime   time.Time
	success   int
	errors    int
}

func (bp *loadProgram) String() string {
	return fmt.Sprintf("%d:%d:%d", bp.numRequests, bp.minBlobSize, bp.maxBlobSize)
}

func parseLoadProgram(program string) (*loadProgram, error) {
	parts := strings.Split(program, ":")
	if len(parts) < 3 || len(parts) > 4 {
		return nil, fmt.Errorf("unable to parse program: expected 3-4 fields, got %d fields", len(parts))
	}

	numRequests, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse number of requests: %s: %s", parts[0], err)
	}

	minBlobSize, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("unable to parse min blob size: %s: %s", parts[1], err)
	}

	maxBlobSize, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("unable to parse max blob size: %s: %s", parts[2], err)
	}

	concurrency := 50
	if len(parts) >= 4 {
		c, err := strconv.Atoi(parts[3])
		if err != nil {
			return nil, fmt.Errorf("unable to parse concurrency: %s: %s", parts[3], err)
		}
		concurrency = c
	}

	p := loadProgram{
		numRequests: numRequests,
		minBlobSize: minBlobSize,
		maxBlobSize: maxBlobSize,
		concurrency: concurrency,
	}

	return &p, nil
}

type workItem struct {
	ctx      context.Context
	clients  *clientsStruct
	blobSize int
}

type workResult struct {
	blobSize int
	err      error
}

func worker(workChan <-chan *workItem, resultChan chan<- *workResult) {
	log.Debug("worker started")
	for wi := range workChan {
		resultChan <- processWorkItem(wi)
	}
	log.Debug("worker stopped")
}

func processWorkItem(wi *workItem) *workResult {
	buf := make([]byte, wi.blobSize)
	n, err := rand.Read(buf)
	if err != nil {
		log.Error("rand failed: %s", err)
		return &workResult{
			err: err,
		}
	}
	if n != wi.blobSize {
		log.Errorf("rand gave less than expected")
		return &workResult{
			err: err,
		}
	}
	digest := casutil.ComputeDigest(buf)

	request := remote_pb.BatchUpdateBlobsRequest{
		InstanceName: wi.clients.instanceName,
		Requests: []*remote_pb.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest,
				Data:   buf,
			},
		},
	}

	response, err := wi.clients.casClient.BatchUpdateBlobs(wi.ctx, &request)
	if err == nil {
		s := status.FromProto(response.Responses[0].Status)
		if s.Code() == codes.OK {
			return &workResult{
				blobSize: wi.blobSize,
				err:      nil,
			}
		} else {
			return &workResult{
				blobSize: wi.blobSize,
				err:      s.Err(),
			}
		}
	} else {
		return &workResult{
			blobSize: wi.blobSize,
			err:      err,
		}
	}
}

func runLoadProgram(ctx context.Context, clients *clientsStruct, p *loadProgram) (*loadResult, error) {
	result := loadResult{
		startTime: time.Now(),
	}

	workChan := make(chan *workItem)
	resultChan := make(chan *workResult)

	for c := 0; c < p.concurrency; c++ {
		go worker(workChan, resultChan)
	}

	go func() {
		for i := 0; i < p.numRequests; i++ {
			wi := workItem{
				ctx:      ctx,
				clients:  clients,
				blobSize: rand.Intn(p.maxBlobSize-p.minBlobSize) + p.minBlobSize,
			}
			workChan <- &wi
		}

		close(workChan)
	}()

	for i := 0; i < p.numRequests; i++ {
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
			log.Infof("progress: %d / %d", i, p.numRequests)
		}
	}

	result.endTime = time.Now()

	close(resultChan)

	return &result, nil
}
