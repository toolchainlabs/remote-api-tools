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
	"context"
	"fmt"
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
	bytestream_pb "google.golang.org/genproto/googleapis/bytestream"
	"strconv"
	"strings"
	"sync"
)

type ActionContext struct {
	// Go context for the current operation.
	Ctx context.Context

	// Clients to use to access the CAS.
	CasClient        remote_pb.ContentAddressableStorageClient
	BytestreamClient bytestream_pb.ByteStreamClient
	InstanceName     string
	MaxBatchBlobSize int64
	WriteChunkSize   int64

	// Map of a digest in string form to a bool representing whether it is known to be present or missing
	// in the CAS.
	KnownDigests map[string]bool
	mu           sync.Mutex
}

func (ac *ActionContext) AddKnownDigest(digest *remote_pb.Digest, present bool) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	ac.KnownDigests[fmt.Sprintf("%s-%d", digest.Hash, digest.SizeBytes)] = present
}

func (ac *ActionContext) GetKnownDigests(presence bool) []*remote_pb.Digest {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	result := []*remote_pb.Digest{}

	for digestKey, present := range ac.KnownDigests {
		if present != presence {
			continue
		}

		parts := strings.Split(digestKey, "-")
		if len(parts) != 2 {
			panic("illegal state: known digests key was not parsed correctly")
		}

		sizeBytes, err := strconv.Atoi(parts[1])
		if err != nil {
			panic("illegal state: known digests key was not parsed correctly")
		}

		digest := remote_pb.Digest{
			Hash:      parts[0],
			SizeBytes: int64(sizeBytes),
		}

		result = append(result, &digest)
	}

	return result
}

type Action interface {
	RunAction(actionContext *ActionContext) error
}

func ParseAction(actionStr string) (Action, error) {
	parts := strings.Split(actionStr, ":")
	if len(parts) < 1 {
		return nil, fmt.Errorf("unable to parse load action: %s", actionStr)
	}

	switch parts[0] {
	case "generate":
		return ParseGenerateAction(parts[1:])
	case "read":
		return ParseReadAction(parts[1:])
	case "load-digests":
		return ParseLoadDigestsAction(parts[1:])
	case "save-digests":
		return ParseSaveDigestsAction(parts[1:])
	default:
		return nil, fmt.Errorf("unknown load action name: %s", parts[0])
	}
}
