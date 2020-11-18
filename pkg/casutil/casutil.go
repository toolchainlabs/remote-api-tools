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

package casutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/golang/protobuf/proto"
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ComputeDigest(data []byte) *remote_pb.Digest {
	h := sha256.Sum256(data)
	digest := hex.EncodeToString(h[:])

	return &remote_pb.Digest{
		Hash:      digest,
		SizeBytes: int64(len(data)),
	}
}

func PutBytes(ctx context.Context, casClient remote_pb.ContentAddressableStorageClient, data []byte, instanceName string) (*remote_pb.Digest, error) {
	digest := ComputeDigest(data)

	req1 := remote_pb.FindMissingBlobsRequest{
		BlobDigests:  []*remote_pb.Digest{digest},
		InstanceName: instanceName,
	}

	resp1, err := casClient.FindMissingBlobs(ctx, &req1)
	if err != nil {
		return nil, fmt.Errorf("FindMissingBlobs: %s", err)
	}

	if len(resp1.MissingBlobDigests) > 0 {
		req2 := remote_pb.BatchUpdateBlobsRequest{
			InstanceName: instanceName,
			Requests: []*remote_pb.BatchUpdateBlobsRequest_Request{
				{
					Digest: digest,
					Data:   data,
				},
			},
		}

		resp2, err := casClient.BatchUpdateBlobs(context.Background(), &req2)
		if err != nil {
			return nil, fmt.Errorf("BatchUpdateBlobs: %s", err)
		}

		if len(resp2.Responses) != 1 {
			return nil, fmt.Errorf("illegal state: bad BatchUpdateBlobs response")
		}

		s := status.FromProto(resp2.Responses[0].Status)
		if s.Code() != codes.OK {
			return nil, fmt.Errorf("failed to store to CAS: %s", s.Err())
		}
	}

	return digest, nil
}

func PutProto(ctx context.Context, casClient remote_pb.ContentAddressableStorageClient, m proto.Message, instanceName string) (*remote_pb.Digest, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}

	return PutBytes(ctx, casClient, data, instanceName)
}

func GetBytes(ctx context.Context, casClient remote_pb.ContentAddressableStorageClient, digest *remote_pb.Digest, instanceName string) ([]byte, error) {
	req := remote_pb.BatchReadBlobsRequest{
		InstanceName: instanceName,
		Digests:      []*remote_pb.Digest{digest},
	}

	resp, err := casClient.BatchReadBlobs(ctx, &req)
	if err != nil {
		return nil, err
	}

	return resp.Responses[0].Data, nil
}

func GetProto(
	ctx context.Context,
	casClient remote_pb.ContentAddressableStorageClient,
	digest *remote_pb.Digest,
	instanceName string,
	m proto.Message,
) error {
	data, err := GetBytes(ctx, casClient, digest, instanceName)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(data, m)
	if err != nil {
		return err
	}

	return nil
}
