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
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
	bytestream_pb "google.golang.org/genproto/googleapis/bytestream"
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

func PutBytes(
	ctx context.Context,
	casClient remote_pb.ContentAddressableStorageClient,
	data []byte,
	instanceName string,
) (*remote_pb.Digest, error) {
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

func PutProto(
	ctx context.Context,
	casClient remote_pb.ContentAddressableStorageClient,
	m proto.Message,
	instanceName string,
) (*remote_pb.Digest, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}

	return PutBytes(ctx, casClient, data, instanceName)
}

func GetBytes(
	ctx context.Context,
	casClient remote_pb.ContentAddressableStorageClient,
	digest *remote_pb.Digest,
	instanceName string,
) ([]byte, error) {
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

func FindMissingBlobs(
	ctx context.Context,
	casClient remote_pb.ContentAddressableStorageClient,
	digests []*remote_pb.Digest,
	instanceName string,
) ([]*remote_pb.Digest, error) {
	request := remote_pb.FindMissingBlobsRequest{
		BlobDigests:  digests,
		InstanceName: instanceName,
	}

	response, err := casClient.FindMissingBlobs(ctx, &request)
	if err != nil {
		return nil, err
	}

	return response.MissingBlobDigests, nil
}

func PutBytesStream(
	ctx context.Context,
	client bytestream_pb.ByteStreamClient,
	data []byte,
	chunkSize int64,
	instanceName string,
) (*remote_pb.Digest, error) {
	digest := ComputeDigest(data)

	writeClient, err := client.Write(ctx)
	if err != nil {
		return nil, err
	}

	var writeOffset int64

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	resourceName := fmt.Sprintf(
		"%s/uploads/%s/blobs/%s/%d",
		instanceName,
		id.String(),
		digest.GetHash(),
		digest.GetSizeBytes())
	includeResourceName := true

	for {
		request := bytestream_pb.WriteRequest{
			WriteOffset: writeOffset,
		}

		if includeResourceName {
			request.ResourceName = resourceName
			includeResourceName = false
		}

		writeEndOffset := writeOffset + chunkSize
		if writeEndOffset > int64(len(data)) {
			writeEndOffset = int64(len(data))
			request.FinishWrite = true
		}

		request.Data = data[writeOffset:writeEndOffset]

		log.WithFields(log.Fields{
			"resource_name": request.ResourceName,
			"write_offset":  request.WriteOffset,
			"finish_write":  request.FinishWrite,
			"data_length":   len(request.Data),
		}).Trace("bytestream write request")

		err = writeClient.Send(&request)
		if err != nil {
			return nil, fmt.Errorf("failed to write chunk: %s", err)
		}

		writeOffset += int64(len(request.Data))

		if request.FinishWrite {
			break
		}
	}

	response, err := writeClient.CloseAndRecv()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to write digest: %s", err)
	}

	if response.CommittedSize != int64(len(data)) {
		return nil, fmt.Errorf("failed to write digest: %s", err)
	}

	return digest, nil
}

func GetBytesStream(
	ctx context.Context,
	client bytestream_pb.ByteStreamClient,
	digest *remote_pb.Digest,
	instanceName string,
) ([]byte, error) {
	resourceName := fmt.Sprintf(
		"%s/blobs/%s/%d",
		instanceName,
		digest.GetHash(),
		digest.GetSizeBytes())

	request := bytestream_pb.ReadRequest{
		ResourceName: resourceName,
		ReadOffset:   0,
		ReadLimit:    0,
	}

	readClient, err := client.Read(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("failed to read digest: %s", err)
	}

	data := make([]byte, 0, digest.SizeBytes)

	for {
		response, err := readClient.Recv()
		if err != nil {
			return nil, fmt.Errorf("failed to read digest: %s", err)
		}

		data = append(data, response.Data...)
	}

	return data, nil
}
