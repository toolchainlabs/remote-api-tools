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
	"crypto/tls"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/toolchainlabs/remote-api-tools/pkg/grpcutil"
	remote_pb "github.com/toolchainlabs/remote-api-tools/protos/build/bazel/remote/execution/v2"
	bytestream_pb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type clientsStruct struct {
	conn               *grpc.ClientConn
	instanceName       string
	casClient          remote_pb.ContentAddressableStorageClient
	bytestreamClient   bytestream_pb.ByteStreamClient
	capabilitiesClient remote_pb.CapabilitiesClient
	maxBatchBlobSize   int64
}

func (s *clientsStruct) Close() error {
	return s.conn.Close()
}

func setupClients(
	ctx context.Context,
	server, instanceName string,
	secure, allowInsecureAuth bool,
	authToken string,
) (*clientsStruct, error) {
	dialOptions := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(1) * time.Second,
			Timeout:             time.Duration(5) * time.Second,
			PermitWithoutStream: true,
		}),
	}

	if secure {
		tlsConfig := &tls.Config{}
		creds := credentials.NewTLS(tlsConfig)
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	if authToken != "" {
		log.Info("enabling auth token")
		creds := grpcutil.NewStaticAuthToken(authToken, allowInsecureAuth)
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(creds))
	}

	conn, err := grpc.DialContext(ctx, server, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("dial error: %s", err)
	}

	casClient := remote_pb.NewContentAddressableStorageClient(conn)
	byteStreamClient := bytestream_pb.NewByteStreamClient(conn)
	capabilitiesClient := remote_pb.NewCapabilitiesClient(conn)

	capabilitiesRequest := remote_pb.GetCapabilitiesRequest{
		InstanceName: instanceName,
	}
	capabilities, err := capabilitiesClient.GetCapabilities(ctx, &capabilitiesRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve capabilities: %s", err)
	}

	cs := clientsStruct{
		conn:               conn,
		instanceName:       instanceName,
		casClient:          casClient,
		bytestreamClient:   byteStreamClient,
		capabilitiesClient: capabilitiesClient,
		maxBatchBlobSize:   capabilities.GetCacheCapabilities().MaxBatchTotalSizeBytes,
	}

	return &cs, nil
}
