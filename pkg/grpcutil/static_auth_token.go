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

package grpcutil

import (
	"context"
	"fmt"

	"google.golang.org/grpc/credentials"
)

type staticAuthToken struct {
	token             string
	allowInsecureAuth bool
}

func (s staticAuthToken) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if !s.allowInsecureAuth {
		if err := credentials.CheckSecurityLevel(ctx, credentials.PrivacyAndIntegrity); err != nil {
			return nil, fmt.Errorf("unable to transfer token PerRPCCredentials: %v", err)
		}
	}
	return map[string]string{
		"authorization": fmt.Sprintf("Bearer %s", s.token),
	}, nil
}

func (s staticAuthToken) RequireTransportSecurity() bool {
	return !s.allowInsecureAuth
}

func NewStaticAuthToken(token string, allowInsecureAuth bool) credentials.PerRPCCredentials {
	return staticAuthToken{token: token, allowInsecureAuth: allowInsecureAuth}
}
