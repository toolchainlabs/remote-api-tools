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
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

func main() {
	rand.Seed(time.Now().Unix())

	remoteOpt := pflag.StringP("remote", "r", "", "remote server")
	authTokenFileOpt := pflag.StringP("auth-token-file", "a", "", "auth bearer token to use")
	secureOpt := pflag.BoolP("secure", "s", false, "enable secure mode (TLS)")
	instanceNameOpt := pflag.StringP("instance-name", "i", "", "instance name")
	verboseOpt := pflag.CountP("verbose", "v", "increase logging verbosity")
	useJsonLoggingOpt := pflag.Bool("log-json", false, "log using JSON")
	allowInsecureAuthOpt := pflag.Bool("allow-insecure-auth", false, "allow credentials to be passed unencrypted (i.e., no TLS)")

	pflag.Parse()

	if *useJsonLoggingOpt {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		log.SetFormatter(&log.TextFormatter{
			DisableColors: true,
			FullTimestamp: true,
		})
	}

	if *remoteOpt == "" {
		log.Fatal("--remote option is required")
	}

	args := pflag.Args()
	if len(args) == 0 {
		log.Fatal("benchmark programs must be specified")
	}

	authToken := ""
	if *authTokenFileOpt != "" {
		authTokenBytes, err := ioutil.ReadFile(*authTokenFileOpt)
		if err != nil {
			log.Fatalf("unable to read auth token from %s: %s", *authTokenFileOpt, err)
		}
		authToken = strings.TrimSpace(string(authTokenBytes))
	}

	if *verboseOpt > 1 {
		log.SetLevel(log.TraceLevel)
	} else if *verboseOpt == 1 {
		log.SetLevel(log.DebugLevel)
	}

	var bps []*loadProgram
	for _, arg := range args {
		bp, err := parseLoadProgram(arg)
		if err != nil {
			log.Fatalf("error parsing program: %s", err)
		}
		bps = append(bps, bp)
	}

	ctx := context.Background()

	cs, err := setupClients(ctx, *remoteOpt, *instanceNameOpt, *secureOpt, *allowInsecureAuthOpt, authToken)
	if err != nil {
		log.Fatalf("failed to setup connection: %s", err)
	}
	defer cs.Close()

	for _, bp := range bps {
		result, err := runLoadProgram(ctx, cs, bp)
		if err != nil {
			log.Fatalf("error during benchmark: %s", err)
		}

		fmt.Printf("program: %s\n  startTime: %s\n  endTime: %s\n  success: %d\n  errors: %d\n",
			bp.String(),
			result.startTime.String(),
			result.endTime.String(),
			result.success,
			result.errors,
		)
	}
}
