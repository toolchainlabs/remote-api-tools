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
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	remote_pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/toolchainlabs/remote-api-tools/pkg/casutil"
	"github.com/toolchainlabs/remote-api-tools/pkg/grpcutil"
	longrunning_pb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

func makeCommand(
	now string,
	delaySecs uint,
	platformProperties []string,
	envVars map[string]string,
) *remote_pb.Command {
	delayCmd := ""
	if delaySecs > 0 {
		delayCmd = fmt.Sprintf("&& sleep %d", delaySecs)
	}

	var protoPlatformProperties []*remote_pb.Platform_Property
	for _, p := range platformProperties {
		parts := strings.SplitN(p, "=", 2)
		protoPlatformProperties = append(protoPlatformProperties, &remote_pb.Platform_Property{
			Name:  parts[0],
			Value: parts[1],
		})
	}

	sort.Slice(protoPlatformProperties, func(i, j int) bool {
		left := protoPlatformProperties[i]
		right := protoPlatformProperties[j]

		cmp1 := strings.Compare(left.Name, right.Name)
		if cmp1 < 0 {
			return true
		} else if cmp1 > 0 {
			return false
		}

		cmp2 := strings.Compare(left.Value, right.Value)
		return cmp2 < 0
	})

	envVars["NOW"] = string(now)
	var protoEnvironmentVariables []*remote_pb.Command_EnvironmentVariable
	for key, value := range envVars {
		protoEnvironmentVariables = append(protoEnvironmentVariables, &remote_pb.Command_EnvironmentVariable{
			Name:  key,
			Value: value,
		})
	}

	sort.Slice(protoEnvironmentVariables, func(i, j int) bool {
		left := protoEnvironmentVariables[i]
		right := protoEnvironmentVariables[j]

		cmp1 := strings.Compare(left.Name, right.Name)
		if cmp1 < 0 {
			return true
		} else if cmp1 > 0 {
			return false
		}

		cmp2 := strings.Compare(left.Value, right.Value)
		return cmp2 < 0
	})

	shellCommand := fmt.Sprintf("echo $NOW > foobar && mkdir -p xyzzy && echo $NOW > xyzzy/result %s", delayCmd)
	command := &remote_pb.Command{
		Arguments:            []string{"/bin/sh", "-c", shellCommand},
		OutputFiles:          []string{"foobar"},
		OutputDirectories:    []string{"xyzzy"},
		EnvironmentVariables: protoEnvironmentVariables,
		Platform: &remote_pb.Platform{
			Properties: protoPlatformProperties,
		},
	}

	return command
}

func storeSimpleDirectory(ctx context.Context, casClient remote_pb.ContentAddressableStorageClient, instanceName string) (*remote_pb.Digest, error) {
	content := []byte("There is nothing to see here. Please move on.")
	contentDigest, err := casutil.PutBytes(ctx, casClient, content, instanceName)
	if err != nil {
		return nil, err
	}

	directory := &remote_pb.Directory{
		Files: []*remote_pb.FileNode{
			{
				Name:   "just-a-file",
				Digest: contentDigest,
			},
		},
	}

	return casutil.PutProto(ctx, casClient, directory, instanceName)
}

func getActionResult(ctx context.Context, actionCacheClient remote_pb.ActionCacheClient, actionDigest *remote_pb.Digest, instanceName string) (*remote_pb.ActionResult, error) {
	req := remote_pb.GetActionResultRequest{
		InstanceName: instanceName,
		ActionDigest: actionDigest,
	}

	actionResult, err := actionCacheClient.GetActionResult(ctx, &req)
	if err != nil {
		if s, ok := status.FromError(err); ok {
			if s.Code() == codes.NotFound {
				return nil, nil
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return actionResult, nil
}

func verifyOutputFiles(
	ctx context.Context,
	actionResult *remote_pb.ActionResult,
	now string,
	casClient remote_pb.ContentAddressableStorageClient,
	instanceName string,
) error {
	if len(actionResult.OutputFiles) != 1 {
		return fmt.Errorf("action result should have one output file, got %d", len(actionResult.OutputFiles))
	}

	outputFile := actionResult.OutputFiles[0]

	contentBytes, err := casutil.GetBytes(ctx, casClient, outputFile.Digest, instanceName)
	if err != nil {
		return err
	}

	if string(contentBytes) == now+"\n" {
		return nil
	} else {
		return fmt.Errorf("content from execution does not match, expected: %s, got: %s", now, string(contentBytes))
	}
}

func verifyOutputDirectories(
	ctx context.Context,
	actionResult *remote_pb.ActionResult,
	now string,
	casClient remote_pb.ContentAddressableStorageClient,
	instanceName string,
) error {
	if len(actionResult.OutputDirectories) != 1 {
		return fmt.Errorf("action result should have one output directory, got %d", len(actionResult.OutputFiles))
	}

	outputDirectory := actionResult.OutputDirectories[0]

	var tree remote_pb.Tree
	err := casutil.GetProto(ctx, casClient, outputDirectory.TreeDigest, instanceName, &tree)
	if err != nil {
		return err
	}

	if len(tree.Children) != 0 {
		return fmt.Errorf("output directory should not have subdirectories, got %d subdirs", len(tree.Children))
	}

	if len(tree.Root.Directories) != 0 {
		return fmt.Errorf("output directory root should not have subdirectories, got %d subdirs", len(tree.Root.Directories))
	}

	if len(tree.Root.Files) != 1 {
		return fmt.Errorf("output directory should have one file, got %d files", len(tree.Root.Files))
	}

	contentBytes, err := casutil.GetBytes(ctx, casClient, tree.Root.Files[0].Digest, instanceName)
	if err != nil {
		return err
	}

	if string(contentBytes) == now+"\n" {
		return nil
	} else {
		return fmt.Errorf("content from execution does not match, expected: %s, got: %s", now, string(contentBytes))
	}
}

func verifyActionResult(
	ctx context.Context,
	actionResult *remote_pb.ActionResult,
	now string,
	casClient remote_pb.ContentAddressableStorageClient,
	instanceName string,
) error {
	if actionResult.StdoutDigest != nil && log.IsLevelEnabled(log.DebugLevel) {
		stdoutBytes, err := casutil.GetBytes(ctx, casClient, actionResult.StdoutDigest, instanceName)
		if err != nil {
			return err
		}
		log.WithField("content", string(stdoutBytes)).Debug("stdout")
	}

	if actionResult.StderrDigest != nil && log.IsLevelEnabled(log.DebugLevel) {
		stderrBytes, err := casutil.GetBytes(ctx, casClient, actionResult.StdoutDigest, instanceName)
		if err != nil {
			return err
		}
		log.WithField("content", string(stderrBytes)).Debug("stderr")
	}

	if actionResult.ExitCode != 0 {
		return fmt.Errorf("exit code was non-zero (code=%d)", actionResult.ExitCode)
	}

	err := verifyOutputFiles(ctx, actionResult, now, casClient, instanceName)
	if err != nil {
		return err
	}

	err = verifyOutputDirectories(ctx, actionResult, now, casClient, instanceName)
	if err != nil {
		return err
	}

	return nil
}

type clientsStruct struct {
	conn1             *grpc.ClientConn
	conn2             *grpc.ClientConn
	casClient         remote_pb.ContentAddressableStorageClient
	reClient          remote_pb.ExecutionClient
	actionCacheClient remote_pb.ActionCacheClient
}

func (s *clientsStruct) Close() error {
	err1 := s.conn1.Close()
	if s.conn2 != s.conn1 {
		err2 := s.conn2.Close()
		if err2 != nil {
			return err2
		}
	}
	return err1
}

func setupClients(
	ctx context.Context,
	casServer string,
	executionServer string,
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
		creds := grpcutil.NewStaticAuthToken(authToken, allowInsecureAuth)
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(creds))
	}

	conn1, err := grpc.DialContext(ctx, casServer, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("dial error: %s", err)
	}

	var conn2 *grpc.ClientConn
	if executionServer != casServer {
		conn2, err = grpc.DialContext(ctx, executionServer, dialOptions...)
		if err != nil {
			return nil, fmt.Errorf("dial error: %s", err)
		}
	} else {
		conn2 = conn1
	}

	casClient := remote_pb.NewContentAddressableStorageClient(conn1)
	actionCacheClient := remote_pb.NewActionCacheClient(conn1)
	reClient := remote_pb.NewExecutionClient(conn2)

	cs := clientsStruct{
		conn1:             conn1,
		conn2:             conn2,
		casClient:         casClient,
		reClient:          reClient,
		actionCacheClient: actionCacheClient,
	}

	return &cs, nil
}

func digestToString(digest *remote_pb.Digest) string {
	return fmt.Sprintf("%s/%d", digest.GetHash(), digest.GetSizeBytes())
}

func shouldRetryError(err error) bool {
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Aborted, codes.Internal, codes.ResourceExhausted, codes.Unavailable, codes.Unknown:
			return true

		default:
			return false
		}
	} else {
		return false
	}
}

func smokeTest(
	ctx context.Context,
	cs *clientsStruct,
	instanceName string,
	platformProperties []string,
	delaySecs uint,
	extraEnvVars map[string]string,
	logFields log.Fields,
) error {
	nowBytes, _ := time.Now().MarshalText()
	now := string(nowBytes)
	command := makeCommand(now, delaySecs, platformProperties, extraEnvVars)
	log.WithFields(logFields).WithField("command", command.String()).Debug("constructed command")

	log.WithFields(logFields).Info("storing command in CAS")
	commandDigest, err := casutil.PutProto(ctx, cs.casClient, command, instanceName)
	if err != nil {
		return fmt.Errorf("failed to store command: %s", err)
	}

	log.WithFields(logFields).Info("storing input root in CAS")
	inputRootDigest, err := storeSimpleDirectory(ctx, cs.casClient, instanceName)
	if err != nil {
		return fmt.Errorf("failed to store input root: %s", err)
	}

	log.WithFields(logFields).Info("storing action in CAS")
	action := &remote_pb.Action{
		CommandDigest:   commandDigest,
		InputRootDigest: inputRootDigest,
	}
	actionDigest, err := casutil.PutProto(ctx, cs.casClient, action, instanceName)
	if err != nil {
		return fmt.Errorf("failed to store action: %s", err)
	}
	log.WithFields(logFields).WithFields(log.Fields{
		"command_digest":    digestToString(commandDigest),
		"input_root_digest": digestToString(inputRootDigest),
		"action_digest":     digestToString(actionDigest),
	}).Debug("digests")

	// Check Action Cache to make sure the action was not run. It should not have been run since we insert
	// an environment variable with the current timestamp to make action unique.
	log.WithFields(logFields).Info("checking Action Cache to ensure action is not present (as expected)")
	actionResult, err := getActionResult(ctx, cs.actionCacheClient, actionDigest, instanceName)
	if err != nil {
		return fmt.Errorf("getActionResult: %s", err)
	}
	if actionResult != nil {
		return fmt.Errorf("illegal state: the action which is unique had an action result")
	}

	var operationName string

OUTER:
	for {
		var recvOperation func() (*longrunning_pb.Operation, error)

		if operationName == "" {
			// No operation has been submitted. Submit the action for execution.
			log.WithFields(logFields).Info("submitting action for execution")
			executeRequest := &remote_pb.ExecuteRequest{
				InstanceName: instanceName,
				ActionDigest: actionDigest,
			}
			client, err := cs.reClient.Execute(ctx, executeRequest)
			if err != nil {
				return fmt.Errorf("execute error: %s", err)
			}
			err = client.CloseSend()
			if err != nil {
				return fmt.Errorf("CloseSend error: %s", err)
			}
			recvOperation = func() (*longrunning_pb.Operation, error) {
				return client.Recv()
			}
		} else {
			// The operation has already been submitted. Reconnect to the operation stream.
			log.WithFields(logFields).Info("reconnecting to action's operation stream")
			waitExecutionRequest := &remote_pb.WaitExecutionRequest{
				Name: operationName,
			}
			client, err := cs.reClient.WaitExecution(ctx, waitExecutionRequest)
			if err != nil {
				return fmt.Errorf("wait execution error: %s", err)
			}
			err = client.CloseSend()
			if err != nil {
				return fmt.Errorf("CloseSend error: %s", err)
			}
			recvOperation = func() (*longrunning_pb.Operation, error) {
				return client.Recv()
			}
		}

		// Loop over the operation status returned by the stream.
		for {
			operation, err := recvOperation()
			if err != nil {
				if shouldRetryError(err) {
					continue OUTER
				}
				return fmt.Errorf("stream recv: %s", err)
			}
			operationName = operation.Name

			if s := operation.GetError(); s != nil {
				s2 := status.FromProto(s)
				return fmt.Errorf("execution failed: %s", s2.Err())
			}

			var metadata remote_pb.ExecuteOperationMetadata
			rawMetadata := operation.GetMetadata()
			err = proto.Unmarshal(rawMetadata.Value, &metadata)
			if err != nil {
				return fmt.Errorf("unable to parse metadata: %s", err)
			}
			log.WithFields(logFields).WithFields(log.Fields{
				"stage": metadata.Stage.String(),
			}).Debugf("metadata")

			if operation.Done {
				if s := operation.GetError(); s != nil {
					s2 := status.FromProto(s)
					return fmt.Errorf("execution failed: %s", s2.Err())
				} else if r := operation.GetResponse(); r != nil {
					if r.TypeUrl != "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse" {
						return fmt.Errorf("illegal state: expected execute response to contain %s, got %s",
							"type.googleapis.com/build.bazel.remote.execution.v2.ExecuteRespons", r.TypeUrl)
					}

					var executeResponse remote_pb.ExecuteResponse
					err = proto.Unmarshal(r.Value, &executeResponse)
					if err != nil {
						return fmt.Errorf("failed to decode execute response: %s", err)
					}
					log.WithFields(logFields).Infof("got action result: %s", executeResponse.Result)

					err = verifyActionResult(ctx, executeResponse.Result, now, cs.casClient, instanceName)
					if err != nil {
						return err
					}
					log.WithFields(logFields).Infof("verified output from execution is as expected")
				} else {
					return fmt.Errorf("unable to decode execution response")
				}

				// Exit the outer loop since execution was successful.
				break OUTER
			}

			log.WithFields(logFields).Debugf("operation: %s", operation)
		}
	}

	// Retrieve the action from the Action Cache. Given it just ran, this should succeed.
	log.WithFields(logFields).Info("checking Action Cache to ensure action is present")
	foundActionResult := false
	for retry := 1; retry <= 5; retry++ {
		actionResult, err = getActionResult(ctx, cs.actionCacheClient, actionDigest, instanceName)
		if err != nil {
			return fmt.Errorf("getActionResult: %s", err)
		}
		if actionResult == nil {
			log.WithFields(logFields).Warn("action result not in Action Cache yet, retrying...")
			time.Sleep(time.Duration(500) * time.Millisecond)
			continue
		} else {
			foundActionResult = true
			break
		}
	}

	if !foundActionResult {
		return fmt.Errorf("execution was successful but action cache does not have action result")
	}

	return nil
}

func parallelSmokeTest(
	ctx context.Context,
	cs *clientsStruct,
	instanceName string,
	platformProperties []string,
	count int,
	delaySecs uint,
) error {
	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()
			log.Infof("spawning request %d", i)

			err := smokeTest(ctx, cs, instanceName, platformProperties, delaySecs, map[string]string{
				"COUNTER": strconv.Itoa(i),
			}, log.Fields{
				"i": i,
			})

			if err != nil {
				log.WithField("i", i).Errorf("request %d failed: %s", i, err)
			} else {
				log.WithField("i", i).Infof("request %d success", i)
			}
		}(i, &wg)
	}

	wg.Wait()

	return nil
}

func main() {
	remoteOpt := pflag.StringP("remote", "r", "", "remote server")
	executionServerOpt := pflag.String("execution-server", "", "execution server")
	casServerOpt := pflag.String("cas-server", "", "CAS server")
	authTokenFileOpt := pflag.StringP("auth-token-file", "a", "", "auth bearer token to use")
	secureOpt := pflag.BoolP("secure", "s", false, "enable secure mode (TLS)")
	instanceNameOpt := pflag.StringP("instance-name", "i", "", "instance name")
	timeoutSecsOpt := pflag.UintP("timeout-secs", "t", 0, "timeout in seconds")
	delaySecsOpt := pflag.UintP("delay", "d", 0, "delay on remote execution")
	verboseOpt := pflag.CountP("verbose", "v", "increase logging verbosity")
	useJsonLoggingOpt := pflag.Bool("log-json", false, "log using JSON")
	platformPropertiesOpt := pflag.StringSliceP("platform", "p", nil, "add platform property KEY=VALUE")
	allowInsecureAuthOpt := pflag.Bool("allow-insecure-auth", false, "allow credentials to be passed unencrypted (i.e., no TLS)")
	parallelModeOpt := pflag.Int("parallel", 1, "enable parallel mode")

	pflag.Parse()

	if *useJsonLoggingOpt {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		log.SetFormatter(&log.TextFormatter{
			DisableColors: true,
			FullTimestamp: true,
		})
	}

	var executionServer string
	var casServer string

	if *remoteOpt != "" {
		if *executionServerOpt != "" || *casServerOpt != "" {
			log.Fatal("--remote is mutually exclusive with --execution-server / --cas-server")
		}
		executionServer = *remoteOpt
		casServer = *remoteOpt
	} else if *executionServerOpt != "" || *casServerOpt != "" {
		if *executionServerOpt == "" {
			log.Fatal("--execution-server must be specified when --cas-server is specified")
		}
		if *casServerOpt == "" {
			log.Fatal("--cas-server must be specified when --execution-server is specified")
		}

		executionServer = *executionServerOpt
		casServer = *casServerOpt
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

	ctx := context.Background()
	if *timeoutSecsOpt != 0 {
		duration := time.Duration(*timeoutSecsOpt) * time.Second
		ctx2, cancel := context.WithTimeout(ctx, duration)
		ctx = ctx2
		defer cancel()
	}

	cs, err := setupClients(ctx, casServer, executionServer, *secureOpt, *allowInsecureAuthOpt, authToken)
	if err != nil {
		log.Fatalf("failed to setup connection: %s", err)
	}
	defer cs.Close()

	if *parallelModeOpt < 2 {
		err = smokeTest(ctx, cs, *instanceNameOpt, *platformPropertiesOpt, *delaySecsOpt, map[string]string{}, map[string]interface{}{})
		if err != nil {
			log.Fatalf("smoke test failed: %s", err)
		}
	} else {
		err = parallelSmokeTest(ctx, cs, *instanceNameOpt, *platformPropertiesOpt, *parallelModeOpt, *delaySecsOpt)
		if err != nil {
			log.Fatalf("parallel smoke test failed: %s", err)
		}
	}

	log.Info("smoke test succeeded")
}
