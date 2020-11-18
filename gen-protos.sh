#!/usr/bin/env bash

protoc \
  -Iprotos/ \
  --go_out=,plugins=grpc:protos/ \
  --go_opt=Mbuild/bazel/semver/semver.proto=github.com/toolchainlabs/remote-api-tools/protos/build/bazel/semver \
  protos/build/bazel/semver/*.proto \
  protos/build/bazel/remote/execution/v2/*.proto
