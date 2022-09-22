#!/usr/bin/env bash

if [ ! -d protos ]; then
  echo "ERROR: This script must be run from the root of the repository." 1>&2
  exit 1
fi

protoc \
  -Iprotos/ \
  --go_out=protos/ \
  --go-grpc_out=protos/ \
  --go_opt=Mbuild/bazel/semver/semver.proto=github.com/toolchainlabs/remote-api-tools/protos/build/bazel/semver \
  protos/build/bazel/semver/*.proto \
  protos/build/bazel/remote/execution/v2/*.proto
