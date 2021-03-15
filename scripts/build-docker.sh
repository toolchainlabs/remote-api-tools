#!/bin/sh

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

docker build . --build-arg APP_NAME=smoketest --tag smoketest
docker build . --build-arg APP_NAME=casload --tag casload

docker run -it --rm smoketest
docker run -it --rm casload
