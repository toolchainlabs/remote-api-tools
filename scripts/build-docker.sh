#!/bin/sh

cd "$(git rev-parse --show-toplevel)"

docker build . --build-arg APP_NAME=smoketest --tag smoketest
docker build . --build-arg APP_NAME=casload --tag casload

# sanity check, run apps in containers (they will show help)
docker run -it --rm smoketest
docker run -it --rm casload
