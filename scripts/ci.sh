#!/bin/sh

go mod download

echo "Running go vet..."
go vet ./...

echo "Running gofmt ..."
go_files=$(find . -name \*.go -print)
# shellcheck disable=SC2086
unformatted_go_files=$(gofmt -l $go_files)
if [ -n "$unformatted_go_files" ]; then
  echo "ERROR: The following Go files need to be formatted with gofmt:" 1>&2
  for f in $unformatted_go_files ; do
    echo "  $f" 1>&2
  done
  exit 1
fi

echo "Running Go tests ..."
go test -v ./...

