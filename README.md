# Tooling for Remote Execution API

This project contains miscellaneous tooling useful for dealing with
[Remote Execution API](https://github.com/bazelbuild/remote-apis/blob/master/build/bazel/remote/execution/v2/remote_execution.proto) servers.
Such servers include [BuildBarn](https://github.com/buildbarn/bb-remote-execution),
[Buildfarm](https://github.com/bazelbuild/bazel-buildfarm/), and
[BuildGrid](https://gitlab.com/BuildGrid/buildgrid).

## Tools

### REAPI Smoke Test

The REAPAI "smoke test" is a simple operational check of an REAPI instance. It checks whether an REAPI server
can handle basic CAS, Action Cache, and Execution service requests. The test creates a unique Command proto,
requests its execution, and then checks the outputs and the Action Cache to ensure they are in the expected state.

To build the `smoketest` binary:

```
$ cd cmd/smoketest
$ go build
```

If you are running an REAPI instance at 127.0.0.1:8980, then you can run the smoketest against
that instance with the `OSFamily` platform property set to `Linux` by running:

```
$ ./smoketest -r 127.0.0.1:8980 -p OSFamily=Linux 
```

There are other options to enable TLS, send an authorization token, add additional platform properties, etc.
Run `smoketest -h` to see the available options.

### Load Testing

The `casload` tool generates CAS traffic according to one or more specifications of the form:
NUM_REQUESTS:MIN_BLOB_SIZE:MAX_BLOB_SIZE[:CONCURRENCY].

To build the `casload` binary:

```
$ cd cmd/smoketest
$ go build
```

If you are running a CAS instance at 127.0.0.1:8980, then you can generate 15,000 requests with blob sizes ranging
from 5000 bytes to 10,000 bytes with 100 concurrent requests by running:

```
$ ./casload -r 127.0.0.1:8980 15000:5000:10000:100
```

There are other options to enable TLS, send an authorization token, etc.
Run `casload -h` to see the available options.

## Development

To rebuild the generated Go files, run:

```
$ ./gen-proto.sh
```

## Contributions

Contributions are welcome. Please open a pull request or file an issue.

Please run `go fmt ./...` to format the code before submitting.

## Useful Links

- [remote-apis-testing project](https://gitlab.com/remote-apis-testing/remote-apis-testing)
