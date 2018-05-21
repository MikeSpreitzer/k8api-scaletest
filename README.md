# k8api-scaletest
Scalability testing of the Kubernetes API machinery

## The Pieces

### cmdriver

This is a kube API client that creates and deletes `ConfigMap`
objects.  It makes a simple attempt at a Poisson arrival process, and
a simple attempt at a constant lifetime for each object.

### cmdriverclosed

Another client that thrashes `ConfigMap objects`.  This one uses a
closed-loop structure and includes one update in each object's
lifecycle.

### cm-logger

This is a controller, adapted from
https://github.com/kubernetes/client-go/tree/master/examples/workqueue
, that has an informer on `ConfigMap` objects and a work queue.  This
controller does nothing but log timing information about notifications
from the informer and dequeuing from the work queue.

### flunder-driver

This is a kube API client that creates and deletes `Flunder` objects.
It makes a simple attempt at a Poisson arrival process, and a simple
attempt at a constant lifetime for each object.  Flunders are from
https://github.com/kubernetes/sample-apiserver/tree/master/pkg/apis/wardle
.

### flunder-logger

This is a controller, adapted from
https://github.com/kubernetes/client-go/tree/master/examples/workqueue
, that has an informer on `Flunder` objects and a work queue.  This
controller does nothing but log timing information about notifications
from the informer and dequeuing from the work queue.  Flunders are
from
https://github.com/kubernetes/sample-apiserver/tree/master/pkg/apis/wardle
.

### flunder-watch

This package has a simple executable that loops over time-limited
watches on `Flunder` objects.  This package also has a ReplicaSet of
pods that run an image with that executable.

### go_httperf

This is a cousin of [iperf](https://en.wikipedia.org/wiki/Iperf) based
on Go and HTTP/2.

It is always a server, so that you can fetch its
[`/debug/pprof`](https://golang.org/pkg/net/http/pprof/) (see also
https://tip.golang.org/doc/diagnostics.html#profiling).

Its performance-oriented URL path is `/blocks`, and it requires three
query parameters:

- `size`, a positive integer number of bytes;
- `n`, a positive integer number of blocks to return; and
- `periodMS`, a positive integer number of milliseconds.

GETting such a URL will return the requested number of blocks of
bytes, of the requested size each.  The server will attempt to send 1
block every `periodMS` milliseconds.

If given a `-base-url` on the command line, this program will also act
as a client.  In its client role it takes request parameters plus also
`-threads` and `-connections`.  Each thread repeatedly makes a request
on the server.  Those threads are load-balanced across the given
number of TLS connections to the server.

In both its roles as client and server, `go_httperf` logs the
[TLS keys](https://developer.mozilla.org/en-US/docs/Mozilla/Projects/NSS/Key_Log_Format)
that it uses.

When given `-v=2`, `go_httperf` logs its transmit and receive rates.
Higher values of `-v` produce more detailed logging.


## Building

Each piece is built on its own.  There are two sorts of pieces here:
those that include support for containerization and those that are
simple processes.

For a simple process, just `cd` and `go build`.

For a piece that supports containerization, there are three stages and
a `Makefile` that handles the first two.  To build the executable for
your development machine, just `cd` and `go build`.  The `make` or
`make build` command will build the executable for use in a container.
Use `make publish` to create a Docker image and push it somewhere.  By
default that somewhere is `$LOGNAME` at Dockerhub; you can `make
publish DOCKERPFX=$reg/$ns` to push to `$reg/$ns` instead.  Finally,
there is a YAML file that describes a Kubernetes ReplicaSet that runs
the image that I built (you may want to edit it to run the image that
you build).  You will also need to `kubectl create namespace
scaletest` and `kubectl create -f authz.yaml` before creating the
ReplicaSet.

### Dependencies

The dependencies are kept in `vendor` and managed by Glide.  Use the
`-v` switch on `glide update` and `glide install`.
