# k8api-scaletest
Scalability testing of the Kubernetes API machinery

## The Pieces

### cmdriver

This is a kube API client that creates and deletes `ConfigMap`
objects.  It makes a simple attempt at a Poisson arrival process, and
a simple attempt at a constant lifetime for each object.

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

## Building

Each piece is built on its own.  There are two sorts of pieces here:
those that include support for containerization and those that are
simple processes.

For a simple process, just `cd` and `go build`.

For a piece that supports containerization, there are three stages and
a `Makefile` that handles the first two.  The `make` or `make build`
command will build the executable.  Use `make publish` to create a
Docker image and push it somewhere.  By default that is `$LOGNAME` at
Dockerhub; you can `make publish DOCKERPFX=$reg/$ns` to push to
`$reg/$ns` instead.  Finally, there is a YAML file that describes a
Kubernetes ReplicaSet that runs the image that I built (you may want
to edit it to run the image that you build).

### The Vendor Gotcha

`glide update` or `glide install` will create
`vendor/k8s.io/sample-apiserver/vendor` --- which will, in turn, cause
compilation errors.  Delete `vendor/k8s.io/sample-apiserver/vendor`
before building.  There must be a better way!
