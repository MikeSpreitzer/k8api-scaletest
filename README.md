# k8api-scaletest
Scalability testing of the Kubernetes API machinery

## cmdriver

This is a kube API client that creates and deletes `ConfigMap`
objects.  It makes a simple attempt at a Poisson arrival process, and
a simple attempt at a constant lifetime for each object.

## cm-logger

This is a controller, adapted from
https://github.com/kubernetes/client-go/tree/master/examples/workqueue
, that has an informer on `ConfigMap` objects and a work queue.  This
controller does nothing but log timing information about notifications
from the informer and dequeuing from the work queue.

## flunder-driver

This is a kube API client that creates and deletes `Flunder` objects.
It makes a simple attempt at a Poisson arrival process, and a simple
attempt at a constant lifetime for each object.  Flunders are from
https://github.com/kubernetes/sample-apiserver/tree/master/pkg/apis/wardle
.

## flunder-logger

This is a controller, adapted from
https://github.com/kubernetes/client-go/tree/master/examples/workqueue
, that has an informer on `Flunder` objects and a work queue.  This
controller does nothing but log timing information about notifications
from the informer and dequeuing from the work queue.  Flunders are
from
https://github.com/kubernetes/sample-apiserver/tree/master/pkg/apis/wardle
.

## flunder-watch

This package has a simple executable that loops over time-limited
watches on `Flunder` objects.  This package also has a ReplicaSet of
pods that run an image with that executable.
