/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	wardleclient "k8s.io/sample-apiserver/pkg/client/clientset/versioned"
	wardleinformers "k8s.io/sample-apiserver/pkg/client/informers/externalversions"
	wardlev1a1listers "k8s.io/sample-apiserver/pkg/client/listers/wardle/v1alpha1"
)

type Controller struct {
	queue    workqueue.RateLimitingInterface
	informer cache.Controller
	lister   wardlev1a1listers.FlunderLister

	csvFilename string
	csvFile     io.Writer

	sync.Mutex

	// Data about each endpoint, access under mutex
	objects map[string]*ObjectData
}

type ObjectData struct {
	sync.Mutex
	actuallyExists bool
	ObjectQueueData
}

// ObjectQueueData says what has happened since the last time
// a reference to the object was dequeued for logging.
type ObjectQueueData struct {
	firstEnqueue, lastEnqueue                time.Time
	queuedAdds, queuedUpdates, queuedDeletes uint
}

var zeroObjectQueueData ObjectQueueData

var dummyTime = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

func (c *Controller) getObjectData(key string, addIfMissing, deleteIfPresent bool) *ObjectData {
	c.Lock()
	defer c.Unlock()
	od := c.objects[key]
	if od == nil {
		od = &ObjectData{}
		if addIfMissing {
			c.objects[key] = od
		}
	} else if deleteIfPresent {
		delete(c.objects, key)
	}
	return od
}

func NewController(queue workqueue.RateLimitingInterface, informer cache.Controller, lister wardlev1a1listers.FlunderLister, csvFilename string) *Controller {
	return &Controller{
		informer:    informer,
		queue:       queue,
		lister:      lister,
		csvFilename: csvFilename,
		objects:     make(map[string]*ObjectData),
	}
}

func (c *Controller) processNextItem() bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// Tell the queue that we are done with processing this key. This unblocks the key for other workers
	// This allows safe parallel processing because two objects with the same key are never processed in
	// parallel.
	defer c.queue.Done(key)

	// Invoke the method containing the business logic
	err := c.logDequeue(key.(string))
	// Handle the error if something went wrong during the execution of the business logic
	c.handleErr(err, key)
	return true
}

func (c *Controller) logDequeue(key string) error {
	now := time.Now()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("Failed to split key %q: %v", key, err))
		return nil
	}
	obj, err := c.lister.Flunders(namespace).Get(name)
	if err != nil && !apierrors.IsNotFound(err) {
		runtime.HandleError(fmt.Errorf("Fetching object with key %s from store failed with %v", key, err))
		return nil
	}
	desireExist := err == nil
	od := c.getObjectData(key, desireExist, !desireExist)
	op := "delete"
	var creationTime time.Time = dummyTime
	if obj != nil {
		creationTime = obj.ObjectMeta.CreationTimestamp.Time
	}
	var oqd ObjectQueueData
	func() {
		od.Lock()
		defer od.Unlock()
		oqd = od.ObjectQueueData
		if desireExist {
			if od.actuallyExists {
				op = "update"
			} else {
				op = "create"
			}
			od.ObjectQueueData = zeroObjectQueueData
			od.actuallyExists = true
		}
	}()

	// Log it
	if c.csvFile != nil {
		_, err = c.csvFile.Write([]byte(fmt.Sprintf("%s,%s,%q,%s,%d,%d,%d,%s,%s\n",
			formatTime(now), op, key, formatTimeNoMillis(creationTime),
			oqd.queuedAdds, oqd.queuedUpdates, oqd.queuedDeletes,
			formatTime(oqd.firstEnqueue), formatTime(oqd.lastEnqueue),
		)))
		if err != nil {
			runtime.HandleError(fmt.Errorf("Error writing to CSV file named %q: %+v", c.csvFilename, err))
		}
	} else {
		glog.V(4).Infof("c.csvFile == nil\n")
	}

	return nil
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return
	}

	glog.Infof("Error syncing Flunder %v: %v", key, err)

	// Re-enqueue the key rate limited. Based on the rate limiter on the
	// queue and the re-enqueue history, the key will be processed later again.
	c.queue.AddRateLimited(key)
	return
}

func (c *Controller) Run(threadiness int, stopCh chan struct{}) {
	defer runtime.HandleCrash()

	// Let the workers stop when we are done
	defer c.queue.ShutDown()
	glog.Info("Starting Object Logging controller")

	csvFile, err := os.Create(c.csvFilename)
	if err != nil {
		runtime.HandleError(fmt.Errorf("Failed to create data file named %q: %s", c.csvFilename, err))
	} else {
		c.csvFile = csvFile
		defer csvFile.Close()
	}

	go c.informer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	glog.Info("Stopping Object Logging controller")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func main() {
	var kubeconfig string
	var master string
	var useProtobuf bool
	var dataFilename string
	var numThreads int
	var overrideServer string
	var insecure bool

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.BoolVar(&useProtobuf, "useProtobuf", false, "indicates whether to encode objects with protobuf (as opposed to JSON)")
	flag.StringVar(&dataFilename, "data-filname", "/tmp/obj-log.csv", "name of CSV file to create")
	flag.IntVar(&numThreads, "threads", 1, "number of worker threads")
	flag.StringVar(&overrideServer, "override-server", "", "server to use instead of the configured one (host string, host:port pair, or base URL)")
	flag.BoolVar(&insecure, "insecure", false, "indicates whether to skip verifying the server's certificate")
	flag.Set("logtostderr", "true")
	flag.Parse()

	// creates the connection
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		glog.Fatal(err)
	}
	glog.V(3).Infof("Config=%#v\n", config)
	if insecure {
		glog.Infof("Skipping verification of server cert\n")
		config.TLSClientConfig.Insecure = true
		config.TLSClientConfig.CAFile = ""
		config.TLSClientConfig.CAData = nil
	}
	if overrideServer != "" {
		glog.Infof("Using server %q instead of %q\n", overrideServer, config.Host)
		config.Host = overrideServer
	}
	myAddr := GetHostAddr()
	glog.Infof("Using %s as my host address\n", myAddr)
	glog.Infof("data-filname=%q, threads=%d\n", dataFilename, numThreads)
	config.UserAgent = fmt.Sprintf("obj-logger@%s", myAddr)

	if useProtobuf {
		config.ContentType = "application/vnd.kubernetes.protobuf"
	}

	// creates the clientset
	clientset, err := wardleclient.NewForConfig(config)
	if err != nil {
		glog.Fatal(err)
	}

	informerFactory := wardleinformers.NewSharedInformerFactory(clientset, 0)
	InformerIfc := informerFactory.Wardle().V1alpha1().Flunders()
	informer := InformerIfc.Informer()
	lister := InformerIfc.Lister()

	// create the workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	controller := NewController(queue, informer, lister, dataFilename)

	// Bind the workqueue to a cache with the help of an informer. This way we make sure that
	// whenever the cache is updated, the object key is added to the workqueue.
	// Note that when we finally process the item from the workqueue, we might see a newer version
	// of the object than the version which was responsible for triggering the update.
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			now := time.Now()
			glog.V(4).Infof("ADD %+v\n", obj)
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				od := controller.getObjectData(key, true, false)
				od.Lock()
				defer od.Unlock()
				if od.queuedAdds+od.queuedUpdates+od.queuedDeletes == 0 {
					od.firstEnqueue = now
				}
				od.lastEnqueue = now
				od.queuedAdds++
				queue.Add(key)
			} else {
				glog.Errorf("Failed to parse key from obj %#v: %v\n", obj, err)
			}
		},
		UpdateFunc: func(obj interface{}, newobj interface{}) {
			now := time.Now()
			glog.V(4).Infof("UPDATE %#v\n", newobj)
			key, err := cache.MetaNamespaceKeyFunc(newobj)
			if err == nil {
				od := controller.getObjectData(key, true, false)
				od.Lock()
				defer od.Unlock()
				if od.queuedAdds+od.queuedUpdates+od.queuedDeletes == 0 {
					od.firstEnqueue = now
				}
				od.lastEnqueue = now
				od.queuedUpdates++
				queue.Add(key)
			} else {
				glog.Errorf("Failed to parse key from obj %#v: %v\n", newobj, err)
			}
		},
		DeleteFunc: func(obj interface{}) {
			now := time.Now()
			glog.V(4).Infof("DELETE %#v\n", obj)
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this
			// key function.
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				od := controller.getObjectData(key, true, false)
				od.Lock()
				defer od.Unlock()
				if od.queuedAdds+od.queuedUpdates+od.queuedDeletes == 0 {
					od.firstEnqueue = now
				}
				od.lastEnqueue = now
				od.queuedDeletes++
				queue.Add(key)
			} else {
				glog.Errorf("Failed to parse key from obj %#v: %v\n", obj, err)
			}
		},
	})

	// Now let's start the controller
	stop := make(chan struct{})
	defer close(stop)
	go controller.Run(numThreads, stop)

	// Wait forever
	select {}
}

func formatTime(t time.Time) string {
	t = t.UTC()
	Y, M, D := t.Date()
	h, m, s := t.Clock()
	ms := t.Nanosecond() / 1000000
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%03d", Y, M, D, h, m, s, ms)
}

func formatTimeNoMillis(t time.Time) string {
	t = t.UTC()
	Y, M, D := t.Date()
	h, m, s := t.Clock()
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", Y, M, D, h, m, s)
}
