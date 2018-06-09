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
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	//	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (

	// The HTTP port under which the scraping endpoint ("/metrics") is served.
	MetricsAddr = ":9101"

	// The HTTP path under which the scraping endpoint ("/metrics") is served.
	MetricsPath = "/metrics"

	// The namespace, subsystem and name of the histogram collected by this controller.
	HistogramNamespace  = "scaletest"
	HistogramSubsystem  = "configmaps"
	CreateHistogramName = "create_latency_seconds"
	UpdateHistogramName = "update_latency_seconds"

	// The name of the annotation holding the client-side creation timestamp.
	CreateTimestampAnnotation = "scaletest/createTimestamp"
	UpdateTimestampAnnotation = "scaletest/updateTimestamp"

	// The layout of the annotation holding the client-side creation timestamp.
	CreateTimestampLayout = "2006-01-02 15:04:05.000 -0700"
)

type Controller struct {
	myAddr   string
	compare  bool
	queue    workqueue.RateLimitingInterface
	informer cache.Controller
	lister   corev1listers.ConfigMapLister

	csvFilename string
	csvFile     io.Writer

	// A histogram to collect latency samples
	createLatencyHistogram *prometheus.HistogramVec
	updateLatencyHistogram *prometheus.HistogramVec

	// Counters for unusual events
	updateCounter, strangeCounter *prometheus.CounterVec
	duplicateCounter              prometheus.Counter

	// Guage for ResourceVersion
	rvGauge prometheus.Gauge

	sync.Mutex

	// Data about each endpoint, access under mutex
	objects map[string]*ObjectData
}

type ObjectData struct {
	sync.Mutex
	actuallyExists bool
	ObjectQueueData
	lastSeen *corev1.ConfigMap
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

func NewController(queue workqueue.RateLimitingInterface, informer cache.Controller, lister corev1listers.ConfigMapLister, compare bool, csvFilename, myAddr string) *Controller {
	createHistogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: HistogramNamespace,
			Subsystem: HistogramSubsystem,
			Name:      CreateHistogramName,
			Help:      "Configmap creation notification latency, in seconds",

			Buckets: []float64{-0.1, 0, 0.03125, 0.0625, 0.125, 0.25, 0.5, 1, 2, 4, 8},
		},
		[]string{"logger"},
	)
	if err := prometheus.Register(createHistogram); err != nil {
		glog.Error(err)
		createHistogram = nil
	} else {
		createHistogram.With(prometheus.Labels{"logger": myAddr}).Observe(0)
	}

	updateHistogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: HistogramNamespace,
			Subsystem: HistogramSubsystem,
			Name:      UpdateHistogramName,
			Help:      "Configmap creation notification latency, in seconds",

			Buckets: []float64{-0.1, 0, 0.03125, 0.0625, 0.125, 0.25, 0.5, 1, 2, 4, 8},
		},
		[]string{"logger"},
	)
	if err := prometheus.Register(updateHistogram); err != nil {
		glog.Error(err)
		updateHistogram = nil
	} else {
		updateHistogram.With(prometheus.Labels{"logger": myAddr}).Observe(0)
	}

	updateCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: HistogramNamespace,
			Subsystem: HistogramSubsystem,
			Name:      "updates",
			Help:      "number of updates dequeued",
		},
		[]string{"logger"},
	)
	if err := prometheus.Register(updateCounter); err != nil {
		glog.Error(err)
		updateCounter = nil
	} else {
		updateCounter.With(prometheus.Labels{"logger": myAddr}).Add(0)
	}
	strangeCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: HistogramNamespace,
			Subsystem: HistogramSubsystem,
			Name:      "stranges",
			Help:      "number of strange situations dequeued",
		},
		[]string{"logger"},
	)
	if err := prometheus.Register(strangeCounter); err != nil {
		glog.Error(err)
		strangeCounter = nil
	} else {
		strangeCounter.With(prometheus.Labels{"logger": myAddr}).Add(0)
	}

	duplicateCounter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "duplicates",
			Help:        "number of duplicates dequeued",
			ConstLabels: map[string]string{"logger": myAddr},
		})
	if err := prometheus.Register(duplicateCounter); err != nil {
		glog.Error(err)
		duplicateCounter = nil
	} else {
		duplicateCounter.Add(0)
	}

	rvGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   HistogramNamespace,
			Subsystem:   HistogramSubsystem,
			Name:        "resourceVersion",
			Help:        "latest ResourceVersion observed",
			ConstLabels: map[string]string{"logger": myAddr},
		})
	if err := prometheus.Register(rvGauge); err != nil {
		glog.Error(err)
		rvGauge = nil
	}

	return &Controller{
		myAddr:                 myAddr,
		compare:                compare,
		informer:               informer,
		queue:                  queue,
		lister:                 lister,
		csvFilename:            csvFilename,
		createLatencyHistogram: createHistogram,
		updateLatencyHistogram: updateHistogram,
		updateCounter:          updateCounter,
		strangeCounter:         strangeCounter,
		duplicateCounter:       duplicateCounter,
		rvGauge:                rvGauge,
		objects:                make(map[string]*ObjectData),
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
	obj, err := c.lister.ConfigMaps(namespace).Get(name)
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
	var lastSeen *corev1.ConfigMap
	func() {
		od.Lock()
		defer od.Unlock()
		oqd = od.ObjectQueueData
		if c.compare {
			lastSeen = od.lastSeen
			od.lastSeen = obj.DeepCopy()
		}
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

	var diff int
	if c.compare {
		if ConfigMapQuickEqual(lastSeen, obj) {
			diff = 2
			c.duplicateCounter.Add(1)
		} else {
			diff = 3
		}
	}

	// Log it
	if c.csvFile != nil {
		_, err = c.csvFile.Write([]byte(fmt.Sprintf("%s,%s,%q,%s,%d,%d,%d,%s,%s,%d\n",
			formatTime(now), op, key, formatTimeNoMillis(creationTime),
			oqd.queuedAdds, oqd.queuedUpdates, oqd.queuedDeletes,
			formatTime(oqd.firstEnqueue), formatTime(oqd.lastEnqueue),
			diff,
		)))
		if err != nil {
			runtime.HandleError(fmt.Errorf("Error writing to CSV file named %q: %+v", c.csvFilename, err))
		}
	} else {
		glog.V(4).Infof("c.csvFile == nil\n")
	}

	if diff == 2 {
		return nil
	}

	if oqd.queuedAdds+oqd.queuedUpdates+oqd.queuedDeletes != 1 {
		if c.strangeCounter != nil {
			c.strangeCounter.
				With(prometheus.Labels{"logger": c.myAddr}).
				Add(1)
		}
	} else if oqd.queuedUpdates == 1 && c.updateCounter != nil {
		c.updateCounter.
			With(prometheus.Labels{"logger": c.myAddr}).
			Add(1)
	}

	if op != "delete" && obj != nil && obj.Annotations != nil {
		var ctS string
		var latencyHistogram *prometheus.HistogramVec
		if op == "create" {
			ctS = obj.Annotations[CreateTimestampAnnotation]
			latencyHistogram = c.createLatencyHistogram
		} else {
			ctS = obj.Annotations[UpdateTimestampAnnotation]
			latencyHistogram = c.updateLatencyHistogram
		}
		if ctS != "" && latencyHistogram != nil {
			clientTime, err := time.Parse(CreateTimestampLayout, ctS)
			if err != nil {
				return nil
			}
			latency := now.Sub(clientTime)
			glog.V(4).Infof("Latency = %v for op=%s, key=%s, now=%s, clientTime=%s, ts=%s\n", latency, op, key, now, clientTime, ctS)
			latencyHistogram.
				With(prometheus.Labels{"logger": c.myAddr}).
				Observe(latency.Seconds())
		}
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

	glog.Infof("Error syncing ConfigMap %v: %v", key, err)

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

func (c *Controller) ObserveResourceVersion(obj interface{}) {
	switch o := obj.(type) {
	case cache.DeletedFinalStateUnknown:
		glog.V(5).Infof("Recursing for %#+v @ %#p\n", obj, obj)
		c.ObserveResourceVersion(o.Obj)
	case cache.ExplicitKey:
		glog.V(5).Infof("Got ExplicitKey %q\n", o)
		return
	default:
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			glog.V(5).Infof("apimeta.Accessor(%#+v) threw %#+v\n", obj, err)
			return
		}
		rvS := meta.GetResourceVersion()
		rvU, err := strconv.ParseUint(rvS, 10, 64)
		if err != nil {
			glog.V(5).Infof("Error parsing ResourceVersion %q of %#+v: %#+v\n", rvS, obj, err)
		} else {
			glog.V(5).Infof("Observing ResourceVersion %d of %#+v @ %#p\n", rvU, obj, obj)
			c.rvGauge.Set(float64(rvU))
		}
	}
}

func main() {
	var kubeconfig string
	var master string
	var useProtobuf bool
	var dataFilename string
	var numThreads int
	var noCompare bool

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.BoolVar(&useProtobuf, "useProtobuf", false, "indicates whether to encode objects with protobuf (as opposed to JSON)")
	flag.StringVar(&dataFilename, "data-filname", "/tmp/obj-log.csv", "name of CSV file to create")
	flag.IntVar(&numThreads, "threads", 1, "number of worker threads")
	flag.BoolVar(&noCompare, "no-compare", false, "omit comparing object values")
	flag.Set("logtostderr", "true")
	flag.Parse()

	// creates the connection
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Infof("Config.Host=%q\n", config.Host)
	glog.Infof("Config.APIPath=%q\n", config.APIPath)
	glog.Infof("Config.Prefix=%q\n", config.Prefix)
	myAddr := GetHostAddr()
	glog.Infof("Using %s as my host address\n", myAddr)
	config.UserAgent = fmt.Sprintf("obj-logger@%s", myAddr)

	if useProtobuf {
		config.ContentType = "application/vnd.kubernetes.protobuf"
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatal(err)
	}

	informerFactory := informers.NewSharedInformerFactory(clientset, 0)
	cfgMapInformer := informerFactory.Core().V1().ConfigMaps()
	informer := cfgMapInformer.Informer()
	lister := cfgMapInformer.Lister()

	// create the workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	controller := NewController(queue, informer, lister, !noCompare, dataFilename, myAddr)

	// Bind the workqueue to a cache with the help of an informer. This way we make sure that
	// whenever the cache is updated, the object key is added to the workqueue.
	// Note that when we finally process the item from the workqueue, we might see a newer version
	// of the object than the version which was responsible for triggering the update.
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			now := time.Now()
			glog.V(4).Infof("ADD %+v @ %#p\n", obj, obj)
			controller.ObserveResourceVersion(obj)
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
		UpdateFunc: func(oldobj interface{}, newobj interface{}) {
			now := time.Now()
			glog.V(4).Infof("UPDATE %#v @ %#p\n", newobj, newobj)
			controller.ObserveResourceVersion(newobj)
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
			glog.V(4).Infof("DELETE %#v @ %#p\n", obj, obj)
			controller.ObserveResourceVersion(obj)
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

	// Serve Prometheus metrics
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		glog.Error(http.ListenAndServe(MetricsAddr, nil))
	}()

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

func ConfigMapQuickEqual(x, y *corev1.ConfigMap) bool {
	if x == y {
		return true
	}
	if x == nil || y == nil {
		return false
	}
	return x.Name == y.Name && x.Namespace == y.Namespace &&
		x.UID == y.UID && x.ResourceVersion == y.ResourceVersion &&
		MapStringStringEqual(x.Data, y.Data) &&
		MapStringStringEqual(x.Labels, y.Labels) &&
		MapStringStringEqual(x.Annotations, y.Annotations)
}

func MapStringStringEqual(x, y map[string]string) bool {
	if x == nil {
		return y == nil
	} else if y == nil {
		return false
	}
	if len(x) != len(y) {
		return false
	}
	for k, v := range x {
		if y[k] != v {
			return false
		}
	}
	return true
}
