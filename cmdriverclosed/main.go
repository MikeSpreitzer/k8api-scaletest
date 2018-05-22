package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"

	kubecorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"

	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
)

var kubeconfigPath = flag.String("kubeconfig", "", "Path to kubeconfig file")
var n = flag.Uint64("n", 300, "Total number of objects to create")
var maxPop = flag.Uint64("maxpop", 100, "Maximum object population in system")
var conns = flag.Int("conns", 0, "Number of connections to use; 0 means to use all the endpoints of the kubernetes service")
var threads = flag.Uint64("threads", 1, "Total number of threads to use")
var targetRate = flag.Float64("rate", 100, "Target aggregate rate, in ops/sec")
var dataFilename = flag.String("datafile", "{{.RunID}}-driver.csv", "Name of CSV file to create")
var runID = flag.String("runid", "", "unique ID of this run (default is randomly generated)")
var seed = flag.Int64("seed", 0, "seed for random numbers (other than runid) (default is based on time)")

const namespace = "scaletest"

func main() {

	flag.Set("logtostderr", "true")
	flag.Parse()

	if *runID == "" {
		now := time.Now()
		rand.Seed(now.UnixNano())
		rand.Int63()
		rand.Int63()
		_, M, D := now.Date()
		h, m, _ := now.Clock()
		*runID = fmt.Sprintf("%02d%02d.%02d%02d.%04d", M, D, h, m, rand.Intn(10000))
	} else if good, _ := regexp.MatchString("^[-a-zA-Z0-9!@#$%^&()+=][-a-zA-Z0-9!@#$%^&()+=.]*$", *runID); !good {
		glog.Errorf("runid %q does not match regular expression ^[-a-zA-Z0-9!@#$%%^&()+=][-a-zA-Z0-9!@#$%%^&()+=.]*$\n", *runID)
		os.Exit(1)
	}

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	*dataFilename = strings.Replace(*dataFilename, "{{.RunID}}", *runID, -1)
	if good, _ := regexp.MatchString("^[-a-zA-Z0-9!@#$%^&()+=./]+$", *dataFilename); !good {
		glog.Errorf("data filename %q does not match regular expression ^[-a-zA-Z0-9!@#$%%^&()+=./]+$\n", *dataFilename)
		os.Exit(5)
	}

	randGen := rand.New(rand.NewSource(*seed))
	randGen.Int63()
	randGen.Int63()

	/* connect to the API server */
	config, err := getClientConfig(*kubeconfigPath)
	if err != nil {
		glog.Errorf("Unable to get kube client config: %s", err)
		os.Exit(20)
	}
	config.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()

	parmFileName := *runID + "-driver.parms"
	parmFile, err := os.Create(parmFileName)
	if err != nil {
		glog.Errorf("Failed to create parameter file named %q: %s\n", parmFileName, err)
		os.Exit(10)
	}
	parmFile.WriteString(fmt.Sprintf("KUBECONFIG=%q\n", *kubeconfigPath))
	parmFile.WriteString(fmt.Sprintf("N=%d\n", *n))
	parmFile.WriteString(fmt.Sprintf("MAXPOP=%d\n", *maxPop))
	parmFile.WriteString(fmt.Sprintf("CONNS=%d\n", *conns))
	parmFile.WriteString(fmt.Sprintf("THREADS=%d\n", *threads))
	parmFile.WriteString(fmt.Sprintf("RATE=%g\n", *targetRate))
	parmFile.WriteString(fmt.Sprintf("DATEFILENAME=%q\n", *dataFilename))
	parmFile.WriteString(fmt.Sprintf("RUNID=%q\n", *runID))
	parmFile.WriteString(fmt.Sprintf("SEED=%d\n", *seed))

	urClientset, err := kubeclient.NewForConfig(config)
	if err != nil {
		glog.Errorf("Failed to create a clientset: %s\n", err)
		os.Exit(21)
	}

	var endpoints []string
	var clientsets []*kubeclient.Clientset
	if *conns != 1 {
		endpoints, err = GetEndpoints(urClientset, "kubernetes", "default", "TCP", "https")
		if err != nil {
			glog.Error(err)
			os.Exit(22)
		}
		if *conns == 0 || *conns > len(endpoints) {
			if *conns > 0 {
				glog.Warningf("Only %d endpoints available\n", len(endpoints))
			}
			*conns = len(endpoints)
		} else if *conns < len(endpoints) {
			endpoints = endpoints[:*conns]
		}
		clientsets, err = ClientsetsForEndpoints(config, endpoints)
		if err != nil {
			glog.Error(err)
			os.Exit(23)
		}
	} else {
		endpoints = []string{config.Host}
		clientsets = []*kubeclient.Clientset{urClientset}
	}
	parmFile.WriteString(fmt.Sprintf("ENDPOINTS=\"%s\"\n", endpoints))

	if err = parmFile.Close(); err != nil {
		glog.Errorf("Failed to close parameter file named %q: %s\n", parmFileName, err)
		os.Exit(11)
	}
	fmt.Printf("RunID is %s\n", *runID)
	fmt.Printf("Wrote parameter file %q\n", parmFileName)

	/* open the CVS file we are going to write */
	csvFile, err := os.Create(*dataFilename)
	if err != nil {
		panic(err)
	}

	fmt.Printf("DEBUG: Creating %d objects\n", *n)
	fmt.Printf("DEBUG: maxpop = %d\n", *maxPop)
	fmt.Printf("DEBUG: endpoints are %v\n", endpoints)
	fmt.Printf("DEBUG: CONNS = %d\n", *conns)
	fmt.Printf("DEBUG: THREADS = %d\n", *threads)
	fmt.Printf("DEBUG: RATE = %g/sec\n", *targetRate)
	opPeriod := 1 / *targetRate
	fmt.Printf("DEBUG: op period = %g sec\n", opPeriod)
	digits := int(1 + math.Floor(math.Log10(float64(*n))))
	namefmt := fmt.Sprintf("%%s-%%0%dd", digits)
	t0 := time.Now()
	glog.V(2).Infof("t0 = %s\n", t0)
	var wg sync.WaitGroup
	for i := uint64(0); i < *threads; i++ {
		wg.Add(1)
		go func(thd uint64) {
			defer wg.Done()
			lag := (*maxPop + thd) / (2 * *threads)
			if lag < 1 {
				lag = 1
			}
			numHere := (*n + thd) / (*threads)
			RunThread(clientsets[thd%uint64(*conns)], csvFile, namefmt, *runID, t0, numHere, lag, thd+1, *threads, opPeriod)
		}(i)
	}
	glog.V(2).Info("waiting for threads to finish\n")
	wg.Wait()
	tf := time.Now()
	dt := tf.Sub(t0)
	dts := dt.Seconds()
	rate := 3.0 * float64(*n) / dts
	glog.Infof("%d object lifecycles in %g seconds = %g writes/sec, with %d errors on create, %d on update, and %d on delete\n", *n, dts, rate, createErrors, updateErrors, deleteErrors)
}

var createErrors, updateErrors, deleteErrors int64

/* =============================================== */
/* simulate the lifecycles of one thread's objects */
/* =============================================== */

// RunThreads runs n objects through their lifecycle, with the given
// lag between phases.  The objects are numbered thd, thd+stride,
// thd+2*stride, and so on.

func RunThread(clientset *kubeclient.Clientset, csvFile *os.File, namefmt, runID string, tbase time.Time, n, lag, thd, stride uint64, opPeriod float64) {
	glog.V(3).Infof("Thread %d creating %d objects with lag %d, stride %d, clientset %p\n", thd, n, lag, stride, clientset)
	var iCreate, iUpdate, iDelete uint64
	for iDelete < n {
		dt := float64((iCreate+iUpdate+iDelete)*stride+thd) * opPeriod * float64(time.Second)
		targt := tbase.Add(time.Duration(dt))
		now := time.Now()
		if targt.After(now) {
			gap := targt.Sub(now)
			glog.V(4).Infof("For (%d,%d,%d) in thread %d, target time is %s, now is %s; sleeping %s\n", iCreate, iUpdate, iDelete, thd, targt, now, gap)
			time.Sleep(gap)
		} else {
			glog.V(4).Infof("For (%d,%d,%d) in thread %d, target time is %s, now is %s; no sleep\n", iCreate, iUpdate, iDelete, thd, targt, now)
		}
		if iUpdate >= iDelete+lag || iUpdate >= n {
			if iDelete == 0 {
				glog.V(3).Infof("Thread %d doing first delete\n", thd)
			}
			i := iDelete*stride + thd
			iDelete += 1
			objname := fmt.Sprintf(namefmt, runID, i)
			/* delete the object */
			delopts := &metav1.DeleteOptions{}
			t30 := time.Now()
			err := clientset.CoreV1().ConfigMaps(namespace).Delete(objname, delopts)
			t3f := time.Now()
			writelog("delete", objname, t30, t3f, csvFile, err)
			if err != nil {
				atomic.AddInt64(&deleteErrors, 1)
			}
		} else if iCreate >= iUpdate+lag || iCreate >= n {
			if iUpdate == 0 {
				glog.V(3).Infof("Thread %d doing first update\n", thd)
			}
			i := iUpdate*stride + thd
			iUpdate += 1
			objname := fmt.Sprintf(namefmt, runID, i)
			/* Update the object */
			t20 := time.Now()
			_, err := clientset.CoreV1().ConfigMaps(namespace).Patch(objname, types.StrategicMergePatchType, []byte(`{"data": {"baz": "zab"}}`), "")
			t2f := time.Now()
			writelog("update", objname, t20, t2f, csvFile, err)
			if err != nil {
				atomic.AddInt64(&updateErrors, 1)
			}
		} else {
			i := iCreate*stride + thd
			if iCreate%1000 == 0 {
				glog.V(3).Infof("Thread %d creating object %d\n", thd, i)
			}
			iCreate += 1
			objname := fmt.Sprintf(namefmt, runID, i)
			/* create the object */
			obj := &kubecorev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      objname,
					Namespace: namespace,
					Labels:    map[string]string{"purpose": "scaletest"},
				},
				Data: map[string]string{"foo": "bar"},
			}
			t10 := time.Now()
			_, err := clientset.CoreV1().ConfigMaps(namespace).Create(obj)
			t1f := time.Now()
			writelog("create", obj.Name, t10, t1f, csvFile, err)
			if err != nil {
				atomic.AddInt64(&createErrors, 1)
			}
		}
	}
}

func getClientConfig(kubeconfig string) (restConfig *rest.Config, err error) {
	if kubeconfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return
	}
	restConfig.UserAgent = "scaletest driver"
	glog.V(4).Infof("*rest.Config = %#v", *restConfig)
	return
}

func setupSignalHandler() (stopCh <-chan struct{}) {
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func nextDelta(randGen *rand.Rand, rateParameter float64) float64 {
	nextDeltaSeconds := randGen.ExpFloat64() / rateParameter
	if nextDeltaSeconds > 300 {
		nextDeltaSeconds = 300
	}
	return nextDeltaSeconds
}

func formatTime(t time.Time) string {
	t = t.UTC()
	Y, M, D := t.Date()
	h, m, s := t.Clock()
	ms := t.Nanosecond() / 1000000
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%03d", Y, M, D, h, m, s, ms)
}

func writelog(op string, key string, tBefore, tAfter time.Time, csvFile *os.File, err error) {
	errS := ""
	if err != nil {
		errS = err.Error()
	}
	// fmt.Printf("%s,%s,%s,%q,%q\n", formatTime(tBefore), formatTime(tAfter), op, key, errS)
	csvFile.Write([]byte(fmt.Sprintf("%s,%s,%s,%q,%q\n", formatTime(tBefore), formatTime(tAfter), op, key, errS)))
}
