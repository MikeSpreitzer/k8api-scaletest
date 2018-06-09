package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
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
	//	scheme "k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
)

const (
	// The name of the annotation holding the client-side creation timestamp.
	CreateTimestampAnnotation = "scaletest/createTimestamp"
	UpdateTimestampAnnotation = "scaletest/updateTimestamp"

	// The layout of the annotation holding the client-side creation timestamp.
	CreateTimestampLayout = "2006-01-02 15:04:05.000 -0700"
)

var kubeconfigPath = flag.String("kubeconfig", "", "Path to kubeconfig file")
var n = flag.Uint64("n", 300, "Total number of objects to create")
var maxPop = flag.Uint64("maxpop", 100, "Maximum object population in system")
var conns = flag.Int("conns", 0, "Number of connections to use; 0 means to use all the endpoints of the kubernetes service")
var threads = flag.Uint64("threads", 1, "Total number of threads to use")
var targetRate = flag.Float64("rate", 100, "Target aggregate rate, in ops/sec")
var namePadLength = flag.Uint("name-pad-length", 0, "number of extra characters in object name")
var createValueLength = flag.Int("create-value-length", 100, "Length of data value in create operation")
var updateValueLength = flag.Int("update-value-length", 100, "Length of data value in update operation")
var updates = flag.Uint64("updates", 1, "number of updates in one object's lifecycle")
var dataFilename = flag.String("datafile", "{{.RunID}}-driver.csv", "Name of CSV file to create")
var runID = flag.String("runid", "", "unique ID of this run (default is randomly generated)")
var seed = flag.Int64("seed", 0, "seed for random numbers (other than runid) (default is based on time)")

const namespace = "scaletest"

func main() {

	flag.Set("logtostderr", "true")
	flag.Parse()

	if *updateValueLength < 10 {
		*updateValueLength = 10
	}

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
	myEx, err := os.Executable()
	if err != nil {
		glog.Warningf("os.Executable() threw %#+v\n", err)
	} else {
		myExF, err := os.Open(myEx)
		if err != nil {
			glog.Warningf("os.Open(%q) threw %#+v\n", err)
		} else {
			defer myExF.Close()
			hasher := sha256.New()
			_, err = io.Copy(hasher, myExF)
			if err != nil {
				glog.Warning("io.Copy threw %#+v\n", err)
			} else {
				hash := hasher.Sum(nil)
				parmFile.WriteString(fmt.Sprintf("PROG_SHA256=\"%x\"\n", hash))
			}
		}
	}
	parmFile.WriteString(fmt.Sprintf("KUBECONFIG=%q\n", *kubeconfigPath))
	parmFile.WriteString(fmt.Sprintf("N=%d\n", *n))
	parmFile.WriteString(fmt.Sprintf("MAXPOP=%d\n", *maxPop))
	parmFile.WriteString(fmt.Sprintf("CONNS=%d\n", *conns))
	parmFile.WriteString(fmt.Sprintf("THREADS=%d\n", *threads))
	parmFile.WriteString(fmt.Sprintf("RATE=%g\n", *targetRate))
	parmFile.WriteString(fmt.Sprintf("UPDATES=%d\n", *updates))
	parmFile.WriteString(fmt.Sprintf("NAME_PAD_LENGTH=%d\n", *namePadLength))
	parmFile.WriteString(fmt.Sprintf("CREATE_VALUE_LENGTH=%d\n", *createValueLength))
	parmFile.WriteString(fmt.Sprintf("UPDATE_VALUE_LENGTH=%d\n", *updateValueLength))
	parmFile.WriteString(fmt.Sprintf("DATAFILENAME=%q\n", *dataFilename))
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
	glog.Infof("RunID is %s\n", *runID)
	fmt.Printf("Wrote parameter file %q\n", parmFileName)

	/* open the CVS file we are going to write */
	csvFile, err := os.Create(*dataFilename)
	if err != nil {
		panic(err)
	}

	deltaFmt := fmt.Sprintf(`{"data": {"baz": "%%0%dd"}, "metadata": {"annotations": {%q: %%q}}}`, *updateValueLength, UpdateTimestampAnnotation)

	fmt.Printf("DEBUG: Creating %d objects\n", *n)
	fmt.Printf("DEBUG: maxpop = %d\n", *maxPop)
	fmt.Printf("DEBUG: endpoints are %v\n", endpoints)
	fmt.Printf("DEBUG: CONNS = %d\n", *conns)
	fmt.Printf("DEBUG: THREADS = %d\n", *threads)
	fmt.Printf("DEBUG: RATE = %g/sec\n", *targetRate)
	fmt.Printf("DEBUG: UPDATES = %d\n", *updates)
	fmt.Printf("DEBUG: NamePadLength = %d\n", *namePadLength)
	fmt.Printf("DEBUG: CreateValueLength = %d\n", *createValueLength)
	fmt.Printf("DEBUG: UpdateValueLength = %d\n", *updateValueLength)
	fmt.Printf("DEBUG: deltaFmt = `%s`\n", deltaFmt)
	createValue := ""
	if *createValueLength > 0 {
		createValue = strings.Repeat("X", *createValueLength)
	}
	opPeriod := 1 / *targetRate
	fmt.Printf("DEBUG: op period = %g sec\n", opPeriod)
	digits := uint(1 + math.Floor(math.Log10(float64(*n))))
	totalNameLength := uint(len(*runID)) + 1 + digits + *namePadLength
	if totalNameLength > 253 {
		glog.Errorf("Total name length %d is too long (limit is 253)\n", totalNameLength)
		os.Exit(17)
	}
	namefmt := fmt.Sprintf("%%s-%%0%dd", digits+*namePadLength)
	t0 := time.Now()
	glog.V(2).Infof("t0 = %s\n", t0)
	var wg sync.WaitGroup
	for i := uint64(0); i < *threads; i++ {
		wg.Add(1)
		go func(thd uint64) {
			defer wg.Done()
			// maxpop = [ lag + updates*(lag-1) ] * threads
			// maxpop + updates*threads = lag * (1 + updates) * threads
			lag := (*maxPop + *updates*(*threads) + thd) / ((1 + *updates) * *threads)
			if lag < 1 {
				lag = 1
			}
			numHere := (*n + thd) / (*threads)
			RunThread(clientsets[thd%uint64(*conns)], csvFile, namefmt, *runID, createValue, deltaFmt, t0, *updates, numHere, lag, thd+1, *threads, opPeriod)
		}(i)
	}
	glog.V(2).Info("waiting for threads to finish\n")
	wg.Wait()
	tf := time.Now()
	dt := tf.Sub(t0)
	dts := dt.Seconds()
	rate := float64(2+*updates) * float64(*n) / dts
	glog.Infof("%d object lifecycles in %g seconds = %g writes/sec, with %d errors on create, %d on update, and %d on delete\n", *n, dts, rate, createErrors, updateErrors, deleteErrors)
	glog.Infof("Protobuf length of a created object = %d, protobuf length of an updated object = %d\n", createdObjLen, updatedObjLen)
}

var createErrors, updateErrors, deleteErrors int64
var createdObjLen, updatedObjLen int

/* =============================================== */
/* simulate the lifecycles of one thread's objects */
/* =============================================== */

const hackTimeout = false

// RunThreads runs n objects through their lifecycle, with the given
// lag between phases.  The objects are numbered thd, thd+stride,
// thd+2*stride, and so on.

func RunThread(clientset *kubeclient.Clientset, csvFile *os.File, namefmt, runID, createValue, deltaFmt string, tbase time.Time, updates, n, lag, thd, stride uint64, opPeriod float64) {
	glog.V(3).Infof("Thread %d creating %d objects with lag %d, stride %d, clientset %p\n", thd, n, lag, stride, clientset)
	var iByPhase []uint64 = make([]uint64, 2+updates)
	var iSum uint64
	lastPhase := 1 + updates
	for iByPhase[lastPhase] < n {
		dt := float64(iSum*stride+thd) * opPeriod * float64(time.Second)
		targt := tbase.Add(time.Duration(dt))
		now := time.Now()
		if targt.After(now) {
			gap := targt.Sub(now)
			glog.V(4).Infof("For %#v in thread %d, target time is %s, now is %s; sleeping %s\n", iByPhase, thd, targt, now, gap)
			time.Sleep(gap)
		} else {
			glog.V(4).Infof("For %#v in thread %d, target time is %s, now is %s; no sleep\n", iByPhase, thd, targt, now)
		}
		phase := lastPhase
		for ; phase > 0 && iByPhase[phase-1] < iByPhase[phase]+lag && iByPhase[phase-1] < n; phase-- {
		}
		if iByPhase[phase] == 0 {
			glog.V(3).Infof("Thread %d doing first at phase %d\n", thd, phase)
		}
		i := iByPhase[phase]*stride + thd
		iByPhase[phase] += 1
		iSum += 1
		objname := fmt.Sprintf(namefmt, runID, i)
		ti0 := time.Now()
		if phase == 0 {
			obj := &kubecorev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:        objname,
					Namespace:   namespace,
					Labels:      map[string]string{"purpose": "scaletest"},
					Annotations: map[string]string{CreateTimestampAnnotation: ti0.Format(CreateTimestampLayout)},
				},
				Data: map[string]string{"foo": createValue},
			}
			var err error
			var retObj *kubecorev1.ConfigMap
			if hackTimeout {
				var result kubecorev1.ConfigMap
				retObj = &result
				cv1 := clientset.CoreV1().(*corev1.CoreV1Client)
				rc := cv1.RESTClient()
				err = rc.Post().
					Namespace(namespace).
					Resource("configmaps").
					Param("timeoutSeconds", "17").
					Param("nm", objname).
					//	VersionedParams(
					//		&metav1.ListOptions{TimeoutSeconds: &toSecs},
					//		scheme.ParameterCodec).
					Body(obj).
					Do().
					Into(retObj)
			} else {
				retObj, err = clientset.CoreV1().ConfigMaps(namespace).Create(obj)
			}
			tif := time.Now()
			writelog("create", obj.Name, ti0, tif, csvFile, err)
			if err != nil {
				atomic.AddInt64(&createErrors, 1)
			} else if i == 1 {
				var buf []byte
				var err error
				buf, err = retObj.Marshal()
				if err != nil {
					glog.Warningf("Marshaling returned object %#+v threw %#+v\n", retObj, err)
				} else {
					createdObjLen = len(buf)
				}
			}
		} else if phase < lastPhase {
			ti0s := ti0.Format(CreateTimestampLayout)
			delta := fmt.Sprintf(deltaFmt, phase, ti0s)
			retObj, err := clientset.CoreV1().ConfigMaps(namespace).Patch(objname, types.StrategicMergePatchType, []byte(delta))
			tif := time.Now()
			writelog("update", objname, ti0, tif, csvFile, err)
			if err != nil {
				atomic.AddInt64(&updateErrors, 1)
			} else if i == 1 && phase == 1 {
				var buf []byte
				var err error
				buf, err = retObj.Marshal()
				if err != nil {
					glog.Warningf("Marshaling returned object %#+v threw %#+v\n", retObj, err)
				} else {
					updatedObjLen = len(buf)
				}
			}
		} else {
			delopts := &metav1.DeleteOptions{}
			err := clientset.CoreV1().ConfigMaps(namespace).Delete(objname, delopts)
			tif := time.Now()
			writelog("delete", objname, ti0, tif, csvFile, err)
			if err != nil {
				atomic.AddInt64(&deleteErrors, 1)
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
		if errS == "" {
			errS = fmt.Sprintf("%#+v", err)
		}
	}
	// fmt.Printf("%s,%s,%s,%q,%q\n", formatTime(tBefore), formatTime(tAfter), op, key, errS)
	csvFile.Write([]byte(fmt.Sprintf("%s,%s,%s,%q,%q\n", formatTime(tBefore), formatTime(tAfter), op, key, errS)))
}
