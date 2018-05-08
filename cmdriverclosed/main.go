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
var n = flag.Int("n", 300, "Total number of objects to create")
var conns = flag.Int("conns", 1, "Number of connections to use")
var threads = flag.Int("threads", 1, "Total number of threads to use")
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

	parmFileName := *runID + "-driver.parms"
	parmFile, err := os.Create(parmFileName)
	if err != nil {
		glog.Errorf("Failed to create parameter file named %q: %s\n", parmFileName, err)
		os.Exit(10)
	}
	parmFile.WriteString(fmt.Sprintf("KUBECONFIG=%q\n", *kubeconfigPath))
	parmFile.WriteString(fmt.Sprintf("N=%d\n", *n))
	parmFile.WriteString(fmt.Sprintf("CONNS=%d\n", *conns))
	parmFile.WriteString(fmt.Sprintf("THREADS=%d\n", *threads))
	parmFile.WriteString(fmt.Sprintf("DATEFILENAME=%q\n", *dataFilename))
	parmFile.WriteString(fmt.Sprintf("RUNID=%q\n", *runID))
	parmFile.WriteString(fmt.Sprintf("SEED=%d\n", *seed))
	if err = parmFile.Close(); err != nil {
		glog.Errorf("Failed to close parameter file named %q: %s\n", parmFileName, err)
		os.Exit(11)
	}
	fmt.Printf("RunID is %s\n", *runID)
	fmt.Printf("Wrote parameter file %q\n", parmFileName)

	/* connect to the API server */
	config, err := getClientConfig(*kubeconfigPath)
	if err != nil {
		glog.Errorf("Unable to get kube client config: %s", err)
		os.Exit(20)
	}
	config.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()

	clientsets := make([]*kubeclient.Clientset, *conns)
	for idx := range clientsets {
		clientsets[idx], err = kubeclient.NewForConfig(config)
		if err != nil {
			glog.Error("Failed to create a clientset: %s\n", err)
			os.Exit(21)
		}
	}

	/* open the CVS file we are going to write */
	csvFile, err := os.Create(*dataFilename)
	if err != nil {
		panic(err)
	}

	fmt.Printf("DEBUG: Creating %d objects\n", *n)
	fmt.Printf("DEBUG: CONNS = %d\n", *conns)
	fmt.Printf("DEBUG: THREADS = %d\n", *threads)
	var wg sync.WaitGroup
	digits := int(1 + math.Floor(math.Log10(float64(*n))))
	namefmt := fmt.Sprintf("%%s-%%0%dd", digits)
	t0 := time.Now()
	for i := 1; i <= *threads; i++ {
		wg.Add(1)
		go func(objnum int) {
			defer wg.Done()
			RunObjLifeCycle(clientsets[i%*conns], csvFile, namefmt, *runID, objnum, *threads, *n)
		}(i)
	}

	fmt.Printf("DEBUG: waiting for objects to clear\n")
	wg.Wait()
	tf := time.Now()
	dt := tf.Sub(t0)
	dts := dt.Seconds()
	rate := 3.0 * float64(*n) / dts
	fmt.Printf("%d object lifecycles in %g seconds = %g writes/sec\n", *n, dts, rate)
	//os.Exit(0)

}

/* =========================================== */
/* simulate the lifecycle of a single object   */
/* =========================================== */

func RunObjLifeCycle(clientset *kubeclient.Clientset, csvFile *os.File, namefmt, runID string, phase, stride, n int) {
	var err error

	for i := phase; i <= n; i += stride {
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
		_, err = clientset.CoreV1().ConfigMaps(namespace).Create(obj)
		t1f := time.Now()
		writelog("create", obj.Name, t10, t1f, csvFile, err)

		t20 := time.Now()
		_, err = clientset.CoreV1().ConfigMaps(namespace).Patch(obj.Name, types.StrategicMergePatchType, []byte(`{"data": {"baz": "zab"}}`), "")
		t2f := time.Now()
		writelog("update", obj.Name, t20, t2f, csvFile, err)

		/* delete the object */
		delopts := &metav1.DeleteOptions{}
		t30 := time.Now()
		err = clientset.CoreV1().ConfigMaps(namespace).Delete(obj.Name, delopts)
		t3f := time.Now()
		writelog("delete", obj.Name, t30, t3f, csvFile, err)
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
