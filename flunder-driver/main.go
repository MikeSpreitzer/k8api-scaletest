package main

import (
	"context"
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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"

	wardlev1a1 "k8s.io/sample-apiserver/pkg/apis/wardle/v1alpha1"
	wardleclient "k8s.io/sample-apiserver/pkg/generated/clientset/versioned"
)

var kubeconfigPath = flag.String("kubeconfig", "", "Path to kubeconfig file")
var lambda = flag.Float64("lambda", 1.0, "Rate (1/s) at which new objects are created")
var n = flag.Int("n", 300, "Total number of objects to create")
var maxpop = flag.Int("maxpop", 100, "Maximum object population in system")
var dataFilename = flag.String("datafile", "{{.RunID}}-driver.csv", "Name of CSV file to create")
var runID = flag.String("runid", "", "unique ID of this run (default is randomly generated)")
var seed = flag.Int64("seed", 0, "seed for random numbers (other than runid) (default is based on time)")

const namespace = "scaletest"

func main() {
	klog.InitFlags(nil)
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
		klog.Errorf("runid %q does not match regular expression ^[-a-zA-Z0-9!@#$%%^&()+=][-a-zA-Z0-9!@#$%%^&()+=.]*$\n", *runID)
		os.Exit(1)
	}

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	*dataFilename = strings.Replace(*dataFilename, "{{.RunID}}", *runID, -1)
	if good, _ := regexp.MatchString("^[-a-zA-Z0-9!@#$%^&()+=./]+$", *dataFilename); !good {
		klog.Errorf("data filename %q does not match regular expression ^[-a-zA-Z0-9!@#$%%^&()+=./]+$\n", *dataFilename)
		os.Exit(5)
	}

	randGen := rand.New(rand.NewSource(*seed))
	randGen.Int63()
	randGen.Int63()

	parmFileName := *runID + "-driver.parms"
	parmFile, err := os.Create(parmFileName)
	if err != nil {
		klog.Errorf("Failed to create parameter file named %q: %s\n", parmFileName, err)
		os.Exit(10)
	}
	parmFile.WriteString(fmt.Sprintf("KUBECONFIG=%q\n", *kubeconfigPath))
	parmFile.WriteString(fmt.Sprintf("LAMBDA=%G\n", *lambda))
	parmFile.WriteString(fmt.Sprintf("N=%d\n", *n))
	parmFile.WriteString(fmt.Sprintf("MAXPOP=%d\n", *maxpop))
	parmFile.WriteString(fmt.Sprintf("DATEFILENAME=%q\n", *dataFilename))
	parmFile.WriteString(fmt.Sprintf("RUNID=%q\n", *runID))
	parmFile.WriteString(fmt.Sprintf("SEED=%d\n", *seed))
	if err = parmFile.Close(); err != nil {
		klog.Errorf("Failed to close parameter file named %q: %s\n", parmFileName, err)
		os.Exit(11)
	}
	fmt.Printf("RunID is %s\n", *runID)
	fmt.Printf("Wrote parameter file %q\n", parmFileName)

	/* connect to the API server */
	config, err := getClientConfig(*kubeconfigPath)
	if err != nil {
		klog.Errorf("Unable to get kube client config: %s", err)
		os.Exit(20)
	}
	config.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()

	clientset, err := wardleclient.NewForConfig(config)
	if err != nil {
		klog.Error("Failed to create a clientset: %s\n", err)
		os.Exit(21)
	}

	/* open the CVS file we are going to write */
	csvFile, err := os.Create(*dataFilename)
	if err != nil {
		panic(err)
	}

	ttl := time.Duration(float64(time.Second) * float64(*maxpop) / (*lambda))

	fmt.Printf("DEBUG: Creating %d objects\n", *n)
	fmt.Printf("DEBUG: LAMBDA = %g/sec\n", *lambda)
	fmt.Printf("DEBUG: maxpop = %d\n", *maxpop)
	fmt.Printf("DEBUG: OBJ TTL = %v\n", ttl)
	var wg sync.WaitGroup
	digits := int(1 + math.Floor(math.Log10(float64(*n))))
	namefmt := fmt.Sprintf("%%s-%%0%dd", digits)
	for i := 1; i <= *n; i++ {
		time.Sleep(nextTime(randGen, *lambda))
		wg.Add(1)
		go func(objnum int) {
			defer wg.Done()
			objname := fmt.Sprintf(namefmt, *runID, objnum)
			RunObjLifeCycle(clientset, csvFile, objname, ttl)
		}(i)
	}

	fmt.Printf("DEBUG: waiting for objects to clear\n")
	wg.Wait()
	os.Exit(0)

}

/* =========================================== */
/* simulate the lifecycle of a single object   */
/* =========================================== */

func RunObjLifeCycle(clientset *wardleclient.Clientset, csvFile *os.File, objname string, ttl time.Duration) {
	var err error

	/* create the object */
	obj := &wardlev1a1.Flunder{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objname,
			Namespace: namespace,
			Labels:    map[string]string{"purpose": "scaletest"},
		},
	}
	t10 := time.Now()
	_, err = clientset.WardleV1alpha1().Flunders(namespace).Create(context.Background(), obj, metav1.CreateOptions{FieldManager: "flunder-driver"})
	t1f := time.Now()
	writelog("create", obj.Name, t10, t1f, csvFile, err)

	/* ttl is the target lifetime of the object */
	time.Sleep(ttl)

	/* delete the object */
	delopts := metav1.DeleteOptions{}
	t20 := time.Now()
	err = clientset.WardleV1alpha1().Flunders(namespace).Delete(context.Background(), obj.Name, delopts)
	t2f := time.Now()
	writelog("delete", obj.Name, t20, t2f, csvFile, err)
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
	klog.V(4).Infof("*rest.Config = %#v", *restConfig)
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

func nextTime(randGen *rand.Rand, rateParameter float64) time.Duration {
	nextTimeSeconds := randGen.ExpFloat64() / rateParameter
	if nextTimeSeconds > 300 {
		nextTimeSeconds = 300
	}
	return time.Duration(nextTimeSeconds * float64(time.Second))
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
