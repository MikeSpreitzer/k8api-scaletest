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
	"hash/crc64"
	"math/rand"
	"os"
	"time"

	"github.com/golang/glog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	wardleclient "k8s.io/sample-apiserver/pkg/client/clientset/versioned"
)

func main() {
	var kubeconfig string
	var master string
	var useProtobuf bool
	var namespace string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.BoolVar(&useProtobuf, "useProtobuf", false, "indicates whether to encode objects with protobuf (as opposed to JSON)")
	flag.StringVar(&namespace, "namespace", "", "namespace to watch")
	flag.Set("logtostderr", "true")
	flag.Parse()

	// creates the connection
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		glog.Fatal(err)
	}
	myAddr := GetHostAddr()
	now := time.Now()
	birthmark := fmt.Sprintf("%02d%02d%02d", now.Hour(), now.Minute(), now.Second())
	glog.Infof("Using %q as my host address, %q as my birthmark\n", myAddr, birthmark)
	crcTable := crc64.MakeTable(crc64.ISO)
	crc := int64(crc64.Checksum(([]byte)(myAddr), crcTable))
	rand.Seed(now.UnixNano() + crc)
	rand.Float64()
	rand.Float64()
	config.UserAgent = fmt.Sprintf("flunder-watcher@%s@%s", myAddr, birthmark)

	if useProtobuf {
		config.ContentType = "application/vnd.kubernetes.protobuf"
	}

	// creates the clientset
	clientset, err := wardleclient.NewForConfig(config)
	if err != nil {
		glog.Fatal(err)
	}

	flunderIfc := clientset.WardleV1alpha1().Flunders(namespace)

	for {
		objlist, err := flunderIfc.List(metav1.ListOptions{})
		if err != nil {
			glog.Errorf("List returned error %s\n", err)
			os.Exit(10)
		}
		rv := objlist.ResourceVersion
		var timeout int64 = int64(300 + rand.Intn(300))
		glog.Infof("Watching namespace %q from ResourceVersion %q with myAddr=%q, timeout=%d\n", namespace, rv, myAddr, timeout)
		watch, err := flunderIfc.Watch(metav1.ListOptions{
			ResourceVersion: rv,
			TimeoutSeconds:  &timeout,
		})
		if err != nil {
			glog.Errorf("Watch returned error %s\n", err)
			os.Exit(12)
		}
		eventChan := watch.ResultChan()
	EventLoop:
		for {
			select {
			case event, ok := <-eventChan:
				if ok {
					glog.Infof("Got event type=%v, obj=%+v\n", event.Type, event.Object)
				} else {
					glog.Info("Result channel closed\n")
					break EventLoop
				}
			}
		}
	}
}
