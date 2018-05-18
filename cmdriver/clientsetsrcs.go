package main

import (
	"math/rand"
	"sync"

	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type ClientsetSrc interface {
	WithInterface(func(kubeclient.Interface) error) error
}

type singleClientsetSrc struct {
	ifc kubeclient.Interface
}

func SingleClientsetSrc(ifc kubeclient.Interface) ClientsetSrc {
	return &singleClientsetSrc{ifc}
}

func (scs *singleClientsetSrc) WithInterface(thunk func(kubeclient.Interface) error) error {
	return thunk(scs.ifc)
}

type multiClientsetSrc struct {
	sync.Mutex
	rand *rand.Rand
	ifcs []kubeclient.Interface
}

func MultiClientsetSrc(restConfig *rest.Config, clientset kubeclient.Interface, serviceName, serviceNS, protocol, scheme string, seed int64) (ClientsetSrc, []string, error) {
	eps, err := GetEndpoints(clientset, serviceName, serviceNS, protocol, scheme)
	if err != nil {
		return nil, nil, err
	}
	ifcs, err := ClientsetsForEndpoints(restConfig, eps)
	if err != nil {
		return nil, eps, err
	}
	randSrc := rand.NewSource(seed)
	return &multiClientsetSrc{
		rand: rand.New(randSrc),
		ifcs: ifcs,
	}, eps, nil
}

func (mcs *multiClientsetSrc) WithInterface(thunk func(kubeclient.Interface) error) error {
	var idx int
	func() {
		mcs.Lock()
		defer mcs.Unlock()
		idx = mcs.rand.Intn(len(mcs.ifcs))

	}()
	return thunk(mcs.ifcs[idx])
}
