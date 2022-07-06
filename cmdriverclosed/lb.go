package main

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// GetEndpoints returns a slice of "scheme://addr:port", using the first port
// (if any) in each EndpointSubset that has the requested protocol.
// If the address is IPv6 then it gets surrounded by square brackets.
func GetEndpoints(clientset *kubeclient.Clientset, serviceName, serviceNS, protocol, scheme string) ([]string, error) {
	eps, err := clientset.CoreV1().Endpoints(serviceNS).Get(context.Background(), serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Endpoints named %q in namespace %q: %s", serviceName, serviceNS, err)
	}
	ans := make([]string, 0)
	for _, es := range eps.Subsets {
		var port int32
		for _, ep := range es.Ports {
			if string(ep.Protocol) == protocol {
				port = ep.Port
				break
			}
		}
		if port == 0 {
			continue
		}
		for _, addr := range es.Addresses {
			if strings.Contains(addr.IP, ":") {
				ans = append(ans, fmt.Sprintf("%s://[%s]:%d", scheme, addr.IP, port))
			} else {
				ans = append(ans, fmt.Sprintf("%s://%s:%d", scheme, addr.IP, port))
			}
		}
	}
	return ans, nil
}

func ClientsetsForEndpoints(restConfig *rest.Config, endpoints []string) (ans []*kubeclient.Clientset, err error) {
	ans = make([]*kubeclient.Clientset, len(endpoints))
	for idx, ep := range endpoints {
		config := new(rest.Config)
		*config = *restConfig
		config.Host = ep
		ans[idx], err = kubeclient.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failure making clientset for host %s", config.Host)
		}
	}
	return
}
