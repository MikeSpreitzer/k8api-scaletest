package main

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	mathrand "math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"github.com/golang/glog"
)

var logPeriod time.Duration

type Counter struct {
	name            string
	mutex           sync.Mutex
	count           uint64
	lastLoggedCount uint64
	lastLoggedTime  time.Time
}

func NewCounter(name string) Counter {
	return Counter{
		name:           name,
		lastLoggedTime: time.Now(),
	}
}

func (c *Counter) Add(d uint64) {
	c.mutex.Lock()
	c.count += d
	now := time.Now()
	dt := now.Sub(c.lastLoggedTime)
	var curCount, dCount uint64
	var lastTime time.Time
	dolog := dt >= logPeriod
	if dolog {
		lastTime = c.lastLoggedTime
		curCount = c.count
		dCount = c.count - c.lastLoggedCount
		c.lastLoggedCount = c.count
		c.lastLoggedTime = now
	}
	c.mutex.Unlock()
	if dolog {
		dTime := now.Sub(lastTime)
		glog.V(2).Infof("%s = %d, %g/sec since %s\n", c.name, curCount, float64(dCount)/(float64(dTime)/float64(time.Second)), lastTime)
	}
}

var (
	clientBytesCounter = NewCounter("clientBytes")
	serverBytesCounter = NewCounter("serverBytes")
)

func runClients(urlMid string, nConnections, nThreads, blocksPerRequest int) {
	clients := make([]http.Client, nConnections)
	for i := range clients {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		http2.ConfigureTransport(tr)
		clients[i] = http.Client{Transport: tr}
	}
	for i := 0; i < nThreads; i++ {
		go runThread(urlMid, &clients[i%nConnections], blocksPerRequest)
	}
}

func runThread(urlMid string, client *http.Client, blocksPerRequest int) {
	var buffer [1024]byte
	for {
		blocksThisRequest := blocksPerRequest + mathrand.Intn(blocksPerRequest)
		url := fmt.Sprintf("%s&n=%d", urlMid, blocksThisRequest)
		glog.V(4).Infof("Requesting %q\n", url)
		resp, err := client.Get(url)
		if err != nil {
			glog.Errorf("Request of %q got error %#v\n", url, err)
			return
		}
		readAndClose(resp.Body, buffer[:])
	}
}

func readAndClose(r io.ReadCloser, buffer []byte) {
	defer r.Close()
	nBytes := uint64(0)
	nReads := 0
	for {
		n, err := r.Read(buffer)
		var nu uint64
		if n >= 0 {
			nu = uint64(n)
		}
		nBytes += nu
		nReads += 1
		if err != nil {
			break
		}
	}
	glog.V(3).Infof("Got %d bytes in %d reads\n", nBytes, nReads)
	clientBytesCounter.Add(nBytes)
}

func serve(listenPort int) {
	cert, err := selfSignedCert()
	if err != nil {
		glog.Error(err)
		return
	}
	tempDir := os.TempDir()
	keyLogFilename := filepath.Join(tempDir, fmt.Sprintf("%d.keys", listenPort))
	glog.Infof("Saving TLS master keys to %s\n", keyLogFilename)
	keyLogFile, err := os.Create(keyLogFilename)
	if err != nil {
		glog.Error(err)
		return
	}
	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		KeyLogWriter: keyLogFile,
	}
	server := &http.Server{
		Addr:      fmt.Sprintf(":%d", listenPort),
		TLSConfig: serverTLSConfig,
	}
	http2.ConfigureServer(server, &http2.Server{
		MaxConcurrentStreams: 10000,
	})
	http.HandleFunc("/blocks", handleBlocks)
	err = server.ListenAndServeTLS("", "")
	glog.Fatal(err)
}

func handleBlocks(resp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		glog.Error(err)
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	blockSizeStrs := req.Form["size"]
	nBlocksStrs := req.Form["n"]
	periodMSStrs := req.Form["periodMS"]
	if len(blockSizeStrs) != 1 || len(nBlocksStrs) != 1 || len(periodMSStrs) != 1 {
		glog.Error(err)
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	blockSize, err := strconv.Atoi(blockSizeStrs[0])
	if err != nil {
		glog.Error(err)
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	nBlocks, err := strconv.Atoi(nBlocksStrs[0])
	if err != nil {
		glog.Error(err)
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	periodMS, err := strconv.ParseInt(periodMSStrs[0], 10, 64)
	if err != nil {
		glog.Error(err)
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	var dt time.Duration = time.Duration(periodMS) * time.Millisecond
	block := make([]byte, blockSize)
	resp.WriteHeader(http.StatusOK)
	block[0] = 65
	block[blockSize-1] = 90
	t0 := time.Now()
	tNext := t0
	for i := 0; i < nBlocks; i++ {
		nw, err := resp.Write(block)
		if err != nil {
			glog.Error(err)
			return
		}
		now := time.Now()
		tNext = tNext.Add(dt)
		glog.V(5).Infof("Wrote %d; tNext=%s, now=%s\n", nw, tNext, now)
		if now.Before(tNext) {
			d := tNext.Sub(now)
			time.Sleep(d)
		}
	}
	if glog.V(3) {
		tf := time.Now()
		glog.Infof("Wrote %d blocks of %d in %s\n", nBlocks, blockSize, tf.Sub(t0))
	}
	serverBytesCounter.Add(uint64(nBlocks) * uint64(blockSize))
}

func selfSignedCert() (cert tls.Certificate, err error) {
	privateKey, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	if err != nil {
		return
	}
	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)
	cert_tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1701),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	derBytes, err := x509.CreateCertificate(cryptorand.Reader, &cert_tmpl, &cert_tmpl, &privateKey.PublicKey, privateKey)
	var certPEMBuf bytes.Buffer
	var keyPEMBuf bytes.Buffer
	err = pem.Encode(&certPEMBuf, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err != nil {
		return
	}
	err = pem.Encode(&keyPEMBuf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err != nil {
		return
	}
	cert, err = tls.X509KeyPair(certPEMBuf.Bytes(), keyPEMBuf.Bytes())
	return
}

func main() {
	var listenPort int
	var baseURL string
	var nConnections int
	var blockSize int
	var periodSecs float64
	var blocksPerRequest int
	var nThreads int
	flag.IntVar(&listenPort, "listen", 8080, "port at which to listen")
	flag.StringVar(&baseURL, "base-url", "", "base URL to request; e.g., `https://host:post`; implies client")
	flag.IntVar(&nConnections, "connections", 1, "number of connections to open when client")
	flag.IntVar(&blockSize, "block-size", 500, "block size to request")
	flag.Float64Var(&periodSecs, "period", 0.01, "block transmission period to request if client")
	flag.IntVar(&blocksPerRequest, "blocks-per-request", 2001, "number of blocks per request")
	flag.IntVar(&nThreads, "threads", 1, "number of threads making requests")
	flag.DurationVar(&logPeriod, "log-period", time.Minute, "period of grand total logging")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: server or client for go http perf test")
		flag.PrintDefaults()
	}
	flag.Set("logtostderr", "true")
	flag.Parse()
	go serve(listenPort)
	if baseURL != "" {
		periodMS := int(periodSecs*1000 + 0.5)
		urlMid := fmt.Sprintf("%s/blocks?size=%d&periodMS=%d", baseURL, blockSize, periodMS)
		glog.Infof("urlMid=%q\n", urlMid)
		runClients(urlMid, nConnections, nThreads, blocksPerRequest)
	}
	select {}
}
