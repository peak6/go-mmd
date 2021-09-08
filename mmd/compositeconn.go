package mmd

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"log"
	"net"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CompositeConn struct {
	conns       map[string]*ConnImpl
	mmdConn     *ConnImpl
	cfg         *ConnConfig
	mu          sync.RWMutex
	callTimeout time.Duration
	servers     []*Server
	stopping    *atomic.Bool
}

func (c *CompositeConn) Subscribe(service string, body interface{}) (*Chan, error) {
	conn, err := c.getOrCreateConnection(service)
	if err != nil {
		return nil, err
	}

	return conn.Subscribe(service, body)
}

func (c *CompositeConn) Unsubscribe(cid ChannelId, body interface{}) error {
	conn := c.getConnectionForChannel(cid)
	if conn != nil {
		return conn.Unsubscribe(cid, body)
	}
	return nil
}

func (c *CompositeConn) Call(service string, body interface{}) (interface{}, error) {
	conn, err := c.getOrCreateConnection(service)
	if err != nil {
		return nil, err
	}

	return conn.Call(service, body)
}

func (c *CompositeConn) CallAuthenticated(service string, token AuthToken, body interface{}) (interface{}, error) {
	conn, err := c.getOrCreateConnection(service)
	if err != nil {
		return nil, err
	}

	return conn.CallAuthenticated(service, token, body)
}

func (c *CompositeConn) SetDefaultCallTimeout(dur time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, conn := range c.conns {
		conn.SetDefaultCallTimeout(dur)
	}
	c.callTimeout = dur
}

func (c *CompositeConn) GetDefaultCallTimeout() time.Duration {
	return c.callTimeout
}

var isIstioService, _ = strconv.ParseBool(os.Getenv("IS_ISTIO_SERVICE"))

func (c *CompositeConn) RegisterLocalService(name string, fn ServiceFunc) error {
	if isIstioService {
		log.Println("Registering direct mmd service " + name)
		return c.registerDirectService(name, fn)
	} else {
		log.Println("Registering brokered mmd service" + name)
		return c.mmdConn.RegisterLocalService(name, fn)
	}
}

func (c *CompositeConn) RegisterService(name string, fn ServiceFunc) error {
	if isIstioService {
		log.Println("Registering direct mmd service" + name)
		return c.registerDirectService(name, fn)
	} else {
		log.Println("Registering brokered mmd service" + name)
		return c.mmdConn.RegisterService(name, fn)
	}
}

func (c *CompositeConn) registerDirectService(service string, fn ServiceFunc) error {
	re := regexp.MustCompile("[.\\\\-]")
	listenPortEnvVar := re.ReplaceAllString(strings.ToUpper(service), "_") + "_LISTEN_PORT"
	envVal, ok := os.LookupEnv(listenPortEnvVar)
	if !ok {
		return fmt.Errorf("listen port for service %s is not configured - must set %s", service, listenPortEnvVar)
	}
	listenPort, err := strconv.Atoi(envVal)
	if err != nil {
		return fmt.Errorf("invalid listen port for service %s: %s. Error: %v", service, envVal, err)
	}

	log.Printf("%s is configured to listen on port %d", service, listenPort)

	server := &Server{
		serviceName: service,
		listenPort:  listenPort,
		cfg:         c.cfg,
		serviceFunc: fn,
		closeChan:   make(chan bool),
	}

	started := make(chan error)
	go server.start(started)
	err = <-started

	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.servers = append(c.servers, server)

	return nil
}

func (c *CompositeConn) createSocketConnection(isRetryConnection bool, notifyOnConnect bool) error {
	c.mmdConn.config.OnDisconnect = c.onDisconnect
	return c.mmdConn.createSocketConnection(isRetryConnection, notifyOnConnect)
}

func (c *CompositeConn) close() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for service, conn := range c.conns {
		conn.cleanupReader()
		conn.cleanupSocket()
		err := conn.close()
		if err != nil {
			log.Println("Error closing direct connection", err)
		}
		delete(c.conns, service)
	}

	err = c.mmdConn.close()
	if err != nil {
		log.Println("Error closing mmd connection", err)
	}

	for _, server := range c.servers {
		err = server.stop()
		if err != nil {
			log.Println("Error stopping mmd server", err)
		}
	}

	return
}

func (c *CompositeConn) onDisconnect() {
	if c.stopping.CAS(false, true) {
		log.Println("Closing and reconnect composite connection")
		c.close()
		if c.cfg.AutoRetry {
			start := time.Now()
			c.createSocketConnection(true, true)
			elapsed := time.Since(start)
			log.Println("Socket reset. Connected to mmd after :", elapsed)
		}
		c.stopping.Store(false)
	}
}

func (c *CompositeConn) getOrCreateConnection(service string) (*ConnImpl, error) {
	log.Println("get or create connection " + service)

	if conn := c.getConnection(service); conn != nil {
		log.Println("Found existing connection for " + service)
		return conn, nil
	} else {
		log.Println("No existing connection found for " + service + ". Creating one")

		accessMethod, err := c.getAccessMethod(service)
		if err != nil {
			return nil, err
		}
		log.Printf("Access method for %s: %d", service, accessMethod)

		return c.createConnection(service, accessMethod)
	}
}

func (c *CompositeConn) getConnection(service string) *ConnImpl {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conns[service]
}

func (c *CompositeConn) createConnection(service string, serviceType mmdAccessMethod) (*ConnImpl, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[service]; ok {
		return conn, nil
	}

	var conn *ConnImpl
	switch serviceType {
	case MMD:
		conn = c.mmdConn
	case ISTIO:
		var err error
		conn, err = c.createAndInitDirectConnection(service)
		if err != nil {
			return nil, err
		}
		log.Println("Successfully initialized direct connection for service " + service)
	}

	c.conns[service] = conn
	return conn, nil
}

const DIRECT_CONNECTION_TIMEOUT_SECONDS = 5

func (c *CompositeConn) createAndInitDirectConnection(service string) (*ConnImpl, error) {
	log.Println("Creating new direct connection for " + service)

	newConfig := *(c.cfg)
	newUrl, err := getServiceUrl(service)
	if err != nil {
		return nil, err
	}
	log.Println("Using service url " + newUrl + " for service " + service)
	newConfig.Url = newUrl

	newConfig.ConnTimeout = DIRECT_CONNECTION_TIMEOUT_SECONDS
	newConfig.OnDisconnect = c.onDisconnect

	conn := createConnection(&newConfig)

	err = conn.createSocketConnection(false, false)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

var env = computeEnv()
var nameserverHost = getEnv("NAMESERVER_HOST", "istio-dns."+env+".k8s.peak6.net")
var istioIngressHost = getEnv("ISTIO_INGRESS_HOST", env+".istioingress.peak6.net")
var _, isInK8s = os.LookupEnv("KUBERNETES_SERVICE_HOST")
var useIstioIngress = getEnvBool("USE_ISTIO_INGRESS", !isInK8s)

var envs = map[byte]string{'d': "dev", 's': "stg", 'u': "uat", 'p': "prd"}

func computeEnv() string {
	if env, ok := os.LookupEnv("ENVIRONMENT"); ok {
		return env
	} else {
		hostname, _ := os.Hostname()
		if len(hostname) > 7 && !strings.Contains(hostname, "-") {
			if env, ok := envs[hostname[7]]; ok {
				return env
			}
		}
	}
	return "dev"
}

var serviceToEnvVar = regexp.MustCompile("[.\\\\-]")

func getServiceUrl(service string) (string, error) {
	listenPortEnvVar := serviceToEnvVar.ReplaceAllString(strings.ToUpper(service), "_") + "_URL"
	envVal, ok := os.LookupEnv(listenPortEnvVar)
	if ok {
		log.Println("Found env override for service url for service " + service + ": " + envVal)
		return envVal, nil
	}

	log.Println("Getting service url for " + service)

	k8sServiceName := strings.ReplaceAll(service, ".", "-")
	k8sFqdn := k8sServiceName + ".default.svc.cluster.local"

	var resolver *net.Resolver
	if useIstioIngress {
		log.Println("Using istio ingress, using custom resolver with configured nameserver " + nameserverHost + " for DNS SRV lookup")

		resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: time.Millisecond * time.Duration(10000)}
				return d.DialContext(ctx, network, nameserverHost+":53")
			},
		}
	} else {
		log.Println("Not using istio ingress - using default reachable nameserver for DNS SRV lookup")

		resolver = net.DefaultResolver
	}

	_, addrs, err := resolver.LookupSRV(context.Background(), "tcp-mmd", "tcp", k8sFqdn)
	if err != nil {
		return "", err
	}

	var host string
	if !useIstioIngress {
		host = addrs[0].Target
		log.Println("Not using istio ingress, using resolved address as host: " + host)

		host = addrs[0].Target
	} else {
		log.Println("Using istio ingress as host: " + istioIngressHost)

		host = istioIngressHost
	}

	port := addrs[0].Port

	log.Printf("Resolved port %d", port)

	return fmt.Sprintf("%s:%d", host, port), nil
}

type mmdAccessMethod int

const (
	MMD mmdAccessMethod = iota
	ISTIO
	ERROR
)

const serviceDiscoveryServiceName = "mmd.istio.service.discovery"

var preferMmd = getEnvBool("PREFER_MMD_CONNECTION", false)

func (c *CompositeConn) getAccessMethod(service string) (mmdAccessMethod, error) {
	log.Println("Looking up service type for service " + service)

	re := regexp.MustCompile("[.\\\\-]")
	listenPortEnvVar := re.ReplaceAllString(strings.ToUpper(service), "_") + "_ACCESS_METHOD"
	envVal, ok := os.LookupEnv(listenPortEnvVar)
	if ok {
		log.Println("Found env override for service url for service " + service + ": " + envVal)

		switch envVal {
		case "MMD":
			return MMD, nil
		case "ISTIO":
			return ISTIO, nil
		default:
			return ERROR, fmt.Errorf("Invalid env val override " + envVal + " for service method of service " + service)
		}
	}

	if service == serviceDiscoveryServiceName {
		return ISTIO, nil
	}

	serviceDiscoveryServiceConn, err := c.getOrCreateConnection(serviceDiscoveryServiceName)
	if err != nil {
		return ERROR, err
	}

	resp, err := serviceDiscoveryServiceConn.Call(serviceDiscoveryServiceName, map[string]interface{}{"service": service})
	if err != nil {
		return ERROR, err
	}

	respMap, ok := resp.(map[interface{}]interface{})
	if !ok {
		return ERROR, fmt.Errorf("service discovery service response was not a map: %s", reflect.TypeOf(resp))
	}
	serviceType := respMap[service]

	switch serviceType {
	case "MMD":
		return MMD, nil
	case "ISTIO":
		return ISTIO, nil
	case "BOTH":
		if preferMmd {
			return MMD, nil
		} else {
			return ISTIO, nil
		}
	case "UNKNOWN":
		return MMD, nil
	default:
		return ERROR, fmt.Errorf("unrecognized service discovery service response: %s", serviceType)
	}
}

func (c *CompositeConn) getConnectionForChannel(cid ChannelId) *ConnImpl {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, conn := range c.conns {
		if _, ok := conn.dispatch[cid]; ok {
			return conn
		}
	}
	return nil
}

func (c *CompositeConn) String() string {
	return fmt.Sprint("mmdConn=", c.mmdConn.String(), ", conns=", c.conns)
}
