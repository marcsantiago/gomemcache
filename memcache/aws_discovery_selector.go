package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// AWS documentation: https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.html
// https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoDiscovery.AddingToYourClientLibrary.html

var (
	// ErrAutoDiscover is returned when no nodes are found in the configuration.
	ErrAutoDiscover   = errors.New("memcache: no nodes found in the configuration")
	ErrInvalidCommand = errors.New("memcache: error in response")
)

type Discovery struct {
	clusterAddress string
	nodes          []string
	pollInterval   time.Duration

	mu         sync.RWMutex
	serverList *ServerList
}

// NewAWSDiscoverySelector creates a new Discovery designed to work with the AWS AutoDiscovery feature.
func NewAWSDiscoverySelector(clusterAddress string, options ...func(*Discovery)) *Discovery {
	d := &Discovery{
		clusterAddress: clusterAddress,
	}

	d.pollInterval = time.Hour
	for _, option := range options {
		option(d)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(d.pollInterval)
	go func() {
		_ = d.discover()
		for {
			select {
			case <-c:
				ticker.Stop()
			case <-ticker.C:
				_ = d.discover()
			}
		}
	}()
	return d
}

// WithCustomPollInterval sets the poll interval for the Discovery. Default is 1 hour.
func WithCustomPollInterval(interval time.Duration) func(*Discovery) {
	return func(d *Discovery) {
		d.pollInterval = interval
	}
}

// discover fetches the list of nodes from the configuration endpoint.
func (d *Discovery) discover() error {
	connection, errDial := net.Dial("tcp", d.clusterAddress)
	if errDial != nil {
		return errDial
	}
	defer func() {
		_ = connection.Close()
	}()

	rw := bufio.NewReadWriter(bufio.NewReader(connection), bufio.NewWriter(connection))
	_, errFprintf := fmt.Fprintf(rw, "config get cluster\r\n")
	if errFprintf != nil {
		return errFprintf
	}

	if errFlush := rw.Flush(); errFlush != nil {
		return errFlush
	}

	nodes, errParse := parseNodes(rw)
	if errParse != nil {
		return errParse
	}

	if len(nodes) == 0 {
		return ErrAutoDiscover
	}

	replaceNodes := false
	if d.serverList == nil {
		replaceNodes = true
	} else {
		currentNodes := d.serverList.addresses
		for _, node := range nodes {
			if !containsNode(currentNodes, node) {
				replaceNodes = true
				break
			}
		}
	}

	if replaceNodes {
		var serverList ServerList
		err := serverList.SetServers(nodes...)
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.serverList = &serverList
		d.mu.Unlock()
	}
	return nil
}

func parseNodes(r *bufio.ReadWriter) ([]string, error) {
	var nodes []string
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nodes, nil
			}
			return nodes, err
		}

		if bytes.Equal(line, resultEnd) {
			return nodes, nil
		}

		if bytes.Contains(line, []byte("ERROR")) {
			return nodes, ErrInvalidCommand
		}

		if bytes.Contains(line, []byte("amazonaws")) {
			// each node is separated by a space
			rawNodes := bytes.Split(bytes.ReplaceAll(line, []byte(`\n\r\n`), nil), []byte(" "))
			nodes = make([]string, 0, len(rawNodes))
			for i := 0; i < len(rawNodes); i++ {
				// each node has 3 parts: hostname, ip, port
				// hostname|ip-address|port
				config := bytes.Split(rawNodes[i], []byte("|"))
				if len(config) != 3 {
					continue
				}
				nodes = append(nodes, string(config[0])+":"+string(config[2]))
			}
		}
	}
}

func (d *Discovery) PickServer(key string) (net.Addr, error) {
	return d.serverList.PickServer(key)
}

func (d *Discovery) Each(f func(net.Addr) error) error {
	return d.serverList.Each(f)
}

func containsNode(nodes []net.Addr, node string) bool {
	for _, n := range nodes {
		if n.String() == node {
			return true
		}
	}
	return false
}
