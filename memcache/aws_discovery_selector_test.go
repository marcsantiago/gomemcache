package memcache

import (
	"bufio"
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestDiscovery_parseNodes(t *testing.T) {
	message := `CONFIG cluster 0 136\r\n
12\n
myCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211 myCluster.pc4ldq.0002.use1.cache.amazonaws.com|10.80.249.27|11211\n\r\n 
END\r\n`

	var buf bytes.Buffer
	buf.WriteString(message)

	r := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
	nodes, err := parseNodes(r)
	if err != nil {
		t.Fatalf("parseNodes() error = %v, want nil", err)
	}
	expectedNodes := []string{"myCluster.pc4ldq.0001.use1.cache.amazonaws.com:11211", "myCluster.pc4ldq.0002.use1.cache.amazonaws.com:11211"}
	if !reflect.DeepEqual(nodes, expectedNodes) {
		t.Fatalf("parseNodes() got = %v\nwant %v", strings.Join(nodes, ", "), strings.Join(expectedNodes, ", "))
	}
}
