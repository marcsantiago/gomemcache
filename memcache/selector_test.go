/*
Copyright 2014 The gomemcache AUTHORS

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

package memcache

import (
	"math"
	"net"
	"strconv"
	"testing"
)

func TestHashRing_GetTargetNode(t *testing.T) {
	t.Parallel()
	type fields struct {
		nodes []string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		iteration int
		want      string
		wantErr   bool
	}{
		{
			name:      "Test GetTargetNode",
			fields:    fields{nodes: []string{"1", "2", "3", "4"}},
			args:      args{key: "id_d5d25b3b-5acc-49fb-8cc7-0798ceeece69"},
			iteration: 1_000_000,
			want:      "1",
			wantErr:   false,
		},
		{
			name:      "Test GetTargetNode",
			fields:    fields{nodes: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}},
			args:      args{key: "id_ced5c816-f8a8-4f6e-bcc7-61472f099857"},
			iteration: 1_000_000,
			want:      "1",
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ss ServerList
			var addresses []net.Addr
			for _, node := range tt.fields.nodes {
				addresses = append(addresses, &staticAddr{ntw: "tcp", str: node})
			}
			ss.addresses = addresses
			for i := 0; i < tt.iteration; i++ {
				got, err := ss.PickServer(tt.args.key)
				if (err != nil) != tt.wantErr {
					t.Fatalf("PickServer() error = %v, wantErr %v", err, tt.wantErr)
				}
				if got.String() != tt.want {
					t.Fatalf("PickServer() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestHashRing_Distribution(t *testing.T) {
	t.Parallel()
	replicas := []string{"1", "2", "3", "4"}
	var ss ServerList
	var addresses []net.Addr
	for _, node := range replicas {
		addresses = append(addresses, &staticAddr{ntw: "tcp", str: node})
	}
	ss.addresses = addresses

	iter := 1_000_000
	distributionMap := make(map[string]int)
	for i := 0; i < iter; i++ {
		targetID := "id_" + strconv.Itoa(i)
		node, _ := ss.PickServer(targetID)
		distributionMap[node.String()]++
	}

	if len(distributionMap) != len(replicas) {
		t.Fatalf("PickServer() got = %v, want %v", len(distributionMap), len(replicas))
	}

	tolerance := 0.01
	expected := 0.25
	for _, node := range replicas {
		count := distributionMap[node]
		percentage := float64(count) / float64(iter)
		if !WithinTolerance(expected, percentage, tolerance) {
			t.Fatalf("PickServer() got = %v, want %v, tolerance %v, got %v", percentage, 0.25, tolerance, percentage-expected)
		}
	}
}

func BenchmarkPickServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickServer_Single(b *testing.B) {
	benchPickServer(b, "127.0.0.1:1234")
}

func benchPickServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss ServerList
	var addresses []net.Addr
	for _, node := range servers {
		addresses = append(addresses, &staticAddr{ntw: "tcp", str: node})
	}
	ss.addresses = addresses
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickServer("some key"); err != nil {
			b.Fatal(err)
		}
	}
}

func WithinTolerance(expected, got, tolerance float64) bool {
	if expected == got {
		return true
	}
	d := math.Abs(expected - got)
	if got == 0 {
		return d < tolerance
	}
	return (d / math.Abs(got)) < tolerance
}
