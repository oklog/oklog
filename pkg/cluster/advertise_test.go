package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/go-kit/kit/log"
)

func TestCalculateAdvertiseAddr(t *testing.T) {
	r := mockResolver{
		"validhost.com": []net.IPAddr{{IP: net.ParseIP("10.21.32.43")}},
		"multihost.com": []net.IPAddr{{IP: net.ParseIP("10.1.0.1")}, {IP: net.ParseIP("10.1.0.2")}},
	}
	for _, testcase := range []struct {
		name          string
		bindAddr      string
		advertiseAddr string
		want          string
	}{
		{"Public bind no advertise",
			"1.2.3.4", "", "1.2.3.4",
		},
		{"Private bind no advertise",
			"10.1.2.3", "", "10.1.2.3",
		},
		{"Zeroes bind public advertise",
			"0.0.0.0", "2.3.4.5", "2.3.4.5",
		},
		{"Zeroes bind private advertise",
			"0.0.0.0", "172.16.1.9", "172.16.1.9",
		},
		{"Public bind private advertise",
			"188.177.166.155", "10.11.12.13", "10.11.12.13",
		},
		{"IPv6 bind no advertise",
			"::", "", "::",
		},
		{"IPv6 bind private advertise",
			"::", "172.16.1.1", "172.16.1.1",
		},
		{"Valid hostname as bind addr",
			"validhost.com", "", "10.21.32.43",
		},
		{"Valid hostname as advertise addr",
			"0.0.0.0", "validhost.com", "10.21.32.43",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			ip, err := CalculateAdvertiseIP(testcase.bindAddr, testcase.advertiseAddr, r, log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			if want, have := testcase.want, ip.String(); want != have {
				t.Fatalf("want '%s', have '%s'", want, have)
			}
		})
	}
}

type mockResolver map[string][]net.IPAddr

func (r mockResolver) LookupIPAddr(_ context.Context, address string) ([]net.IPAddr, error) {
	if ips, ok := r[address]; ok {
		return ips, nil
	}
	return nil, fmt.Errorf("address not found")
}
