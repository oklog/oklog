package cluster

import (
	"context"
	"fmt"
	"net"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/go-sockaddr"
	"github.com/pkg/errors"
)

// Resolver models net.DefaultResolver.
type Resolver interface {
	LookupIPAddr(ctx context.Context, address string) ([]net.IPAddr, error)
}

// CalculateAdvertiseIP deduces the best IP on which to advertise our API
// based on a user-provided bind host and advertise host. This code lifts
// some logic out of the memberlist internals, and augments it with extra
// logic to resolve hostnames. (Memberlist demands pure IPs.)
func CalculateAdvertiseIP(bindHost, advertiseHost string, resolver Resolver, logger log.Logger) (net.IP, error) {
	// Prefer advertise host, if it's given.
	if advertiseHost != "" {
		// Best case: parse a plain IP.
		if ip := net.ParseIP(advertiseHost); ip != nil {
			if ip4 := ip.To4(); ip4 != nil {
				ip = ip4
			}
			return ip, nil
		}

		// Otherwise, try to resolve it as if it's a hostname.
		ips, err := resolver.LookupIPAddr(context.Background(), advertiseHost)
		if err == nil && len(ips) == 1 {
			if ip4 := ips[0].IP.To4(); ip4 != nil {
				ips[0].IP = ip4
			}
			return ips[0].IP, nil
		}

		// Didn't work, fall back to the bind host.
		if err == nil && len(ips) != 1 {
			err = fmt.Errorf("advertise host '%s' resolved to %d IPs", advertiseHost, len(ips))
		}
		level.Warn(logger).Log("err", err, "msg", "falling back to bind host")
	}

	// If bind host is all-zeroes, try to get a private IP.
	if bindHost == "0.0.0.0" {
		privateIP, err := sockaddr.GetPrivateIP()
		if err != nil {
			return nil, errors.Wrap(err, "failed to deduce private IP from all-zeroes bind address")
		}
		if privateIP == "" {
			return nil, errors.Wrap(err, "no private IP found, and explicit advertise address not provided")
		}
		ip := net.ParseIP(privateIP)
		if ip == nil {
			return nil, errors.Errorf("failed to parse private IP '%s'", privateIP)
		}
		return ip, nil
	}

	// Otherwise, try to parse the bind host as an IP.
	if ip := net.ParseIP(bindHost); ip != nil {
		return ip, nil
	}

	// And finally, try to resolve the bind host.
	ips, err := resolver.LookupIPAddr(context.Background(), bindHost)
	if err == nil && len(ips) == 1 {
		if ip4 := ips[0].IP.To4(); ip4 != nil {
			ips[0].IP = ip4
		}
		return ips[0].IP, nil
	}

	// Didn't work. This time it's fatal.
	if err == nil && len(ips) != 1 {
		err = fmt.Errorf("bind host '%s' resolved to %d IPs", bindHost, len(ips))
	}
	return nil, errors.Wrap(err, "bind host failed to resolve")
}
