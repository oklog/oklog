package cluster

import (
	"net"

	"github.com/hashicorp/go-sockaddr"
	"github.com/pkg/errors"
)

// CalculateAdvertiseAddress attempts to clone logic from deep within memberlist
// (NetTransport.FinalAdvertiseAddr) in order to surface its conclusions to the
// application, so we can provide more actionable error messages if the user has
// inadvertantly misconfigured their cluster. Note that the output from this
// function is only ever logged, it's not used as part of program execution.
//
// We do a bit more work here than memberlist; namely, if the addrs are given as
// hostnames, we try to resolve them to IPs. This is so we don't log potentially
// confusing messages, like "couldn't parse IP" when a hostname is given and
// would actually work just fine.
//
// https://github.com/hashicorp/memberlist/blob/022f081/net_transport.go#L126
func CalculateAdvertiseAddress(bindAddr, advertiseAddr string) (net.IP, error) {
	if advertiseAddr != "" {
		// First try to parse as IP.
		ip := net.ParseIP(advertiseAddr)
		if ip != nil {
			if ip4 := ip.To4(); ip4 != nil {
				ip = ip4
			}
			return ip, nil
		}

		// If that fails, try to resolve as hostname.
		resolved, err := net.ResolveIPAddr("ip", advertiseAddr)
		if err == nil {
			if ip4 := resolved.IP.To4(); ip4 != nil {
				resolved.IP = ip4
			}
			return resolved.IP, nil
		}

		return nil, errors.Errorf("failed to parse advertise addr '%s'", advertiseAddr)
	}

	// No explicit advertise address, try to deduce one from bind address.
	if bindAddr == "0.0.0.0" {
		privateIP, err := sockaddr.GetPrivateIP()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get private IP")
		}
		if privateIP == "" {
			return nil, errors.Wrap(err, "no private IP found, explicit advertise addr not provided")
		}
		ip := net.ParseIP(privateIP)
		if ip == nil {
			return nil, errors.Errorf("failed to parse private IP '%s'", privateIP)
		}
		return ip, nil
	}

	// Try to parse the addr as an IP.
	ip := net.ParseIP(bindAddr)
	if ip != nil {
		return ip, nil
	}

	// If that fails, try to resolve as hostname,
	resolved, err := net.ResolveIPAddr("ip", bindAddr)
	if err == nil {
		if ip4 := resolved.IP.To4(); ip4 != nil {
			resolved.IP = ip4
		}
		return resolved.IP, nil
	}

	return nil, errors.Errorf("failed to parse bind addr '%s'", bindAddr)
}
