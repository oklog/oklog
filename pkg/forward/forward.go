package forward

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Count or add several values to a given counter. This could be a prometheus Counter
type Counter interface {
	Inc()
	Add(float64)
}

type Forwarder struct {
	Prefix         string
	Logger         log.Logger
	Backpressure   string
	BufferSize     int
	Disconnects    Counter
	ShortWrites    Counter
	ForwardBytes   Counter
	ForwardRecords Counter
}

func NewForwarder(prefix string, backpressure string) *Forwarder {
	return &Forwarder{
		Prefix:       prefix,
		Backpressure: backpressure,
		BufferSize:   1024, // default
	}
}

func (f *Forwarder) Forward(r io.Reader, urls []*url.URL) error {
	disconnects := f.Disconnects
	shortWrites := f.ShortWrites
	forwardBytes := f.ForwardBytes
	forwardRecords := f.ForwardRecords
	logger := f.Logger
	if logger == nil {
		w := log.NewSyncWriter(os.Stderr)
		logger = log.NewLogfmtLogger(w)
	}
	backpressure := f.Backpressure

	// textScanner models bufio.Scanner, so we can provide
	// an alternate ringbuffered implementation.
	type textScanner interface {
		Scan() bool
		Text() string
		Err() error
	}
	// Build a scanner for the input, and the last record we scanned.
	// These both outlive any individual connection to an ingester.
	var (
		s       textScanner
		backoff = time.Duration(0)
	)
	switch strings.ToLower(backpressure) {
	case "block":
		s = bufio.NewScanner(r)
	case "buffer":
		rb := NewBufferedScanner(NewRingBuffer(f.BufferSize))
		go rb.Consume(r)
		s = rb
	default:
		level.Error(logger).Log("backpressure", backpressure, "err", "invalid backpressure option")
		os.Exit(1)
	}
	// Enter the connect and forward loop. We do this forever.
	for ; ; urls = append(urls[1:], urls[0]) { // rotate thru URLs
		// We gonna try to connect to this first one.
		target := urls[0]

		host, port, err := net.SplitHostPort(target.Host)
		if err != nil {
			return errors.Wrapf(err, "unexpected error")
		}

		// Support e.g. "tcp+dnssrv://host:port"
		fields := strings.SplitN(target.Scheme, "+", 2)
		if len(fields) == 2 {
			proto, suffix := fields[0], fields[1]
			switch suffix {
			case "dns", "dnsip":
				ips, err := net.LookupIP(host)
				if err != nil {
					level.Warn(logger).Log("LookupIP", host, "err", err)
					backoff = exponential(backoff)
					time.Sleep(backoff)
					continue
				}
				host = ips[rand.Intn(len(ips))].String()
				target.Scheme, target.Host = proto, net.JoinHostPort(host, port)

			case "dnssrv":
				_, records, err := net.LookupSRV("", proto, host)
				if err != nil {
					level.Warn(logger).Log("LookupSRV", host, "err", err)
					backoff = exponential(backoff)
					time.Sleep(backoff)
					continue
				}
				host = records[rand.Intn(len(records))].Target
				target.Scheme, target.Host = proto, net.JoinHostPort(host, port) // TODO(pb): take port from SRV record?

			case "dnsaddr":
				names, err := net.LookupAddr(host)
				if err != nil {
					level.Warn(logger).Log("LookupAddr", host, "err", err)
					backoff = exponential(backoff)
					time.Sleep(backoff)
					continue
				}
				host = names[rand.Intn(len(names))]
				target.Scheme, target.Host = proto, net.JoinHostPort(host, port)

			default:
				level.Warn(logger).Log("unsupported_scheme_suffix", suffix, "using", proto)
				target.Scheme = proto // target.Host stays the same
			}
		}
		level.Debug(logger).Log("raw_target", urls[0].String(), "resolved_target", target.String())

		conn, err := net.Dial(target.Scheme, target.Host)
		if err != nil {
			level.Warn(logger).Log("Dial", target.String(), "err", err)
			backoff = exponential(backoff)
			time.Sleep(backoff)
			continue
		}

		ok := s.Scan()
		for ok {
			// We enter the loop wanting to write s.Text() to the conn.
			record := s.Text()
			if n, err := fmt.Fprintf(conn, "%s%s\n", f.Prefix, record); err != nil {
				disconnects.Inc()
				level.Warn(logger).Log("disconnected_from", target.String(), "due_to", err)
				break
			} else if n < len(record)+1 {
				shortWrites.Inc()
				level.Warn(logger).Log("short_write_to", target.String(), "n", n, "less_than", len(record)+1)
				break // TODO(pb): we should do something more sophisticated here
			}

			// Only once the write succeeds do we scan the next record.
			backoff = 0 // reset the backoff on a successful write
			forwardBytes.Add(float64(len(record)) + 1)
			forwardRecords.Inc()
			ok = s.Scan()
		}
		if !ok {
			level.Info(logger).Log("stdin", "exhausted", "due_to", s.Err())
			return nil
		}
	}

}

func exponential(d time.Duration) time.Duration {
	const (
		min = 16 * time.Millisecond
		max = 1024 * time.Millisecond
	)
	d *= 2
	if d < min {
		d = min
	}
	if d > max {
		d = max
	}
	return d
}
