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

// Forwarder manages the connection from a logging source to an oklog ingester
// Forwarder uses a Scanner (optionally a bufio.Scanner) to send individual messages over the wire.
// TODO add a cancellation method (using context.Context to interrupt forwarding)
type Forwarder struct {
	URLs           []*url.URL
	Prefix         string
	Logger         log.Logger
	Disconnects    Counter
	ShortWrites    Counter
	ForwardBytes   Counter
	ForwardRecords Counter
	Scanner        func(r io.Reader) textScanner
}

// Blocking Forwarder will apply backpressure whilst an onward connection is unavailable, which is is the default behaviour for `oklog forward`.
// Messages will never be dropped.
func NewBlockingForwarder(urls []*url.URL, prefix string) *Forwarder {
	textScannerFunc := func(r io.Reader) textScanner {
		s := bufio.NewScanner(r)
		return s
	}
	return &Forwarder{
		URLs:           urls,
		Prefix:         prefix,
		Disconnects:    NoOpCounter{},
		ShortWrites:    NoOpCounter{},
		ForwardBytes:   NoOpCounter{},
		ForwardRecords: NoOpCounter{},
		Scanner:        textScannerFunc,
	}
}

// Buffered Forwarder will buffer records whilst an onward connection is unavailable. Once the buffer is full, further messages are dropped.
// bufferSize refers to the maximum number of log messages (rather than e.g. bytes) in the buffer
func NewBufferedForwarder(urls []*url.URL, prefix string, bufferSize int) *Forwarder {
	textScannerFunc := func(r io.Reader) textScanner {
		rb := NewBufferedScanner(NewRingBuffer(bufferSize))
		go rb.Consume(r)
		return rb
	}
	return &Forwarder{
		URLs:           urls,
		Prefix:         prefix,
		Disconnects:    NoOpCounter{},
		ShortWrites:    NoOpCounter{},
		ForwardBytes:   NoOpCounter{},
		ForwardRecords: NoOpCounter{},
		Scanner:        textScannerFunc,
	}
}

func (f *Forwarder) Forward(r io.Reader) error {
	logger := f.Logger
	if logger == nil {
		w := log.NewSyncWriter(os.Stderr)
		logger = log.NewLogfmtLogger(w)
	}

	// Build a scanner for the input, and the last record we scanned.
	// These both outlive any individual connection to an ingester.
	var (
		s       textScanner
		backoff = time.Duration(0)
	)
	s = f.Scanner(r)
	// Enter the connect and forward loop. We do this forever.
	for ; ; f.URLs = append(f.URLs[1:], f.URLs[0]) { // rotate thru URLs
		// We gonna try to connect to this first one.
		target := f.URLs[0]

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
		level.Debug(logger).Log("raw_target", f.URLs[0].String(), "resolved_target", target.String())

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
				f.Disconnects.Inc()
				level.Warn(logger)
				level.Warn(logger).Log("disconnected_from", target.String(), "due_to", err)
				break
			} else if n < len(record)+1 {
				f.ShortWrites.Inc()
				level.Warn(logger).Log("short_write_to", target.String(), "n", n, "less_than", len(record)+1)
				break // TODO(pb): we should do something more sophisticated here
			}

			// Only once the write succeeds do we scan the next record.
			backoff = 0 // reset the backoff on a successful write
			f.ForwardBytes.Add(float64(len(record)) + 1)
			f.ForwardRecords.Inc()
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

// textScanner models bufio.Scanner, so we can provide
// an alternate ringbuffered implementation.
type textScanner interface {
	Scan() bool
	Text() string
	Err() error
}

// Count or add several values to a given counter. This could be a prometheus Counter
type Counter interface {
	Inc()
	Add(float64)
}

type NoOpCounter struct {
}

func (c NoOpCounter) Inc() {
}
func (c NoOpCounter) Add(i float64) {
}
