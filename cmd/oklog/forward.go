package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

func runForward(args []string) error {
	flagset := flag.NewFlagSet("forward", flag.ExitOnError)
	var (
		debug    = flagset.Bool("debug", false, "debug logging")
		apiAddr  = flagset.String("api", "", "listen address for forward API (and metrics)")
		prefixes = stringslice{}
	)
	flagset.Var(&prefixes, "prefix", "prefix annotated on each log record (repeatable)")
	flagset.Usage = usageFor(flagset, "oklog forward [flags] <ingester> [<ingester>...]")
	if err := flagset.Parse(args); err != nil {
		return err
	}
	args = flagset.Args()
	if len(args) <= 0 {
		return errors.New("specify at least one ingest address as an argument")
	}

	// Logging.
	var logger log.Logger
	{
		logLevel := level.AllowInfo()
		if *debug {
			logLevel = level.AllowAll()
		}
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = level.NewFilter(logger, logLevel)
	}

	// Instrumentation.
	forwardBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "forward_bytes_total",
		Help:      "Bytes forwarded.",
	})
	forwardRecords := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "forward_records_total",
		Help:      "Records forwarded.",
	})
	disconnects := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "forward_disconnects",
		Help:      "Number of times forwarder is disconnected from ingester.",
	})
	shortWrites := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "forward_short_writes",
		Help:      "Number of times forwarder performs a short write to the ingester.",
	})
	prometheus.MustRegister(
		forwardBytes,
		forwardRecords,
		disconnects,
		shortWrites,
	)

	// For now, just a quick-and-dirty metrics server.
	if *apiAddr != "" {
		apiNetwork, apiAddress, _, _, err := parseAddr(*apiAddr, defaultAPIPort)
		if err != nil {
			return err
		}
		apiListener, err := net.Listen(apiNetwork, apiAddress)
		if err != nil {
			return err
		}
		go func() {
			mux := http.NewServeMux()
			registerMetrics(mux)
			registerProfile(mux)
			registerHealthCheck(mux)
			panic(http.Serve(apiListener, mux))
		}()
	}

	// Parse URLs for forwarders.
	var urls []*url.URL
	for _, addr := range args {
		schema, host, _, _, err := parseAddr(addr, defaultFastPort)
		if err != nil {
			return errors.Wrap(err, "parsing ingest address")
		}
		u, err := url.Parse(fmt.Sprintf("%s://%s", schema, host))
		if err != nil {
			return errors.Wrap(err, "parsing ingest URL")
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return errors.Wrapf(err, "couldn't split host:port")
		}
		urls = append(urls, u)
	}

	// Construct the prefix expression.
	var prefix string
	if len(prefixes) > 0 {
		prefix = strings.Join(prefixes, " ") + " "
	}

	// Shuffle the order.
	rand.Seed(time.Now().UnixNano())
	for i := range urls {
		j := rand.Intn(i + 1)
		urls[i], urls[j] = urls[j], urls[i]
	}

	// Build a scanner for the input, and the last record we scanned.
	// These both outlive any individual connection to an ingester.
	// TODO(pb): have flag for backpressure vs. drop
	var (
		s       = bufio.NewScanner(os.Stdin)
		backoff = time.Duration(0)
	)

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
			record := fmt.Sprintf("%s%s\n", prefix, s.Text())
			if n, err := fmt.Fprintf(conn, record); err != nil {
				disconnects.Inc()
				level.Warn(logger).Log("disconnected_from", target.String(), "due_to", err)
				break
			} else if n < len(record) {
				shortWrites.Inc()
				level.Warn(logger).Log("short_write_to", target.String(), "n", n, "less_than", len(record))
				break // TODO(pb): we should do something more sophisticated here
			}

			// Only once the write succeeds do we scan the next record.
			backoff = 0 // reset the backoff on a successful write
			forwardBytes.Add(float64(len(record)))
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
