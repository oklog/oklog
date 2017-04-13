package main

import (
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
	"github.com/oklog/oklog/pkg/forward"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

func runForward(args []string) error {
	flagset := flag.NewFlagSet("forward", flag.ExitOnError)
	var (
		debug        = flagset.Bool("debug", false, "debug logging")
		apiAddr      = flagset.String("api", "", "listen address for forward API (and metrics)")
		prefixes     = stringslice{}
		backpressure = flagset.String("backpressure", "block", "block, buffer")
		bufferSize   = flagset.Int("buffer-size", 1024, "in -backpressure=buffer mode, ringbuffer size in records")
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

	return forward.Forward(os.Stdin, urls, prefix, logger, *backpressure, *bufferSize, disconnects, shortWrites, forwardBytes, forwardRecords)
}
