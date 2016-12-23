package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/experimental_level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/oklog/prototype/pkg/cluster"
	"github.com/oklog/prototype/pkg/group"
	"github.com/oklog/prototype/pkg/ingest"
	"github.com/oklog/prototype/pkg/store"
	"github.com/oklog/ulid"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s <mode> [flags]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "MODES\n")
	fmt.Fprintf(os.Stderr, "  forward      Forwarding agent\n")
	fmt.Fprintf(os.Stderr, "  ingest       Ingester node\n")
	fmt.Fprintf(os.Stderr, "  store        Storage node\n")
	fmt.Fprintf(os.Stderr, "  ingeststore  Combination ingest+store node, for small installations\n")
	fmt.Fprintf(os.Stderr, "  query        Querying commandline tool\n")
	fmt.Fprintf(os.Stderr, "\n")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var run func([]string) error
	switch strings.ToLower(os.Args[1]) {
	case "forward":
		run = runForward
	case "ingest":
		run = runIngest
	case "store":
		run = runStore
	case "ingeststore":
		run = runIngestStore
	case "query":
		run = runQuery
	default:
		usage()
		os.Exit(1)
	}

	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func runForward(args []string) error {
	if len(args) <= 0 {
		return errors.New("specify at least one ingest address as an argument")
	}

	// Logging.
	var logger log.Logger
	logger = log.NewLogfmtLogger(os.Stderr)
	logger = log.NewContext(logger).With("ts", log.DefaultTimestampUTC)
	logger = level.New(logger, level.Config{Allowed: level.AllowAll()})

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
	// TODO(pb): gotta mount the API somewhere

	// Parse all the addresses into URLs.
	var urls []*url.URL
	for _, addr := range args {
		u, err := url.Parse(strings.ToLower(addr))
		if err != nil {
			return errors.Wrap(err, "parsing ingest address")
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return errors.Wrapf(err, "host:port portion of ingest address %s", addr)
		}
		urls = append(urls, u)
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
		ok      = s.Scan()
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

		for ok {
			// We enter the loop wanting to write s.Text() to the conn.
			record := s.Text()
			if n, err := fmt.Fprintf(conn, "%s\n", record); err != nil {
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

func runIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	var (
		apiAddr               = fs.String("api", "tcp://0.0.0.0:7650", "listen address for ingest API")
		fastAddr              = fs.String("ingest.fast", "tcp://0.0.0.0:7651", "listen address for fast (async) writes")
		durableAddr           = fs.String("ingest.durable", "tcp://0.0.0.0:7652", "listen address for durable (sync) writes")
		bulkAddr              = fs.String("ingest.bulk", "tcp://0.0.0.0:7653", "listen address for bulk (whole-segment) writes")
		clusterAddr           = fs.String("cluster", "tcp://0.0.0.0:7659", "listen address for cluster")
		ingestPath            = fs.String("ingest.path", filepath.Join("data", "ingest"), "path holding segment files for ingest tier")
		segmentFlushSize      = fs.Int("ingest.segment-flush-size", 25*1024*1024, "flush segments after they grow to this size")
		segmentFlushAge       = fs.Duration("ingest.segment-flush-age", 3*time.Second, "flush segments after they are active for this long")
		segmentPendingTimeout = fs.Duration("ingest.segment-pending-timeout", time.Minute, "pending segments that are claimed but uncommitted are failed after this long")
		clusterPeers          = stringset{}
	)
	fs.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// +-1----------------+   +-3----------+   +-2------+   +-1---------+ +-1----+
	// | Fast listener    |<--| Write      |-->| Writer |-->| IngestLog | | Peer |
	// +------------------+   | handler    |   +--------+   +-----------+ +------+
	// +-1----------------+   |            |                  ^             ^
	// | Durable listener |<--|            |                  |             |
	// +------------------+   |            |                  |             |
	// +-1----------------+   |            |                  |             |
	// | Bulk listener    |<--|            |                  |             |
	// +------------------+   +------------+                  |             |
	// +-1----------------+   +-2----------+                  |             |
	// | API listener     |<--| Ingest API |------------------'             |
	// |                  |   |            |--------------------------------'
	// +------------------+   +------------+

	// Logging.
	var logger log.Logger
	logger = log.NewLogfmtLogger(os.Stderr)
	logger = log.NewContext(logger).With("ts", log.DefaultTimestampUTC)
	logger = level.New(logger, level.Config{Allowed: level.AllowAll()})

	// Instrumentation.
	connectedClients := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "connected_clients",
		Help:      "Number of currently connected clients by modality.",
	}, []string{"modality"})
	clusterDelegateInvocations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "cluster_delegate_invocations_total",
		Help:      "Total invocations of cluster delegate callbacks by method.",
	}, []string{"method"})
	ingestWriterBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_bytes_written_total",
		Help:      "The total number of bytes written.",
	})
	ingestWriterRecords := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_records_written_total",
		Help:      "The total number of records written.",
	})
	ingestWriterSyncs := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_syncs_total",
		Help:      "The number of times an active segment is explicitly fsynced.",
	})
	ingestWriterRotations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_flushes_total",
		Help:      "The number of times an active segment is flushed.",
	}, []string{"reason"})
	segmentState := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "segment_state_transitions",
		Help:      "Segment state transitions, by next state and reason.",
	}, []string{"mark", "due_to"})
	apiDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "api_request_duration_seconds",
		Help:      "API request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})
	prometheus.MustRegister(
		connectedClients,
		clusterDelegateInvocations,
		ingestWriterBytes,
		ingestWriterRecords,
		ingestWriterSyncs,
		ingestWriterRotations,
		segmentState,
		apiDuration,
	)

	// Parse URLs for listeners.
	fastURL, err := url.Parse(strings.ToLower(*fastAddr))
	if err != nil {
		return err
	}
	durableURL, err := url.Parse(strings.ToLower(*durableAddr))
	if err != nil {
		return err
	}
	bulkURL, err := url.Parse(strings.ToLower(*bulkAddr))
	if err != nil {
		return err
	}
	apiURL, err := url.Parse(strings.ToLower(*apiAddr))
	if err != nil {
		return err
	}
	_, apiPortStr, err := net.SplitHostPort(apiURL.Host)
	if err != nil {
		return errors.Wrap(err, "splitting API listen address")
	}
	apiPort, err := strconv.Atoi(apiPortStr)
	if err != nil {
		return errors.Wrap(err, "parsing API port")
	}
	clusterURL, err := url.Parse(strings.ToLower(*clusterAddr))
	if err != nil {
		return err
	}
	clusterHost, clusterPortStr, err := net.SplitHostPort(clusterURL.Host)
	if err != nil {
		return err
	}
	clusterPort, err := strconv.Atoi(clusterPortStr)
	if err != nil {
		return err
	}

	// Bind listeners.
	fastListener, err := net.Listen(fastURL.Scheme, fastURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("fast", fastURL.String())
	durableListener, err := net.Listen(durableURL.Scheme, durableURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("durable", durableURL.String())
	bulkListener, err := net.Listen(bulkURL.Scheme, bulkURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("bulk", bulkURL.String())
	apiListener, err := net.Listen(apiURL.Scheme, apiURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("API", apiURL.String())

	// Create ingest log and its writer.
	ingestLog, err := ingest.NewFileLog(*ingestPath)
	if err != nil {
		return err
	}
	ingestWriter, err := ingest.NewWriter(
		ingestLog,
		*segmentFlushSize,
		*segmentFlushAge,
		ingestWriterBytes,
		ingestWriterRecords,
		ingestWriterSyncs,
		ingestWriterRotations,
	)
	if err != nil {
		return err
	}
	level.Info(logger).Log("ingest_path", *ingestPath)

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterHost, clusterPort,
		clusterPeers.slice(),
		cluster.PeerTypeIngest, apiPort,
		clusterDelegateInvocations,
		log.NewContext(logger).With("component", "cluster"),
	)
	if err != nil {
		return err
	}
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "cluster_size",
		Help:      "Number of peers in the cluster from this node's perspective.",
	}, func() float64 { return float64(peer.ClusterSize()) }))

	// Execution group.
	var g group.Group
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			<-cancel
			ingestWriter.Stop()
			return nil
		}, func(error) {
			close(cancel)
		})
	}
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			<-cancel
			return peer.Leave(time.Second)
		}, func(error) {
			close(cancel)
		})
	}
	{
		entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
		idGen := func() string { return ulid.MustNew(ulid.Now(), entropy).String() } // TODO(pb): could improve efficiency
		g.Add(func() error {
			return ingest.HandleConnections(fastListener, ingestWriter, ingest.HandleFastWriter, idGen, connectedClients.WithLabelValues("fast"))
		}, func(error) {
			fastListener.Close()
		})
		g.Add(func() error {
			return ingest.HandleConnections(durableListener, ingestWriter, ingest.HandleDurableWriter, idGen, connectedClients.WithLabelValues("durable"))
		}, func(error) {
			durableListener.Close()
		})
		g.Add(func() error {
			return ingest.HandleConnections(bulkListener, ingestWriter, ingest.HandleBulkWriter, idGen, connectedClients.WithLabelValues("bulk"))
		}, func(error) {
			bulkListener.Close()
		})
		g.Add(func() error {
			mux := http.NewServeMux()
			mux.Handle("/ingest/", http.StripPrefix("/ingest", ingest.NewAPI(peer, ingestLog, *segmentPendingTimeout, apiDuration, segmentState)))
			mux.Handle("/metrics", promhttp.Handler())
			return http.Serve(apiListener, mux)
		}, func(error) {
			apiListener.Close()
		})
	}
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}
	return g.Run()
}

func runStore(args []string) error {
	fs := flag.NewFlagSet("store", flag.ExitOnError)
	var (
		apiAddr           = fs.String("api", "tcp://0.0.0.0:7650", "listen address for store API")
		clusterAddr       = fs.String("cluster", "tcp://0.0.0.0:7659", "listen address for cluster")
		storePath         = fs.String("store.path", filepath.Join("data", "store"), "path holding segment files for storage tier")
		segmentTargetSize = fs.Int64("store.segment-target-size", 100*1024, "try to keep store segments about this size")
		segmentRetain     = fs.Duration("store.segment-retain", 7*24*time.Hour, "retention period for segment files")
		segmentPurge      = fs.Duration("store.segment-purge", 24*time.Hour, "purge deleted segment files after this long")
		clusterPeers      = stringset{}
	)
	fs.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	//                                    +-1--------+ +-1----+
	//                                    | StoreLog | | Peer |
	//                                    +----------+ +------+
	// +-1------------+   +-2---------+      ^ ^ ^       ^ ^
	// | API listener |<--| Store API |------' | |       | |
	// |              |   |           |------------------' |
	// +--------------+   +-----------+        | |         |
	//                    +-2---------+        | |         |
	//                    | Compacter |--------' |         |
	//                    +-----------+          |         |
	//                    +-2---------+          |         |
	//                    | Consumer  |----------'         |
	//                    |           |--------------------'
	//                    +-----------+

	// Logging.
	var logger log.Logger
	logger = log.NewLogfmtLogger(os.Stderr)
	logger = log.NewContext(logger).With("ts", log.DefaultTimestampUTC)
	logger = level.New(logger, level.Config{Allowed: level.AllowAll()})

	// Instrumentation.
	clusterDelegateInvocations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "cluster_delegate_invocations_total",
		Help:      "Total invocations of cluster delegate callbacks by method.",
	}, []string{"method"})
	apiDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "api_request_duration_seconds",
		Help:      "API request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})
	compactDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "compact_duration_seconds",
		Help:      "Duration of each compaction in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"kind", "compacted", "result"})
	replicateBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "replicate_bytes",
		Help:      "Bytes replicated to this node.",
	})
	trashSegments := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "trash_segments",
		Help:      "Segments moved to trash.",
	}, []string{"success"})
	purgeSegments := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "purge_segments",
		Help:      "Segments purged from trash.",
	}, []string{"success"})
	prometheus.MustRegister(
		clusterDelegateInvocations,
		apiDuration,
		compactDuration,
		replicateBytes,
		trashSegments,
		purgeSegments,
	)

	// Parse URLs for listeners.
	apiURL, err := url.Parse(strings.ToLower(*apiAddr))
	if err != nil {
		return err
	}
	_, apiPortStr, err := net.SplitHostPort(apiURL.Host)
	if err != nil {
		return errors.Wrap(err, "splitting API listen address")
	}
	apiPort, err := strconv.Atoi(apiPortStr)
	if err != nil {
		return errors.Wrap(err, "parsing API port")
	}
	clusterURL, err := url.Parse(strings.ToLower(*clusterAddr))
	if err != nil {
		return err
	}
	clusterHost, clusterPortStr, err := net.SplitHostPort(clusterURL.Host)
	if err != nil {
		return err
	}
	clusterPort, err := strconv.Atoi(clusterPortStr)
	if err != nil {
		return err
	}

	// Bind listeners.
	apiListener, err := net.Listen(apiURL.Scheme, apiURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("API", apiURL.String())

	// Create storelog.
	storeLog, err := store.NewFileLog(*storePath, *segmentTargetSize)
	if err != nil {
		return err
	}
	level.Info(logger).Log("StoreLog", *storePath)

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterHost, clusterPort,
		clusterPeers.slice(),
		cluster.PeerTypeStore, apiPort,
		clusterDelegateInvocations,
		log.NewContext(logger).With("component", "cluster"),
	)
	if err != nil {
		return err
	}
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "cluster_size",
		Help:      "Number of peers in the cluster from this node's perspective.",
	}, func() float64 { return float64(peer.ClusterSize()) }))

	// Execution group.
	var g group.Group
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			<-cancel
			return peer.Leave(time.Second)
		}, func(error) {
			close(cancel)
		})
	}
	{
		c := store.NewConsumer(
			peer,
			storeLog,
			*segmentTargetSize,
			log.NewContext(logger).With("component", "Consumer"),
		)
		g.Add(func() error {
			c.Run()
			return nil
		}, func(error) {
			c.Stop()
		})
	}
	{
		c := store.NewCompacter(
			storeLog,
			*segmentTargetSize,
			*segmentRetain,
			*segmentPurge,
			compactDuration,
			trashSegments,
			purgeSegments,
			log.NewContext(logger).With("component", "Compacter"),
		)
		g.Add(func() error {
			c.Run()
			return nil
		}, func(error) {
			c.Stop()
		})
	}
	{
		g.Add(func() error {
			mux := http.NewServeMux()
			mux.Handle("/store/", http.StripPrefix("/store", store.NewAPI(peer, storeLog, apiDuration, replicateBytes)))
			mux.Handle("/metrics", promhttp.Handler())
			return http.Serve(apiListener, mux)
		}, func(error) {
			apiListener.Close()
		})
	}
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}
	return g.Run()
}

func runIngestStore(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	var (
		apiAddr               = fs.String("api", "tcp://0.0.0.0:7650", "listen address for ingest and store APIs")
		fastAddr              = fs.String("ingest.fast", "tcp://0.0.0.0:7651", "listen address for fast (async) writes")
		durableAddr           = fs.String("ingest.durable", "tcp://0.0.0.0:7652", "listen address for durable (sync) writes")
		bulkAddr              = fs.String("ingest.bulk", "tcp://0.0.0.0:7653", "listen address for bulk (whole-segment) writes")
		clusterAddr           = fs.String("cluster", "tcp://0.0.0.0:7659", "listen address for cluster")
		ingestPath            = fs.String("ingest.path", filepath.Join("data", "ingest"), "path holding segment files for ingest tier")
		segmentFlushSize      = fs.Int("ingest.segment-flush-size", 25*1024*1024, "flush segments after they grow to this size")
		segmentFlushAge       = fs.Duration("ingest.segment-flush-age", 3*time.Second, "flush segments after they are active for this long")
		segmentPendingTimeout = fs.Duration("ingest.segment-pending-timeout", time.Minute, "pending segments that are claimed but uncommitted are failed after this long")
		storePath             = fs.String("store.path", filepath.Join("data", "store"), "path holding segment files for storage tier")
		segmentTargetSize     = fs.Int64("store.segment-target-size", 100*1024, "try to keep store segments about this size")
		segmentRetain         = fs.Duration("store.segment-retain", 7*24*time.Hour, "retention period for segment files")
		segmentPurge          = fs.Duration("store.segment-purge", 24*time.Hour, "purge deleted segment files after this long")
		clusterPeers          = stringset{}
	)
	fs.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// +-1----------------+   +-3----------+   +-2------+   +-1---------+  +-1--------+  +-1-----+
	// | Fast listener    |<--| Write      |-->| Writer |-->| IngestLog |  | StoreLog |  | Peer  |
	// +------------------+   | handler    |   +--------+   +-----------+  +----------+  +-------+
	// +-1----------------+   |            |                      ^           ^ ^ ^        ^ ^ ^
	// | Durable listener |<--|            |                      |           | | |        | | |
	// +------------------+   |            |                      |           | | |        | | |
	// +-1----------------+   |            |                      |           | | |        | | |
	// | Bulk listener    |<--|            |                      |           | | |        | | |
	// +------------------+   +------------+                      |           | | |        | | |
	// +-1----------------+   +-2----------+                      |           | | |        | | |
	// | API listener     |<--| Ingest API |----------------------'           | | |        | | |
	// |                  |   |            |-----------------------------------------------' | |
	// |                  |   +------------+                                  | | |          | |
	// |                  |   +-2----------+                                  | | |          | |
	// |                  |<--| Store API  |----------------------------------' | |          | |
	// |                  |   |            |-------------------------------------------------' |
	// +------------------+   +------------+                                    | |            |
	//                        +-2----------+                                    | |            |
	//                        | Compacter  |------------------------------------' |            |
	//                        +------------+                                      |            |
	//                        +-2----------+                                      |            |
	//                        | Consumer   |--------------------------------------'            |
	//                        |            |---------------------------------------------------'
	//                        +------------+

	// Logging.
	var logger log.Logger
	logger = log.NewLogfmtLogger(os.Stderr)
	logger = log.NewContext(logger).With("ts", log.DefaultTimestampUTC)
	logger = level.New(logger, level.Config{Allowed: level.AllowAll()})

	// Instrumentation.
	connectedClients := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "connected_clients",
		Help:      "Number of currently connected clients by modality.",
	}, []string{"modality"})
	clusterDelegateInvocations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "cluster_delegate_invocations_total",
		Help:      "Total invocations of cluster delegate callbacks by method.",
	}, []string{"method"})
	ingestWriterBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_bytes_written_total",
		Help:      "The total number of bytes written.",
	})
	ingestWriterRecords := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_records_written_total",
		Help:      "The total number of records written.",
	})
	ingestWriterSyncs := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_syncs_total",
		Help:      "The number of times an active segment is explicitly fsynced.",
	})
	ingestWriterRotations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_writer_flushes_total",
		Help:      "The number of times an active segment is flushed.",
	}, []string{"reason"})
	segmentState := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "segment_state_transitions",
		Help:      "Segment state transitions, by next state and reason.",
	}, []string{"mark", "due_to"})
	replicateBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "replicate_bytes",
		Help:      "Bytes replicated to this node.",
	})
	apiDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "api_request_duration_seconds",
		Help:      "API request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})
	compactDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "compact_duration_seconds",
		Help:      "Duration of each compaction in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"kind", "compacted", "result"})
	trashSegments := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "trash_segments",
		Help:      "Segments moved to trash.",
	}, []string{"success"})
	purgeSegments := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "purge_segments",
		Help:      "Segments purged from trash.",
	}, []string{"success"})
	prometheus.MustRegister(
		connectedClients,
		clusterDelegateInvocations,
		ingestWriterBytes,
		ingestWriterRecords,
		ingestWriterSyncs,
		ingestWriterRotations,
		segmentState,
		replicateBytes,
		apiDuration,
		compactDuration,
		trashSegments,
		purgeSegments,
	)

	// Parse URLs for listeners.
	fastURL, err := url.Parse(strings.ToLower(*fastAddr))
	if err != nil {
		return err
	}
	durableURL, err := url.Parse(strings.ToLower(*durableAddr))
	if err != nil {
		return err
	}
	bulkURL, err := url.Parse(strings.ToLower(*bulkAddr))
	if err != nil {
		return err
	}
	apiURL, err := url.Parse(strings.ToLower(*apiAddr))
	if err != nil {
		return err
	}
	_, apiPortStr, err := net.SplitHostPort(apiURL.Host)
	if err != nil {
		return errors.Wrap(err, "splitting API listen address")
	}
	apiPort, err := strconv.Atoi(apiPortStr)
	if err != nil {
		return errors.Wrap(err, "parsing API port")
	}
	clusterURL, err := url.Parse(strings.ToLower(*clusterAddr))
	if err != nil {
		return err
	}
	clusterHost, clusterPortStr, err := net.SplitHostPort(clusterURL.Host)
	if err != nil {
		return err
	}
	clusterPort, err := strconv.Atoi(clusterPortStr)
	if err != nil {
		return err
	}

	// Bind listeners.
	fastListener, err := net.Listen(fastURL.Scheme, fastURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("fast", fastURL.String())
	durableListener, err := net.Listen(durableURL.Scheme, durableURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("durable", durableURL.String())
	bulkListener, err := net.Listen(bulkURL.Scheme, bulkURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("bulk", bulkURL.String())
	apiListener, err := net.Listen(apiURL.Scheme, apiURL.Host)
	if err != nil {
		return err
	}
	level.Info(logger).Log("API", apiURL.String())

	// Create ingestlog and its writer.
	ingestLog, err := ingest.NewFileLog(*ingestPath)
	if err != nil {
		return err
	}
	ingestWriter, err := ingest.NewWriter(
		ingestLog,
		*segmentFlushSize,
		*segmentFlushAge,
		ingestWriterBytes,
		ingestWriterRecords,
		ingestWriterSyncs,
		ingestWriterRotations,
	)
	if err != nil {
		return err
	}
	level.Info(logger).Log("ingest_path", *ingestPath)

	// Create storelog.
	storeLog, err := store.NewFileLog(*storePath, *segmentTargetSize)
	if err != nil {
		return err
	}
	level.Info(logger).Log("store_path", *storePath)

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterHost, clusterPort,
		clusterPeers.slice(),
		cluster.PeerTypeIngestStore, apiPort,
		clusterDelegateInvocations,
		log.NewContext(logger).With("component", "cluster"),
	)
	if err != nil {
		return err
	}
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "cluster_size",
		Help:      "Number of peers in the cluster from this node's perspective.",
	}, func() float64 { return float64(peer.ClusterSize()) }))

	// Execution group.
	var g group.Group
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			<-cancel
			ingestWriter.Stop()
			return nil
		}, func(error) {
			close(cancel)
		})
	}
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			<-cancel
			return peer.Leave(time.Second)
		}, func(error) {
			close(cancel)
		})
	}
	{
		entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
		idGen := func() string { return ulid.MustNew(ulid.Now(), entropy).String() } // TODO(pb): could improve efficiency
		g.Add(func() error {
			return ingest.HandleConnections(fastListener, ingestWriter, ingest.HandleFastWriter, idGen, connectedClients.WithLabelValues("fast"))
		}, func(error) {
			fastListener.Close()
		})
		g.Add(func() error {
			return ingest.HandleConnections(durableListener, ingestWriter, ingest.HandleDurableWriter, idGen, connectedClients.WithLabelValues("durable"))
		}, func(error) {
			durableListener.Close()
		})
		g.Add(func() error {
			return ingest.HandleConnections(bulkListener, ingestWriter, ingest.HandleBulkWriter, idGen, connectedClients.WithLabelValues("bulk"))
		}, func(error) {
			bulkListener.Close()
		})
	}
	{
		c := store.NewConsumer(
			peer,
			storeLog,
			*segmentTargetSize,
			log.NewContext(logger).With("component", "Consumer"),
		)
		g.Add(func() error {
			c.Run()
			return nil
		}, func(error) {
			c.Stop()
		})
	}
	{
		c := store.NewCompacter(
			storeLog,
			*segmentTargetSize,
			*segmentRetain,
			*segmentPurge,
			compactDuration,
			trashSegments,
			purgeSegments,
			log.NewContext(logger).With("component", "Compacter"),
		)
		g.Add(func() error {
			c.Run()
			return nil
		}, func(error) {
			c.Stop()
		})
	}
	{
		g.Add(func() error {
			mux := http.NewServeMux()
			mux.Handle("/ingest/", http.StripPrefix("/ingest", ingest.NewAPI(peer, ingestLog, *segmentPendingTimeout, apiDuration, segmentState)))
			mux.Handle("/store/", http.StripPrefix("/store", store.NewAPI(peer, storeLog, apiDuration, replicateBytes)))
			mux.Handle("/metrics", promhttp.Handler())
			return http.Serve(apiListener, mux)
		}, func(error) {
			apiListener.Close()
		})
	}
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}
	return g.Run()
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	var (
		storeAddr = fs.String("store", "localhost:7650", "okstore instance")
		from      = fs.String("from", time.Now().Add(-1*time.Hour).Format(time.RFC3339Nano), "from (RFC3339 or duration)")
		to        = fs.String("to", time.Now().Format(time.RFC3339Nano), "to (RFC3339 or duration)")
		q         = fs.String("q", "", "query expression")
		stats     = fs.Bool("stats", false, "statistics only, no data")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	fromDuration, durationErr := time.ParseDuration(*from)
	fromTime, timeErr := time.Parse(time.RFC3339Nano, *from)
	var fromStr string
	switch {
	case durationErr == nil && timeErr != nil:
		fromStr = time.Now().Add(fromDuration).Format(time.RFC3339Nano)
	case durationErr != nil && timeErr == nil:
		fromStr = fromTime.Format(time.RFC3339Nano)
	default:
		return fmt.Errorf("couldn't parse -from (%q) as either duration or time", *from)
	}

	toDuration, durationErr := time.ParseDuration(*to)
	toTime, timeErr := time.Parse(time.RFC3339Nano, *to)
	var toStr string
	switch {
	case durationErr == nil && timeErr != nil:
		toStr = time.Now().Add(toDuration).Format(time.RFC3339Nano)
	case durationErr != nil && timeErr == nil:
		toStr = toTime.Format(time.RFC3339Nano)
	default:
		return fmt.Errorf("couldn't parse -to (%q) as either duration or time", *to)
	}

	fmt.Fprintf(os.Stderr, "-from=%s -to=%s\n", fromStr, toStr)

	method := "GET"
	if *stats {
		method = "HEAD"
	}

	// TODO(pb): use const or client lib for URL
	req, err := http.NewRequest(method, fmt.Sprintf(
		"http://%s/store/query?from=%s&to=%s&q=%s",
		*storeAddr,
		url.QueryEscape(fromStr),
		url.QueryEscape(toStr),
		url.QueryEscape(*q),
	), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	var result store.QueryResult
	result.DecodeFrom(resp)

	fmt.Fprintf(os.Stderr, "%d node(s) queried\n", result.NodesQueried)
	fmt.Fprintf(os.Stderr, "%d segment(s) queried\n", result.SegmentsQueried)
	fmt.Fprintf(os.Stderr, "%d record(s) queried\n", result.RecordsQueried)
	fmt.Fprintf(os.Stderr, "%d record(s) matched\n", result.RecordsMatched)
	fmt.Fprintf(os.Stderr, "%d error(s)\n", result.ErrorCount)
	io.Copy(os.Stdout, result.Records)
	result.Records.Close()
	return nil
}

type stringset map[string]struct{}

func (ss stringset) Set(s string) error {
	ss[s] = struct{}{}
	return nil
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ", ")
}

func (ss stringset) slice() (res []string) {
	res = make([]string, 0, len(ss))
	for s := range ss {
		res = append(res, s)
	}
	return res
}

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-c:
		return fmt.Errorf("received signal %s", sig)
	case <-cancel:
		return errors.New("canceled")
	}
}
