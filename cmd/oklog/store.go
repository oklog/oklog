package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/cors"

	"github.com/oklog/oklog/pkg/cluster"
	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/oklog/pkg/group"
	"github.com/oklog/oklog/pkg/store"
	"github.com/oklog/oklog/pkg/ui"
)

const (
	defaultStoreSegmentConsumers         = 1
	defaultStoreSegmentTargetSize        = 128 * 1024 * 1024
	defaultStoreSegmentTargetAge         = 3 * time.Second
	defaultStoreSegmentBufferSize        = 1024 * 1024
	defaultStoreSegmentReplicationFactor = 2
	defaultStoreSegmentRetain            = 7 * 24 * time.Hour
	defaultStoreSegmentPurge             = 24 * time.Hour
	defaultStoreSegmentDelay             = 100 * time.Millisecond
)

var (
	defaultStorePath = filepath.Join("data", "store")
)

func runStore(args []string) error {
	flagset := flag.NewFlagSet("store", flag.ExitOnError)
	var (
		debug                    = flagset.Bool("debug", false, "debug logging")
		apiAddr                  = flagset.String("api", defaultAPIAddr, "listen address for store API")
		clusterBindAddr          = flagset.String("cluster", defaultClusterAddr, "listen address for cluster")
		clusterAdvertiseAddr     = flagset.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		storePath                = flagset.String("store.path", defaultStorePath, "path holding segment files for storage tier")
		segmentConsumers         = flagset.Int("store.segment-consumers", defaultStoreSegmentConsumers, "concurrent segment consumers")
		segmentTargetSize        = flagset.Int64("store.segment-target-size", defaultStoreSegmentTargetSize, "try to keep store segments about this size")
		segmentTargetAge         = flagset.Duration("store.segment-target-age", defaultStoreSegmentTargetAge, "replicate once the aggregate segment is this old")
		segmentDelay             = flagset.Duration("store.segment-delay", defaultStoreSegmentDelay, "request next segment files after this delay")
		segmentBufferSize        = flagset.Int64("store.segment-buffer-size", defaultStoreSegmentBufferSize, "per-segment in-memory read buffer during queries")
		segmentReplicationFactor = flagset.Int("store.segment-replication-factor", defaultStoreSegmentReplicationFactor, "how many copies of each segment to replicate")
		segmentRetain            = flagset.Duration("store.segment-retain", defaultStoreSegmentRetain, "retention period for segment files")
		segmentPurge             = flagset.Duration("store.segment-purge", defaultStoreSegmentPurge, "purge deleted segment files after this long")
		uiLocal                  = flagset.Bool("ui.local", false, "ignore embedded files and go straight to the filesystem")
		filesystem               = flagset.String("filesystem", defaultFilesystem, "real, virtual, nop")
		clusterPeers             = stringslice{}
	)
	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	flagset.Usage = usageFor(flagset, "oklog store [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	//                                    +-1---------+ +-1----+
	//                                    | store.Log | | Peer |
	//                                    +-----------+ +------+
	// +-1------------+   +-2---------+      ^ ^ ^        ^ ^
	// | API listener |<--| Store API |------' | |        | |
	// |              |   |           |-------------------' |
	// +--------------+   +-----------+        | |          |
	//                    +-2---------+        | |          |
	//                    | Compacter |--------' |          |
	//                    +-----------+          |          |
	//                    +-2---------+          |          |
	//                    | Consumer  |----------'          |
	//                    |           |---------------------'
	//                    +-----------+

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
	apiDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "api_request_duration_seconds",
		Help:      "API request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})
	compactDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "store_compact_duration_seconds",
		Help:      "Duration of each compaction in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"kind", "compacted", "result"})
	consumedSegments := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "store_consumed_segments",
		Help:      "Segments consumed from ingest nodes.",
	})
	consumedBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "store_consumed_bytes",
		Help:      "Bytes consumed from ingest nodes.",
	})
	replicatedSegments := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "store_replicated_segments",
		Help:      "Segments replicated, by direction i.e. ingress or egress.",
	}, []string{"direction"})
	replicatedBytes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "store_replicated_bytes",
		Help:      "Segments replicated, by direction i.e. ingress or egress.",
	}, []string{"direction"})
	trashedSegments := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "store_trashed_segments",
		Help:      "Segments moved to trash.",
	}, []string{"success"})
	purgedSegments := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "store_purged_segments",
		Help:      "Segments purged from trash.",
	}, []string{"success"})
	prometheus.MustRegister(
		apiDuration,
		compactDuration,
		consumedSegments,
		consumedBytes,
		replicatedSegments,
		replicatedBytes,
		trashedSegments,
		purgedSegments,
	)

	// Parse URLs for listeners.
	apiNetwork, apiAddress, _, apiPort, err := parseAddr(*apiAddr, defaultAPIPort)
	if err != nil {
		return err
	}
	_, _, clusterBindHost, clusterBindPort, err := parseAddr(*clusterBindAddr, defaultClusterPort)
	if err != nil {
		return err
	}
	level.Info(logger).Log("cluster_bind", fmt.Sprintf("%s:%d", clusterBindHost, clusterBindPort))
	var (
		clusterAdvertiseHost string
		clusterAdvertisePort int
	)
	if *clusterAdvertiseAddr != "" {
		_, _, clusterAdvertiseHost, clusterAdvertisePort, err = parseAddr(*clusterAdvertiseAddr, defaultClusterPort)
		if err != nil {
			return err
		}
		level.Info(logger).Log("cluster_advertise", fmt.Sprintf("%s:%d", clusterAdvertiseHost, clusterAdvertisePort))
	}

	// Calculate an advertise IP.
	advertiseIP, err := cluster.CalculateAdvertiseIP(clusterBindHost, clusterAdvertiseHost, net.DefaultResolver, logger)
	if err != nil {
		level.Error(logger).Log("err", "couldn't deduce an advertise IP: "+err.Error())
		return err
	}
	if hasNonlocal(clusterPeers) && isUnroutable(advertiseIP.String()) {
		level.Warn(logger).Log("err", "this node advertises itself on an unroutable IP", "ip", advertiseIP.String())
		level.Warn(logger).Log("err", "this node will be unreachable in the cluster")
		level.Warn(logger).Log("err", "provide -cluster.advertise-addr as a routable IP address or hostname")
	}
	level.Info(logger).Log("user_bind_host", clusterBindHost, "user_advertise_host", clusterAdvertiseHost, "calculated_advertise_ip", advertiseIP)
	clusterAdvertiseHost = advertiseIP.String()
	if clusterAdvertisePort == 0 {
		clusterAdvertisePort = clusterBindPort
	}

	// Bind listeners.
	apiListener, err := net.Listen(apiNetwork, apiAddress)
	if err != nil {
		return err
	}
	level.Info(logger).Log("API", fmt.Sprintf("%s://%s", apiNetwork, apiAddress))

	// Create storelog.
	var fsys fs.Filesystem
	switch strings.ToLower(*filesystem) {
	case "real":
		fsys = fs.NewRealFilesystem()
	case "virtual":
		fsys = fs.NewVirtualFilesystem()
	case "nop":
		fsys = fs.NewNopFilesystem()
	default:
		return errors.Errorf("invalid -filesystem %q", *filesystem)
	}
	storeLog, err := store.NewFileLog(
		fsys,
		*storePath,
		*segmentTargetSize, *segmentBufferSize,
		store.LogReporter{Logger: log.With(logger, "component", "FileLog")},
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := storeLog.Close(); err != nil {
			level.Error(logger).Log("err", err)
		}
	}()
	level.Info(logger).Log("StoreLog", *storePath)

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterBindHost, clusterBindPort,
		clusterAdvertiseHost, clusterAdvertisePort,
		clusterPeers,
		cluster.PeerTypeStore, apiPort,
		log.With(logger, "component", "cluster"),
	)
	if err != nil {
		return err
	}
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "cluster_size",
		Help:      "Number of peers in the cluster from this node's perspective.",
	}, func() float64 { return float64(peer.ClusterSize()) }))

	// Create the HTTP clients we'll use for various purposes.
	unlimitedClient := http.DefaultClient // no timeouts, be careful
	timeoutClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			ResponseHeaderTimeout: 5 * time.Second,
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableKeepAlives:   false,
			MaxIdleConnsPerHost: 1,
		},
	}

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
	for i := 0; i < *segmentConsumers; i++ {
		c := store.NewConsumer(
			peer,
			timeoutClient,
			*segmentTargetSize,
			*segmentTargetAge,
			*segmentDelay,
			*segmentReplicationFactor,
			consumedSegments,
			consumedBytes,
			replicatedSegments.WithLabelValues("egress"),
			replicatedBytes.WithLabelValues("egress"),
			store.LogReporter{Logger: log.With(logger, "component", "Consumer")},
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
			trashedSegments,
			purgedSegments,
			store.LogReporter{Logger: log.With(logger, "component", "Compacter")},
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
			api := store.NewAPI(
				peer,
				storeLog,
				timeoutClient,
				unlimitedClient,
				replicatedSegments.WithLabelValues("ingress"),
				replicatedBytes.WithLabelValues("ingress"),
				apiDuration,
				store.LogReporter{Logger: log.With(logger, "component", "API")},
			)
			defer func() {
				if err := api.Close(); err != nil {
					level.Warn(logger).Log("err", err)
				}
			}()
			mux.Handle("/store/", http.StripPrefix("/store", api))
			mux.Handle("/ui/", http.StripPrefix("/ui", ui.NewAPI(logger, *uiLocal)))
			registerMetrics(mux)
			registerProfile(mux)
			registerHealthCheck(mux)
			return http.Serve(apiListener, cors.Default().Handler(mux))
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
