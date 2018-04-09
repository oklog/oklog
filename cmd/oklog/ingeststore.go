package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
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
	"github.com/oklog/oklog/pkg/ingest"
	"github.com/oklog/oklog/pkg/record"
	"github.com/oklog/oklog/pkg/store"
	"github.com/oklog/oklog/pkg/ui"
)

func runIngestStore(args []string) error {
	flagset := flag.NewFlagSet("ingeststore", flag.ExitOnError)
	var (
		debug                    = flagset.Bool("debug", false, "debug logging")
		apiAddr                  = flagset.String("api", defaultAPIAddr, "listen address for ingest and store APIs")
		topicMode                = flagset.String("ingest.topic-mode", topicModeStatic, "topic mode for ingested records (static, dynamic)")
		topic                    = flagset.String("ingest.topic", "default", "static topic name (requires -topic-mode=static)")
		fastAddr                 = flagset.String("ingest.fast", defaultFastAddr, "listen address for fast (async) writes")
		durableAddr              = flagset.String("ingest.durable", defaultDurableAddr, "listen address for durable (sync) writes")
		bulkAddr                 = flagset.String("ingest.bulk", defaultBulkAddr, "listen address for bulk (whole-segment) writes")
		clusterBindAddr          = flagset.String("cluster", defaultClusterAddr, "listen address for cluster")
		clusterAdvertiseAddr     = flagset.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		ingestPath               = flagset.String("ingest.path", defaultIngestPath, "path holding segment files for ingest tier")
		segmentFlushSize         = flagset.Int("ingest.segment-flush-size", defaultIngestSegmentFlushSize, "flush segments after they grow to this size")
		segmentFlushAge          = flagset.Duration("ingest.segment-flush-age", defaultIngestSegmentFlushAge, "flush segments after they are active for this long")
		segmentPendingTimeout    = flagset.Duration("ingest.segment-pending-timeout", defaultIngestSegmentPendingTimeout, "claimed but uncommitted pending segments are failed after this long")
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
	flagset.Usage = usageFor(flagset, "oklog ingeststore [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	// +-1----------------+   +-2----------+   +-1----------+  +-1---------+  +-1-----+
	// | Fast listener    |<--| Write      |-->| ingest.Log |  | store.Log |  | Peer  |
	// +------------------+   | handler    |   +------------+  +-----------+  +-------+
	// +-1----------------+   |            |         ^            ^ ^ ^         ^ ^ ^
	// | Durable listener |<--|            |         |            | | |         | | |
	// +------------------+   |            |         |            | | |         | | |
	// +-1----------------+   |            |         |            | | |         | | |
	// | Bulk listener    |<--|            |         |            | | |         | | |
	// +------------------+   +------------+         |            | | |         | | |
	// +-1----------------+   +-2----------+         |            | | |         | | |
	// | API listener     |<--| Ingest API |---------'            | | |         | | |
	// |                  |   |            |------------------------------------' | |
	// |                  |   +------------+                      | | |           | |
	// |                  |   +-2----------+                      | | |           | |
	// |                  |<--| Store API  |----------------------' | |           | |
	// |                  |   |            |--------------------------------------' |
	// +------------------+   +------------+                        | |             |
	//                        +-2----------+                        | |             |
	//                        | Compacter  |------------------------' |             |
	//                        +------------+                          |             |
	//                        +-2----------+                          |             |
	//                        | Consumer   |--------------------------'             |
	//                        |            |----------------------------------------'
	//                        +------------+

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
	connectedClients := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "oklog",
		Name:      "connected_clients",
		Help:      "Number of currently connected clients by modality.",
	}, []string{"modality"})
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
	flushedSegmentAge := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "ingest_segment_flush_age_seconds",
		Help:      "Age of segment when flushed in seconds.",
		Buckets:   prometheus.DefBuckets,
	})
	flushedSegmentSize := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "ingest_segment_flush_size_bytes",
		Help:      "Size of active segment when flushed in bytes.",
		Buckets:   []float64{1 << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18, 1 << 19, 1 << 20, 1 << 21, 1 << 22, 1 << 23, 1 << 24},
	})
	failedSegments := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_failed_segments",
		Help:      "Segments consumed, but failed and returned to flushed.",
	})
	committedSegments := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_committed_segments",
		Help:      "Segments successfully consumed and committed.",
	})
	committedBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "oklog",
		Name:      "ingest_committed_bytes",
		Help:      "Bytes successfully consumed and committed.",
	})
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
	apiDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "oklog",
		Name:      "api_request_duration_seconds",
		Help:      "API request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})
	prometheus.MustRegister(
		connectedClients,
		ingestWriterBytes,
		ingestWriterRecords,
		ingestWriterSyncs,
		ingestWriterRotations,
		flushedSegmentAge,
		flushedSegmentSize,
		failedSegments,
		committedSegments,
		committedBytes,
		compactDuration,
		consumedSegments,
		consumedBytes,
		replicatedSegments,
		replicatedBytes,
		trashedSegments,
		purgedSegments,
		apiDuration,
	)

	// Parse listener addresses.
	fastNetwork, fastAddress, _, _, err := parseAddr(*fastAddr, defaultFastPort)
	if err != nil {
		return err
	}
	durableNetwork, durableAddress, _, _, err := parseAddr(*durableAddr, defaultDurablePort)
	if err != nil {
		return err
	}
	bulkNetwork, bulkAddress, _, _, err := parseAddr(*bulkAddr, defaultBulkPort)
	if err != nil {
		return err
	}
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
	fastListener, err := net.Listen(fastNetwork, fastAddress)
	if err != nil {
		return err
	}
	level.Info(logger).Log("fast", fmt.Sprintf("%s://%s", fastNetwork, fastAddress))
	durableListener, err := net.Listen(durableNetwork, durableAddress)
	if err != nil {
		return err
	}
	level.Info(logger).Log("durable", fmt.Sprintf("%s://%s", durableNetwork, durableAddress))
	bulkListener, err := net.Listen(bulkNetwork, bulkAddress)
	if err != nil {
		return err
	}
	level.Info(logger).Log("bulk", fmt.Sprintf("%s://%s", bulkNetwork, bulkAddress))
	apiListener, err := net.Listen(apiNetwork, apiAddress)
	if err != nil {
		return err
	}
	level.Info(logger).Log("API", fmt.Sprintf("%s://%s", apiNetwork, apiAddress))

	// Create ingestlog.
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
	ingestLog, err := ingest.NewFileLog(fsys, *ingestPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := ingestLog.Close(); err != nil {
			level.Error(logger).Log("err", err)
		}
	}()
	level.Info(logger).Log("ingest_path", *ingestPath)

	// Create storelog.
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
	level.Info(logger).Log("store_path", *storePath)

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterBindHost, clusterBindPort,
		clusterAdvertiseHost, clusterAdvertisePort,
		clusterPeers,
		cluster.PeerTypeIngestStore, apiPort,
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

	var rfac record.ReaderFactory
	{
		topicb := []byte(*topic)
		switch {
		case *topicMode == topicModeDynamic:
			rfac = record.NewDynamicReader
		case *topicMode == topicModeStatic && record.IsValidTopic(topicb):
			rfac = record.StaticReaderFactory(topicb)
		case *topicMode == topicModeStatic && !record.IsValidTopic(topicb):
			return fmt.Errorf("topic name %q invalid", *topic)
		default:
			return fmt.Errorf("topic mode %q invalid, must be %q or %q", *topicMode, topicModeStatic, topicModeDynamic)
		}
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
	{
		g.Add(func() error {
			return ingest.HandleConnections(
				fastListener,
				ingest.HandleFastWriter,
				rfac,
				ingestLog,
				*segmentFlushAge, *segmentFlushSize,
				connectedClients.WithLabelValues("fast"),
				ingestWriterBytes, ingestWriterRecords, ingestWriterSyncs,
				flushedSegmentAge, flushedSegmentSize,
			)
		}, func(error) {
			fastListener.Close()
		})
		g.Add(func() error {
			return ingest.HandleConnections(
				durableListener,
				ingest.HandleDurableWriter,
				rfac,
				ingestLog,
				*segmentFlushAge, *segmentFlushSize,
				connectedClients.WithLabelValues("durable"),
				ingestWriterBytes, ingestWriterRecords, ingestWriterSyncs,
				flushedSegmentAge, flushedSegmentSize,
			)
		}, func(error) {
			durableListener.Close()
		})
		g.Add(func() error {
			return ingest.HandleConnections(
				bulkListener,
				ingest.HandleBulkWriter,
				rfac,
				ingestLog,
				*segmentFlushAge, *segmentFlushSize,
				connectedClients.WithLabelValues("bulk"),
				ingestWriterBytes, ingestWriterRecords, ingestWriterSyncs,
				flushedSegmentAge, flushedSegmentSize,
			)
		}, func(error) {
			bulkListener.Close()
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
			mux.Handle("/ingest/", http.StripPrefix("/ingest", ingest.NewAPI(
				peer,
				ingestLog,
				*segmentPendingTimeout,
				failedSegments,
				committedSegments,
				committedBytes,
				apiDuration,
			)))
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
