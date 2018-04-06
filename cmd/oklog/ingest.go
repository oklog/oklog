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
	"github.com/oklog/oklog/pkg/ingest"
	"github.com/oklog/oklog/pkg/record"
)

const (
	defaultFastPort                    = 7651
	defaultDurablePort                 = 7652
	defaultBulkPort                    = 7653
	defaultIngestSegmentFlushSize      = 16 * 1024 * 1024
	defaultIngestSegmentFlushAge       = 3 * time.Second
	defaultIngestSegmentPendingTimeout = time.Minute
)

const (
	topicModeStatic  = "static"
	topicModeDynamic = "dynamic"
)

var (
	defaultFastAddr    = fmt.Sprintf("tcp://0.0.0.0:%d", defaultFastPort)
	defaultDurableAddr = fmt.Sprintf("tcp://0.0.0.0:%d", defaultDurablePort)
	defaultBulkAddr    = fmt.Sprintf("tcp://0.0.0.0:%d", defaultBulkPort)
	defaultIngestPath  = filepath.Join("data", "ingest")
)

func runIngest(args []string) error {
	flagset := flag.NewFlagSet("ingest", flag.ExitOnError)
	var (
		debug                 = flagset.Bool("debug", false, "debug logging")
		topicMode             = flagset.String("topic-mode", topicModeStatic, "topic mode for ingested records (static, prefix)")
		topic                 = flagset.String("topic", "default", "static topic name (requires -topic-mode=static)")
		apiAddr               = flagset.String("api", defaultAPIAddr, "listen address for ingest API")
		fastAddr              = flagset.String("ingest.fast", defaultFastAddr, "listen address for fast (async) writes")
		durableAddr           = flagset.String("ingest.durable", defaultDurableAddr, "listen address for durable (sync) writes")
		bulkAddr              = flagset.String("ingest.bulk", defaultBulkAddr, "listen address for bulk (whole-segment) writes")
		clusterBindAddr       = flagset.String("cluster", defaultClusterAddr, "listen address for cluster")
		clusterAdvertiseAddr  = flagset.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		ingestPath            = flagset.String("ingest.path", defaultIngestPath, "path holding segment files for ingest tier")
		segmentFlushSize      = flagset.Int("ingest.segment-flush-size", defaultIngestSegmentFlushSize, "flush segments after they grow to this size")
		segmentFlushAge       = flagset.Duration("ingest.segment-flush-age", defaultIngestSegmentFlushAge, "flush segments after they are active for this long")
		segmentPendingTimeout = flagset.Duration("ingest.segment-pending-timeout", defaultIngestSegmentPendingTimeout, "claimed but uncommitted pending segments are failed after this long")
		filesystem            = flagset.String("filesystem", defaultFilesystem, "real, virtual, nop")
		clusterPeers          = stringslice{}
	)
	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	flagset.Usage = usageFor(flagset, "oklog ingest [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	// +-1----------------+   +-2----------+   +-1----------+ +-1----+
	// | Fast listener    |<--| Write      |-->| ingest.Log | | Peer |
	// +------------------+   | handler    |   +------------+ +------+
	// +-1----------------+   |            |     ^              ^
	// | Durable listener |<--|            |     |              |
	// +------------------+   |            |     |              |
	// +-1----------------+   |            |     |              |
	// | Bulk listener    |<--|            |     |              |
	// +------------------+   +------------+     |              |
	// +-1----------------+   +-2----------+     |              |
	// | API listener     |<--| Ingest API |-----'              |
	// |                  |   |            |--------------------'
	// +------------------+   +------------+

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

	// Create ingest log.
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

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterBindHost, clusterBindPort,
		clusterAdvertiseHost, clusterAdvertisePort,
		clusterPeers,
		cluster.PeerTypeIngest, apiPort,
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
