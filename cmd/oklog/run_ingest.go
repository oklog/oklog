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
	level "github.com/go-kit/kit/log/experimental_level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/prototype/pkg/cluster"
	"github.com/oklog/prototype/pkg/fs"
	"github.com/oklog/prototype/pkg/group"
	"github.com/oklog/prototype/pkg/ingest"
)

const (
	defaultFastPort    = 7651
	defaultDurablePort = 7652
	defaultBulkPort    = 7653
)

func runIngest(args []string) error {
	flagset := flag.NewFlagSet("ingest", flag.ExitOnError)
	var (
		apiAddr               = flagset.String("api", "tcp://0.0.0.0:7650", "listen address for ingest API")
		fastAddr              = flagset.String("ingest.fast", fmt.Sprintf("tcp://0.0.0.0:%d", defaultFastPort), "listen address for fast (async) writes")
		durableAddr           = flagset.String("ingest.durable", fmt.Sprintf("tcp://0.0.0.0:%d", defaultDurablePort), "listen address for durable (sync) writes")
		bulkAddr              = flagset.String("ingest.bulk", fmt.Sprintf("tcp://0.0.0.0:%d", defaultBulkPort), "listen address for bulk (whole-segment) writes")
		clusterAddr           = flagset.String("cluster", "tcp://0.0.0.0:7659", "listen address for cluster")
		ingestPath            = flagset.String("ingest.path", filepath.Join("data", "ingest"), "path holding segment files for ingest tier")
		segmentFlushSize      = flagset.Int("ingest.segment-flush-size", 25*1024*1024, "flush segments after they grow to this size")
		segmentFlushAge       = flagset.Duration("ingest.segment-flush-age", 3*time.Second, "flush segments after they are active for this long")
		segmentPendingTimeout = flagset.Duration("ingest.segment-pending-timeout", time.Minute, "pending segments that are claimed but uncommitted are failed after this long")
		filesystem            = flagset.String("filesystem", "real", "real, virtual, nop")
		clusterPeers          = stringset{}
	)
	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	if err := flagset.Parse(args); err != nil {
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
	//committedSegmentAge := prometheus.NewHistogram(prometheus.HistogramOpts{
	//	Namespace: "oklog",
	//	Name:      "ingest_segment_committed_age_second",
	//	Help:      "Age of segment when committed in seconds.",
	//	Buckets:   prometheus.DefBuckets,
	//})
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
		//committedSegmentAge,
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
	_, _, clusterHost, clusterPort, err := parseAddr(*clusterAddr, defaultClusterPort)
	if err != nil {
		return err
	}
	level.Info(logger).Log("cluster", fmt.Sprintf("%s:%d", clusterHost, clusterPort))

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
	level.Info(logger).Log("ingest_path", *ingestPath)

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterHost, clusterPort,
		clusterPeers.slice(),
		cluster.PeerTypeIngest, apiPort,
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
		//cancel := make(chan struct{})
		//g.Add(func() error {
		//	<-cancel
		//	ingestWriter.Stop()
		//	return nil
		//}, func(error) {
		//	close(cancel)
		//})
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
		g.Add(func() error {
			return ingest.HandleConnections(
				fastListener,
				ingest.HandleFastWriter,
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
