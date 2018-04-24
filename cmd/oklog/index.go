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

	"github.com/oklog/oklog/pkg/cluster"
	"github.com/oklog/oklog/pkg/event"
	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/oklog/pkg/group"
	"github.com/oklog/oklog/pkg/index"
)

var (
	defaultIndexPath = filepath.Join("data", "index")
)

func runIndex(args []string) error {
	flagset := flag.NewFlagSet("store", flag.ExitOnError)
	var (
		debug                = flagset.Bool("debug", false, "debug logging")
		apiAddr              = flagset.String("api", defaultAPIAddr, "listen address for store API")
		clusterBindAddr      = flagset.String("cluster", defaultClusterAddr, "listen address for cluster")
		clusterAdvertiseAddr = flagset.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		storePath            = flagset.String("store.path", defaultStorePath, "path holding segment files for the storage tier")
		indexPath            = flagset.String("index.path", defaultIndexPath, "path holding segment and index files for the index tier")
		filesystem           = flagset.String("filesystem", defaultFilesystem, "real, virtual, nop")
		clusterPeers         = stringslice{}
	)
	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	flagset.Usage = usageFor(flagset, "oklog store [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
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
	_ = apiListener

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

	// Create peer.
	peer, err := cluster.NewPeer(
		clusterBindHost, clusterBindPort,
		clusterAdvertiseHost, clusterAdvertisePort,
		clusterPeers,
		cluster.PeerTypeIndex, apiPort,
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
	_ = unlimitedClient
	_ = timeoutClient

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
		ix, err := index.NewIndex(fsys, *indexPath, *storePath,
			event.LogReporter{Logger: log.With(logger, "component", "Indexer")})
		if err != nil {
			return err
		}
		tickr := time.NewTicker(time.Second)
		stopc := make(chan struct{})

		g.Add(func() error {
			for {
				select {
				case <-tickr.C:
					if err := ix.Sync(); err != nil {
						fmt.Println("err", err)
					}
				case <-stopc:
					return nil
				}
			}
		}, func(error) {
			close(stopc)
			tickr.Stop()
		})
	}
	// {
	// 	g.Add(func() error {
	// 		mux := http.NewServeMux()
	// 		api := index.NewAPI(
	// 			peer,
	// 			indexer,
	// 			timeoutClient,
	// 			unlimitedClient,
	// 		)
	// 		defer func() {
	// 			if err := api.Close(); err != nil {
	// 				level.Warn(logger).Log("err", err)
	// 			}
	// 		}()
	// 		mux.Handle("/index/", http.StripPrefix("/index", api))
	// 		registerMetrics(mux)
	// 		registerProfile(mux)
	// 		registerHealthCheck(mux)
	// 		return http.Serve(apiListener, cors.Default().Handler(mux))
	// 	}, func(error) {
	// 		apiListener.Close()
	// 	})
	// }
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
