package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var version = "dev" // set by release script

func usage() {
	fmt.Fprintf(os.Stderr, "USAGE\n")
	fmt.Fprintf(os.Stderr, "  %s <mode> [flags]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "MODES\n")
	fmt.Fprintf(os.Stderr, "  forward      Forwarding agent\n")
	fmt.Fprintf(os.Stderr, "  ingest       Ingester node\n")
	fmt.Fprintf(os.Stderr, "  store        Storage node\n")
	fmt.Fprintf(os.Stderr, "  ingeststore  Combination ingest+store node, for small installations\n")
	fmt.Fprintf(os.Stderr, "  query        Querying commandline tool\n")
	fmt.Fprintf(os.Stderr, "  stream       Streaming commandline tool\n")
	fmt.Fprintf(os.Stderr, "  testsvc      Test service, emits log lines at a fixed rate\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "VERSION\n")
	fmt.Fprintf(os.Stderr, "  %s (%s)\n", version, runtime.Version())
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
	case "stream":
		run = runStream
	case "testsvc":
		run = runTestService
	default:
		usage()
		os.Exit(1)
	}

	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

const (
	defaultAPIPort     = 7650
	defaultClusterPort = 7659
	defaultFilesystem  = "real"
)

var (
	defaultAPIAddr     = fmt.Sprintf("tcp://0.0.0.0:%d", defaultAPIPort)
	defaultClusterAddr = fmt.Sprintf("tcp://0.0.0.0:%d", defaultClusterPort)
)

type stringslice []string

func (ss *stringslice) Set(s string) error {
	(*ss) = append(*ss, s)
	return nil
}

func (ss *stringslice) String() string {
	if len(*ss) <= 0 {
		return "..."
	}
	return strings.Join(*ss, ", ")
}

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-c:
		return fmt.Errorf("received signal %s", sig)
	case <-cancel:
		return errors.New("canceled")
	}
}

// "udp://host:1234", 80 => udp host:1234 host 1234
// "host:1234", 80       => tcp host:1234 host 1234
// "host", 80            => tcp host:80   host 80
func parseAddr(addr string, defaultPort int) (network, address, host string, port int, err error) {
	u, err := url.Parse(strings.ToLower(addr))
	if err != nil {
		return network, address, host, port, err
	}

	switch {
	case u.Scheme == "" && u.Opaque == "" && u.Host == "" && u.Path != "": // "host"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Path, strconv.Itoa(defaultPort)), ""
	case u.Scheme != "" && u.Opaque != "" && u.Host == "" && u.Path == "": // "host:1234"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Scheme, u.Opaque), ""
	case u.Scheme != "" && u.Opaque == "" && u.Host != "" && u.Path == "": // "tcp://host[:1234]"
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			u.Host = net.JoinHostPort(u.Host, strconv.Itoa(defaultPort))
		}
	default:
		return network, address, host, port, errors.Errorf("%s: unsupported address format", addr)
	}

	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return network, address, host, port, err
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		return network, address, host, port, err
	}

	return u.Scheme, u.Host, host, port, nil
}

func registerHealthCheck(mux *http.ServeMux) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})
}

func registerMetrics(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.Handler())
}

func registerProfile(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func usageFor(fs *flag.FlagSet, short string) func() {
	return func() {
		fmt.Fprintf(os.Stderr, "USAGE\n")
		fmt.Fprintf(os.Stderr, "  %s\n", short)
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "FLAGS\n")
		w := tabwriter.NewWriter(os.Stderr, 0, 2, 2, ' ', 0)
		fs.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "\t-%s %s\t%s\n", f.Name, f.DefValue, f.Usage)
		})
		w.Flush()
		fmt.Fprintf(os.Stderr, "\n")
	}
}

func hasNonlocal(clusterPeers stringslice) bool {
	for _, peer := range clusterPeers {
		if host, _, err := net.SplitHostPort(peer); err == nil {
			peer = host
		}
		if ip := net.ParseIP(peer); ip != nil && !ip.IsLoopback() {
			return true
		} else if ip == nil && strings.ToLower(peer) != "localhost" {
			return true
		}
	}
	return false
}

func isUnroutable(addr string) bool {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	if ip := net.ParseIP(addr); ip != nil && (ip.IsUnspecified() || ip.IsLoopback()) {
		return true // typically 0.0.0.0 or localhost
	} else if ip == nil && strings.ToLower(addr) == "localhost" {
		return true
	}
	return false
}
