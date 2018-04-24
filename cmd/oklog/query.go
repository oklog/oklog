package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/oklog/oklog/pkg/store"
	"github.com/oklog/ulid"
)

func runQuery(args []string) error {
	flagset := flag.NewFlagSet("query", flag.ExitOnError)
	var (
		storeAddr = flagset.String("store", "localhost:7650", "address of store instance to query")
		from      = flagset.String("from", "1h", "from, as RFC3339 timestamp or duration ago")
		to        = flagset.String("to", "now", "to, as RFC3339 timestamp or duration ago")
		q         = flagset.String("q", "", "query expression")
		regex     = flagset.Bool("regex", false, "parse -q as regular expression")
		stats     = flagset.Bool("stats", false, "statistics only, no records (implies -v)")
		nocopy    = flagset.Bool("nocopy", false, "don't read the response body")
		withulid  = flagset.Bool("ulid", false, "include ULID prefix with each record")
		withtime  = flagset.Bool("time", false, "include time prefix with each record")
		verbose   = flagset.Bool("v", false, "verbose output to stderr")
	)
	flagset.Usage = usageFor(flagset, "oklog query [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	begin := time.Now()

	verbosePrintf := func(string, ...interface{}) {}
	if *verbose || *stats {
		verbosePrintf = func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}

	_, hostport, _, _, err := parseAddr(*storeAddr, defaultAPIPort)
	if err != nil {
		return errors.Wrap(err, "couldn't parse -store")
	}

	fromDuration, durationErr := time.ParseDuration(*from)
	fromTime, timeErr := time.Parse(time.RFC3339Nano, *from)
	fromNow := strings.ToLower(*from) == "now"
	var fromStr string
	switch {
	case fromNow:
		fromStr = time.Now().Format(time.RFC3339)
	case durationErr == nil && timeErr != nil:
		fromStr = time.Now().Add(neg(fromDuration)).Format(time.RFC3339)
	case durationErr != nil && timeErr == nil:
		fromStr = fromTime.Format(time.RFC3339)
	default:
		return fmt.Errorf("couldn't parse -from (%q) as either duration or time", *from)
	}

	toDuration, durationErr := time.ParseDuration(*to)
	toTime, timeErr := time.Parse(time.RFC3339, *to)
	toNow := strings.ToLower(*to) == "now"
	var toStr string
	switch {
	case toNow:
		toStr = time.Now().Format(time.RFC3339)
	case durationErr == nil && timeErr != nil:
		toStr = time.Now().Add(neg(toDuration)).Format(time.RFC3339)
	case durationErr != nil && timeErr == nil:
		toStr = toTime.Format(time.RFC3339)
	default:
		return fmt.Errorf("couldn't parse -to (%q) as either duration or time", *to)
	}

	method := "GET"
	if *stats {
		method = "HEAD"
	}

	var asRegex string
	if *regex {
		asRegex = "&regex=true"
	}

	req, err := http.NewRequest(method, fmt.Sprintf(
		"http://%s/store%s?from=%s&to=%s&q=%s%s",
		hostport,
		store.APIPathUserQuery,
		url.QueryEscape(fromStr),
		url.QueryEscape(toStr),
		url.QueryEscape(*q),
		asRegex,
	), nil)
	if err != nil {
		return err
	}
	verbosePrintf("GET %s\n", req.URL.String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		req.URL.RawQuery = "" // for pretty print
		return errors.Errorf("%s %s: %s", req.Method, req.URL.String(), resp.Status)
	}

	var result store.QueryResult
	if err := result.DecodeFrom(resp); err != nil {
		return errors.Wrap(err, "decoding query result")
	}

	qtype := "normal string"
	if result.Params.Regex {
		qtype = "regular expression"
	}

	verbosePrintf("Response in %s\n", time.Since(begin))
	verbosePrintf("Queried from %s\n", result.Params.From)
	verbosePrintf("Queried to %s\n", result.Params.To)
	verbosePrintf("Queried %s %q\n", qtype, result.Params.Q)
	verbosePrintf("%d node(s) queried\n", result.NodesQueried)
	verbosePrintf("%d segment(s) queried\n", result.SegmentsQueried)
	verbosePrintf("%dB (%dMiB) maximum data set size\n", result.MaxDataSetSize, result.MaxDataSetSize/(1024*1024))
	verbosePrintf("%d error(s)\n", result.ErrorCount)
	verbosePrintf("%s server-reported duration\n", result.Duration)

	switch {
	case *nocopy:
		break
	case *withulid:
		io.Copy(os.Stdout, result.Records)
	case *withtime:
		io.Copy(os.Stdout, parseTime(result.Records))
	default:
		io.Copy(os.Stdout, strip(result.Records))
	}
	result.Records.Close()

	return nil
}

func neg(d time.Duration) time.Duration {
	if d > 0 {
		d = -d
	}
	return d
}

func strip(r io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			pw.Write(s.Bytes()[ulid.EncodedSize+1:])
			pw.Write([]byte{'\n'})
		}
		pw.CloseWithError(s.Err())
	}()
	return pr
}

func parseTime(r io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			id, _ := ulid.Parse(s.Text()[:ulid.EncodedSize])
			var (
				msec = id.Time()
				sec  = msec / 1e3
				rem  = msec % 1e3
				nsec = rem * 1e6
				t    = time.Unix(int64(sec), int64(nsec))
			)

			pw.Write([]byte(t.Format(time.RFC3339)))
			pw.Write(s.Bytes()[ulid.EncodedSize:])
			pw.Write([]byte{'\n'})
		}
		pw.CloseWithError(s.Err())
	}()
	return pr
}
