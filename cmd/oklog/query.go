package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
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
		verbose   = flagset.Bool("v", false, "verbose output to stderr")
	)
	flagset.Usage = usageFor(flagset, "oklog query [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	begin := time.Now()

	stdverbose := ioutil.Discard
	if *verbose || *stats {
		stdverbose = os.Stderr
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

	fmt.Fprintf(stdverbose, "%s %s\n", method, req.URL.String())
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

	qtype := "string"
	if result.Params.Regex {
		qtype = "regex"
	}

	dataSetSize := int64(result.NodesQueried+result.ErrorCount) * result.MaxDataSetSize

	w := tabwriter.NewWriter(stdverbose, 0, 2, 2, ' ', 0)
	fmt.Fprintf(w, "Response in\t%s\n", time.Since(begin))
	fmt.Fprintf(w, "Queried from\t%s\n", result.Params.From)
	fmt.Fprintf(w, "Queried to\t%s\n", result.Params.To)
	fmt.Fprintf(w, "Queried %s\t%q\n", qtype, result.Params.Q)
	fmt.Fprintf(w, "Node count\t%d\n", result.NodesQueried)
	fmt.Fprintf(w, "Segment count\t%d\n", result.SegmentsQueried)
	fmt.Fprintf(w, "Approx. query set\t%dB (%dMiB)\n", dataSetSize, dataSetSize/(1024*1024))
	fmt.Fprintf(w, "Error count\t%d\n", result.ErrorCount)
	fmt.Fprintf(w, "Reported duration\t%s\n", result.Duration)
	w.Flush()

	switch {
	case *nocopy:
		break
	case *withulid:
		io.Copy(os.Stdout, result.Records)
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
