package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/oklog/prototype/pkg/store"
)

func runQuery(args []string) error {
	flagset := flag.NewFlagSet("query", flag.ExitOnError)
	var (
		storeAddr = flagset.String("store", "localhost:7650", "address of store instance to query")
		from      = flagset.String("from", "1h", "from, as RFC3339 timestamp or duration ago")
		to        = flagset.String("to", "now", "to, as RFC3339 timestamp or duration ago")
		q         = flagset.String("q", "", "query expression")
		regex     = flagset.Bool("regex", false, "parse -q as regular expression")
		stats     = flagset.Bool("stats", false, "statistics only, no records")
		nocopy    = flagset.Bool("nocopy", false, "don't read the response body")
	)
	flagset.Usage = usageFor(flagset, "oklog query [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	begin := time.Now()

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

	fmt.Fprintf(os.Stderr, "-from %s -to %s\n", fromStr, toStr)

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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	var result store.QueryResult
	result.DecodeFrom(resp)

	qtype := "normal string"
	if result.Regex {
		qtype = "regular expression"
	}

	fmt.Fprintf(os.Stderr, "Response in %s\n", time.Since(begin))
	fmt.Fprintf(os.Stderr, "Queried from %s\n", result.From)
	fmt.Fprintf(os.Stderr, "Queried to %s\n", result.To)
	fmt.Fprintf(os.Stderr, "Queried %s %q\n", qtype, result.Q)
	fmt.Fprintf(os.Stderr, "%d node(s) queried\n", result.NodesQueried)
	fmt.Fprintf(os.Stderr, "%d segment(s) queried\n", result.SegmentsQueried)
	fmt.Fprintf(os.Stderr, "%dB (%dMiB) maximum data set size\n", result.MaxDataSetSize, result.MaxDataSetSize/(1024*1024))
	fmt.Fprintf(os.Stderr, "%d error(s)\n", result.ErrorCount)
	fmt.Fprintf(os.Stderr, "%s server-reported duration\n", result.Duration)

	if !*nocopy {
		io.Copy(os.Stdout, result.Records)
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
