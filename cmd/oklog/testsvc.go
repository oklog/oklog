package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

func runTestService(args []string) error {
	flagset := flag.NewFlagSet("testsvc", flag.ExitOnError)
	var (
		id   = flagset.String("id", "foo", "ID for this instance")
		size = flagset.Int("size", 512, "bytes per record")
		rate = flagset.Int("rate", 5, "records per second")
	)
	flagset.Usage = usageFor(flagset, "oklog testsvc [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	// Populate a set of records.
	fmt.Fprintf(os.Stderr, "reticulating splines...\n")
	rand.Seed(time.Now().UnixNano())
	var (
		tslen = len(fmt.Sprintf("%s", time.Now().Format(time.RFC3339)))
		idlen = len(*id)
		ctlen = 9                                 // %09d
		presz = tslen + 1 + idlen + 1 + ctlen + 1 // "2016-01-01T12:34:56+01:00 foo 000000001 "
		recsz = *size - presz - 1                 // <prefix> <record> <\n>
	)
	if recsz <= 0 {
		return errors.Errorf("with -id %q, minimum -size is %d", *id, presz+1+1)
	}
	var (
		charset = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
		wordMin = 5
		wordMax = 20
		records = make([]string, 10000)
	)
	for i := 0; i < len(records); i++ {
		record := make([]rune, recsz)
		wordLen := wordMin + rand.Intn(wordMax-wordMin)
		for j := range record {
			if (j % wordLen) == (wordLen - 1) {
				record[j] = ' '
			} else {
				record[j] = rune(charset[rand.Intn(len(charset))])
			}
		}
		records[i] = strings.TrimSpace(string(record))
	}

	// Prepare some statistics.
	var (
		nBytes   uint64
		nRecords uint64
	)
	printRate := func(d time.Duration, printEvery int) {
		var prevBytes, prevRecords, iterationCount uint64
		for range time.Tick(d) {
			currBytes := atomic.LoadUint64(&nBytes)
			currRecords := atomic.LoadUint64(&nRecords)
			bytesPerSec := (float64(currBytes) - float64(prevBytes)) / d.Seconds()
			recordsPerSec := (float64(currRecords) - float64(prevRecords)) / d.Seconds()

			prevBytes = currBytes
			prevRecords = currRecords

			iterationCount++
			if iterationCount%uint64(printEvery) == 0 {
				fmt.Fprintf(os.Stderr, "%2ds average: %.2f bytes/sec, %.2f records/sec\n", int(d.Seconds()), bytesPerSec, recordsPerSec)
			}

		}
	}
	go printRate(1*time.Second, 10)
	//go printRate(10*time.Second, 1)

	// Emit.

	// Calculate cycle parameters.
	// If cycles are too short, we never meet our rate target.
	var (
		recordsPerCycle = 1
		timePerCycle    = time.Duration(float64(time.Second) / float64(*rate))
	)
	for timePerCycle < 50*time.Millisecond {
		recordsPerCycle *= 2
		timePerCycle *= 2
	}

	// Emit!
	var count int
	fmt.Fprintf(os.Stderr,
		"%s starting, %d bytes/record, %d records/sec; %d records/cycle, %s/cycle\n",
		*id, *size, *rate, recordsPerCycle, timePerCycle,
	)
	for range time.Tick(timePerCycle) {
		var bytes int
		for i := 0; i < recordsPerCycle; i++ {
			count++
			if n, err := fmt.Fprintf(os.Stdout,
				"%s %s %09d %s\n",
				time.Now().Format(time.RFC3339),
				*id,
				count,
				records[count%len(records)],
			); err != nil {
				fmt.Fprintf(os.Stderr, "%d: %v\n", count, err)
			} else {
				bytes += n
			}
		}
		atomic.AddUint64(&nBytes, uint64(bytes))
		atomic.AddUint64(&nRecords, uint64(recordsPerCycle))
	}
	return nil
}
