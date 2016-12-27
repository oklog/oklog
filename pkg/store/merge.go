package store

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/oklog/ulid"
)

func mergeRecords(w io.Writer, readers ...io.Reader) (low, high ulid.ULID, n int64, err error) {
	// Optimization and safety.
	if len(readers) == 0 {
		return low, high, 0, nil
	}

	// Initialize our state.
	var (
		first   = true
		scanner = make([]*bufio.Scanner, len(readers))
		ok      = make([]bool, len(readers))
		record  = make([]string, len(readers))
		id      = make([]string, len(readers))
	)

	// Advance moves the next record from a reader into our state.
	advance := func(i int) error {
		if ok[i] = scanner[i].Scan(); ok[i] {
			record[i] = scanner[i].Text()
			id[i] = strings.Fields(record[i])[0] // TODO(pb): profiling reveals this to be $expensive$
		} else if err := scanner[i].Err(); err != nil && err != io.EOF {
			return err
		}
		return nil
	}

	// Initialize all of the scanners and their first record.
	for i := 0; i < len(readers); i++ {
		scanner[i] = bufio.NewScanner(readers[i])
		if err := advance(i); err != nil {
			return low, high, n, err
		}
	}

	// Loop until all readers are drained.
	var chosen int
	for {
		// Choose the smallest ULID.
		chosen = -1
		for i := 0; i < len(readers); i++ {
			if !ok[i] {
				continue
			}
			if chosen < 0 || id[i] < id[chosen] {
				chosen = i
			}
		}
		if chosen < 0 {
			break // drained all the scanners
		}

		// Record the first (lowest) and last (highest) ULIDs.
		id := ulid.MustParse(id[chosen])
		if first {
			low, first = id, false
		} else if !first && id == high {
			// Duplicate detected!
			if err := advance(chosen); err != nil {
				return low, high, n, err
			}
			continue
		}
		high = id

		// Write the record.
		n0, err := fmt.Fprintf(w, "%s\n", record[chosen])
		if err != nil {
			return low, high, n, err
		}
		n += int64(n0)

		// Advance the chosen scanner by 1 record.
		if err := advance(chosen); err != nil {
			return low, high, n, err
		}
	}

	return low, high, n, nil
}

// mergeRecordsToLog is a specialization of mergeRecords.
// It enforces segmentTargetSize by creating WriteSegments as necessary.
// It has best-effort semantics, e.g. it won't split large records.
func mergeRecordsToLog(dst Log, segmentTargetSize int64, readers ...io.Reader) (n int64, err error) {
	// Optimization and safety.
	if len(readers) == 0 {
		return 0, nil
	}

	// Per-segment state.
	writeSegment, err := dst.Create()
	if err != nil {
		return n, err
	}
	defer func() {
		// Don't leak active segments.
		if writeSegment != nil {
			if err := writeSegment.Delete(); err != nil {
				panic(err)
			}
		}
	}()
	var (
		nSegment int64
		first    = true
		low      ulid.ULID
		high     ulid.ULID
	)

	// Global state.
	var (
		scanner = make([]*bufio.Scanner, len(readers))
		ok      = make([]bool, len(readers))
		record  = make([]string, len(readers))
		id      = make([]string, len(readers))
	)

	// Advance moves the next record from a reader into our state.
	advance := func(i int) error {
		if ok[i] = scanner[i].Scan(); ok[i] {
			record[i] = scanner[i].Text()
			id[i] = strings.Fields(record[i])[0] // TODO(pb): profiling reveals this to be $expensive$
		} else if err := scanner[i].Err(); err != nil && err != io.EOF {
			return err
		}
		return nil
	}

	// Initialize all of the scanners and their first record.
	for i := 0; i < len(readers); i++ {
		scanner[i] = bufio.NewScanner(readers[i])
		if err := advance(i); err != nil {
			return n, err
		}
	}

	// Loop until all readers are drained.
	var chosen int
	for {
		// Choose the smallest ULID.
		chosen = -1
		for i := 0; i < len(readers); i++ {
			if !ok[i] {
				continue
			}
			if chosen < 0 || id[i] < id[chosen] {
				chosen = i
			}
		}
		if chosen < 0 {
			break // drained all the scanners
		}

		// Record the first (lowest) and last (highest) ULIDs.
		id := ulid.MustParse(id[chosen])
		if first {
			low, first = id, false
		} else if !first && id == high {
			// Duplicate detected!
			if err := advance(chosen); err != nil {
				return n, err
			}
			continue
		}
		high = id

		// Write the record.
		n0, err := fmt.Fprintf(writeSegment, "%s\n", record[chosen])
		if err != nil {
			return n, err
		}
		n += int64(n0)
		nSegment += int64(n0)

		if nSegment >= segmentTargetSize {
			// We've met our target size. Close the segment.
			if err := writeSegment.Close(low, high); err != nil {
				return n, err
			}

			// Create a new segment, and reset our per-segment state.
			writeSegment, err = dst.Create()
			if err != nil {
				return n, err
			}
			nSegment = 0
			first = true
		}

		// Advance the chosen scanner by 1 record.
		if err := advance(chosen); err != nil {
			return n, err
		}
	}

	if nSegment > 0 {
		if err = writeSegment.Close(low, high); err != nil {
			return n, err
		}
	} else {
		err = writeSegment.Delete()
	}
	writeSegment = nil
	return n, err
}
