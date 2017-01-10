package store

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/oklog/ulid"
)

func mergeRecords(w io.Writer, readers ...io.Reader) (low, high ulid.ULID, n int64, err error) {
	// Optimization and safety.
	if len(readers) == 0 {
		return low, high, 0, nil
	}

	// https://github.com/golang/go/wiki/SliceTricks
	notnil := readers[:0]
	for _, r := range readers {
		if r != nil {
			notnil = append(notnil, r)
		}
	}
	readers = notnil

	// Initialize our state.
	var (
		first   = true
		scanner = make([]*bufio.Scanner, len(readers))
		ok      = make([]bool, len(readers))
		record  = make([][]byte, len(readers))
		id      = make([][]byte, len(readers))
	)

	// Advance moves the next record from a reader into our state.
	advance := func(i int) error {
		if ok[i] = scanner[i].Scan(); ok[i] {
			record[i] = scanner[i].Bytes()
			// Something nice, like bytes.Fields, is too slow!
			//id[i] = bytes.Fields(record[i])[0]
			if len(record[i]) < ulid.EncodedSize {
				panic("short record")
			}
			id[i] = record[i][:ulid.EncodedSize]
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
	var (
		chosenIndex int
		chosenULID  ulid.ULID
	)
	for {
		// Choose the smallest ULID from all of the IDs.
		chosenIndex = -1
		for i := 0; i < len(readers); i++ {
			if !ok[i] {
				continue
			}
			if chosenIndex < 0 || bytes.Compare(id[i], id[chosenIndex]) < 0 {
				chosenIndex = i
			}
		}
		if chosenIndex < 0 {
			break // drained all the scanners
		}

		// The first ULID from this batch of readers is the low ULID.
		// The last ULID from this batch of readers is the high ULID.
		// Record the first ULID as low, and the latest ULID as high.
		chosenULID.UnmarshalText(id[chosenIndex])
		if first { // record first as low
			low, first = chosenULID, false
		} else if !first && chosenULID == high { // duplicate!
			if err := advance(chosenIndex); err != nil {
				return low, high, n, err
			}
			continue
		}
		high = chosenULID // record most recent as high

		// Write the record.
		n0, err := w.Write(record[chosenIndex])
		if err != nil {
			return low, high, n, err
		}
		n += int64(n0)
		n1, err := w.Write([]byte{'\n'})
		if err != nil {
			return low, high, n, err
		}
		n += int64(n1)

		// Advance the chosen scanner by 1 record.
		if err := advance(chosenIndex); err != nil {
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

	// https://github.com/golang/go/wiki/SliceTricks
	notnil := readers[:0]
	for _, r := range readers {
		if r != nil {
			notnil = append(notnil, r)
		}
	}
	readers = notnil

	// Per-segment state.
	writeSegment, err := dst.Create()
	if err != nil {
		return n, err
	}
	defer func() {
		// Don't leak active segments.
		if writeSegment != nil {
			if deleteErr := writeSegment.Delete(); deleteErr != nil {
				panic(deleteErr)
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
		record  = make([][]byte, len(readers))
		id      = make([][]byte, len(readers))
	)

	// Advance moves the next record from a reader into our state.
	advance := func(i int) error {
		if ok[i] = scanner[i].Scan(); ok[i] {
			record[i] = scanner[i].Bytes()
			// Something nice, like bytes.Fields, is too slow!
			//id[i] = bytes.Fields(record[i])[0]
			if len(record[i]) < ulid.EncodedSize {
				panic("short record")
			}
			id[i] = record[i][:ulid.EncodedSize]
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
	var (
		chosenIndex int
		chosenULID  ulid.ULID
	)
	for {
		// Choose the smallest ULID.
		chosenIndex = -1
		for i := 0; i < len(readers); i++ {
			if !ok[i] {
				continue
			}
			if chosenIndex < 0 || bytes.Compare(id[i], id[chosenIndex]) < 0 {
				chosenIndex = i
			}
		}
		if chosenIndex < 0 {
			break // drained all the scanners
		}

		// The first ULID from this batch of readers is the low ULID.
		// The last ULID from this batch of readers is the high ULID.
		// Record the first ULID as low, and the latest ULID as high.
		chosenULID.UnmarshalText(id[chosenIndex])
		if first { // record first as low
			low, first = chosenULID, false
		} else if !first && chosenULID == high { // duplicate!
			if err := advance(chosenIndex); err != nil {
				return n, err
			}
			continue
		}
		high = chosenULID // record most recent as high

		// Write the record.
		n0, err := fmt.Fprintf(writeSegment, "%s\n", record[chosenIndex])
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
		if err := advance(chosenIndex); err != nil {
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
