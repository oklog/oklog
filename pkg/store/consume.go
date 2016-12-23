package store

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/experimental_level"

	"github.com/oklog/prototype/pkg/cluster"
)

// Consumer reads segments from the ingesters, and writes merged segments to the
// StoreLog. Consumer is implemented as a state machine: Gather segments,
// replicate, commit, and repeat. Failures invalidate the batch.
type Consumer struct {
	src               *cluster.Peer
	segmentTargetSize int64
	gatherErrors      int                 // heuristic to move out of gather state
	pending           map[string][]string // ingester: segment IDs
	active            *bytes.Buffer       // merged pending segments
	activeSince       time.Time           // active segment has been "open" since this time
	dst               Log
	stop              chan chan struct{}
	logger            log.Logger
}

// NewConsumer creates a consumer.
// Don't forget to Run it.
func NewConsumer(src *cluster.Peer, dst Log, segmentTargetSize int64, logger log.Logger) *Consumer {
	return &Consumer{
		src:               src,
		segmentTargetSize: segmentTargetSize,
		gatherErrors:      0,
		pending:           map[string][]string{},
		active:            &bytes.Buffer{},
		activeSince:       time.Time{},
		dst:               dst,
		stop:              make(chan chan struct{}),
		logger:            logger,
	}
}

// Run consumes segments from ingest nodes, and replicates them to the cluster.
// Run returns when Stop is invoked.
func (c *Consumer) Run() {
	step := time.Tick(100 * time.Millisecond)
	state := c.gather
	for {
		select {
		case <-step:
			state = state()

		case q := <-c.stop:
			c.fail() // any outstanding segments
			close(q)
			return
		}
	}
}

// Stop the consumer from consuming.
func (c *Consumer) Stop() {
	q := make(chan struct{})
	c.stop <- q
	<-q
}

type stateFn func() stateFn

func (c *Consumer) gather() stateFn {
	var (
		base = log.NewContext(c.logger).With("state", "gather")
		//debug = level.Debug(base)
		warn = level.Warn(base)
	)
	//debug.Log("active_since", c.activeSince, "active_records", len(c.active))

	// A naïve way to break out of the gather loop in atypical conditions.
	// TODO(pb): this obviously needs more thought and consideration
	instances := c.src.Current(cluster.PeerTypeIngest)
	if c.gatherErrors > 0 && c.gatherErrors > 2*len(instances) {
		if c.active.Len() <= 0 {
			// We didn't successfully consume any segments.
			// Nothing to do but reset and try again.
			c.gatherErrors = 0
			return c.gather
		}
		// We consumed some segment, at least.
		// Press forward to persistence.
		//debug.Log("gather_errors", c.gatherErrors, "moving_to", "replicate")
		return c.replicate
	}
	if len(instances) == 0 {
		return c.gather // maybe some will come back later
	}

	// More typical exit clauses.
	const maxAge = time.Second // TODO(pb): parameterize
	var (
		tooBig = int64(c.active.Len()) > c.segmentTargetSize
		tooOld = !c.activeSince.IsZero() && time.Now().Sub(c.activeSince) > maxAge
	)
	if tooBig || tooOld {
		return c.replicate
	}

	// Get the oldest segment ID from a random ingester.
	// TODO(pb): use const for "/next" path
	// TODO(pb): use non-default HTTP client
	instance := instances[rand.Intn(len(instances))]
	nextResp, err := http.Get(fmt.Sprintf("http://%s/ingest/next", instance))
	if err != nil {
		warn.Log("ingester", instance, "during", "Next", "err", err)
		c.gatherErrors++
		return c.gather
	}
	defer nextResp.Body.Close()
	nextRespBody, err := ioutil.ReadAll(nextResp.Body)
	if err != nil {
		warn.Log("ingester", instance, "during", "Next", "err", err)
		c.gatherErrors++
		return c.gather
	}
	nextRespBodyStr := strings.TrimSpace(string(nextRespBody))
	if nextResp.StatusCode == http.StatusNotFound {
		// Normal, when the ingester has no more segments to give right now.
		//debug.Log("ingester", instance, "segments", "depleted")
		c.gatherErrors++ // after enough of these errors, we should replicate
		return c.gather
	}
	if nextResp.StatusCode != http.StatusOK {
		warn.Log("ingester", instance, "during", "Next", "returned", nextResp.Status)
		c.gatherErrors++
		return c.gather
	}

	// Mark the segment ID as pending.
	// From this point forward, we must either commit or fail the segment.
	// If we do neither, it will eventually time out, but we should be nice.
	c.pending[instance] = append(c.pending[instance], nextRespBodyStr)
	//debug.Log("instance", instance, "mark_pending", nextRespBodyStr)

	// Read the segment.
	// TODO(pb): use const for "/read" path
	// TODO(pb): use non-default HTTP client
	// TODO(pb): checksum
	readResp, err := http.Get(fmt.Sprintf("http://%s/ingest/read?id=%s", instance, nextRespBodyStr))
	if err != nil {
		// Reading failed, so we can't possibly commit the segment.
		// The simplest thing to do now is to fail everything.
		// TODO(pb): this could be improved i.e. made more granular
		warn.Log("ingester", instance, "during", "Read", "err", err)
		c.gatherErrors++
		return c.fail // fail everything
	}
	defer readResp.Body.Close()
	if readResp.StatusCode != http.StatusOK {
		warn.Log("ingester", instance, "during", "Read", "returned", readResp.Status)
		c.gatherErrors++
		return c.fail // fail everything, same as above
	}

	// Merge the segment into our active segment.
	if _, _, _, err := mergeRecords(c.active, readResp.Body); err != nil {
		warn.Log("ingester", instance, "during", "mergeRecords", "err", err)
		c.gatherErrors++
		return c.fail // fail everything, same as above
	}
	if c.activeSince.IsZero() {
		c.activeSince = time.Now()
	}

	//debug.Log("active", c.active.Len())

	// Repeat!
	return c.gather
}

func (c *Consumer) replicate() stateFn {
	var (
		base = log.NewContext(c.logger).With("state", "replicate")
		//debug = level.Debug(base)
		warn = level.Warn(base)
	)
	//debug.Log()

	// Replicate the segment to the cluster.
	var (
		peers      = c.src.Current(cluster.PeerTypeStore)
		quorum     = (len(peers) / 2) + 1 // TODO(pb): parameterize
		indices    = rand.Perm(len(peers))
		replicated = 0
	)
	//debug.Log("peers", fmt.Sprintf("%v", peers), "quorum", quorum, "indices", fmt.Sprintf("%v", indices))
	for i := 0; i < len(indices) && replicated < quorum; i++ {
		index := indices[i]
		target := peers[index]
		//debug.Log("replicate_attempt", i+1, "target", target)
		// TODO(pb): use const for "/read" path
		// TODO(pb): use non-default HTTP client
		// TODO(pb): checksum
		resp, err := http.Post(fmt.Sprintf("http://%s/store/replicate", target), "application/binary", bytes.NewReader(c.active.Bytes()))
		if err != nil {
			warn.Log("target", target, "during", "POST /store/replicate", "err", err)
			continue // we'll try another one
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			warn.Log("target", target, "during", "POST /store/replicate", "got", resp.Status)
			continue // we'll try another one
		}
		replicated++
		//debug.Log("target", target, "during", "POST /store/replicate", "got", resp.Status, "replicated", replicated, "quorum", quorum)
	}
	if replicated < quorum {
		warn.Log("err", "failed to achieve quorum replication", "want", quorum, "have", replicated)
		return c.fail // harsh, but OK
	}

	// All good!
	return c.commit
}

func (c *Consumer) commit() stateFn {
	return c.resetVia("commit")
}

func (c *Consumer) fail() stateFn {
	return c.resetVia("failed")
}

func (c *Consumer) resetVia(commitOrFailed string) stateFn {
	var (
		base = log.NewContext(c.logger).With("state", commitOrFailed)
		//debug = level.Debug(base)
		warn = level.Warn(base)
	)
	//debug.Log()

	// If commits fail, the segment may be re-replicated; that's OK.
	// If fails fail, the segment will eventually time-out; that's also OK.
	// So we have best-effort semantics, just log the error and move on.
	// TODO(pb): no need to do these serially
	for instance, ids := range c.pending {
		for _, id := range ids {
			// TODO(pb): use enum and consts for URL path
			// TODO(pb): use non-default HTTP client
			resp, err := http.Post(fmt.Sprintf("http://%s/ingest/%s?id=%s", instance, commitOrFailed, id), "text/plain", nil)
			if err != nil {
				warn.Log("instance", instance, "during", "POST", "err", err)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				warn.Log("instance", instance, "during", "POST", "status", resp.Status)
				continue
			}
		}
	}

	// Reset various pending things.
	c.gatherErrors = 0
	c.pending = map[string][]string{}
	c.active.Reset()
	c.activeSince = time.Time{}

	// Back to the beginning.
	return c.gather
}
