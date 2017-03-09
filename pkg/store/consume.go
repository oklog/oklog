package store

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oklog/oklog/pkg/cluster"
	"github.com/oklog/oklog/pkg/ingest"
)

// Consumer reads segments from the ingesters, and replicates merged segments to
// the rest of the cluster. It's implemented as a state machine: gather
// segments, replicate, commit, and repeat. All failures invalidate the entire
// batch.
type Consumer struct {
	peer               *cluster.Peer
	client             *http.Client
	segmentTargetSize  int64
	segmentTargetAge   time.Duration
	replicationFactor  int
	gatherErrors       int                 // heuristic to move out of gather state
	pending            map[string][]string // ingester: segment IDs
	active             *bytes.Buffer       // merged pending segments
	activeSince        time.Time           // active segment has been "open" since this time
	stop               chan chan struct{}
	consumedSegments   prometheus.Counter
	consumedBytes      prometheus.Counter
	replicatedSegments prometheus.Counter
	replicatedBytes    prometheus.Counter
	logger             log.Logger
}

// NewConsumer creates a consumer.
// Don't forget to Run it.
func NewConsumer(
	peer *cluster.Peer,
	client *http.Client,
	segmentTargetSize int64,
	segmentTargetAge time.Duration,
	replicationFactor int,
	consumedSegments, consumedBytes prometheus.Counter,
	replicatedSegments, replicatedBytes prometheus.Counter,
	logger log.Logger,
) *Consumer {
	return &Consumer{
		peer:               peer,
		client:             client,
		segmentTargetSize:  segmentTargetSize,
		segmentTargetAge:   segmentTargetAge,
		replicationFactor:  replicationFactor,
		gatherErrors:       0,
		pending:            map[string][]string{},
		active:             &bytes.Buffer{},
		activeSince:        time.Time{},
		stop:               make(chan chan struct{}),
		consumedSegments:   consumedSegments,
		consumedBytes:      consumedBytes,
		replicatedSegments: replicatedSegments,
		replicatedBytes:    replicatedBytes,
		logger:             logger,
	}
}

// Run consumes segments from ingest nodes, and replicates them to the cluster.
// Run returns when Stop is invoked.
func (c *Consumer) Run() {
	step := time.NewTicker(100 * time.Millisecond)
	defer step.Stop()
	state := c.gather
	for {
		select {
		case <-step.C:
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
		base = log.With(c.logger, "state", "gather")
		warn = level.Warn(base)
	)

	// A naïve way to break out of the gather loop in atypical conditions.
	// TODO(pb): this obviously needs more thought and consideration
	instances := c.peer.Current(cluster.PeerTypeIngest)
	if c.gatherErrors > 0 && c.gatherErrors > 2*len(instances) {
		if c.active.Len() <= 0 {
			// We didn't successfully consume any segments.
			// Nothing to do but reset and try again.
			c.gatherErrors = 0
			return c.gather
		}
		// We consumed some segment, at least.
		// Press forward to persistence.
		return c.replicate
	}
	if len(instances) == 0 {
		return c.gather // maybe some will come back later
	}
	if want, have := c.replicationFactor, len(c.peer.Current(cluster.PeerTypeStore)); have < want {
		// Don't gather if we can't replicate.
		// Better to queue up on the ingesters.
		warn.Log("replication_factor", want, "available_peers", have, "err", "replication currently impossible")
		time.Sleep(time.Second)
		c.gatherErrors++
		return c.gather
	}

	// More typical exit clauses.
	var (
		tooBig = int64(c.active.Len()) > c.segmentTargetSize
		tooOld = !c.activeSince.IsZero() && time.Since(c.activeSince) > c.segmentTargetAge
	)
	if tooBig || tooOld {
		return c.replicate
	}

	// Get the oldest segment ID from a random ingester.
	instance := instances[rand.Intn(len(instances))]
	nextResp, err := c.client.Get(fmt.Sprintf("http://%s/ingest%s", instance, ingest.APIPathNext))
	if err != nil {
		warn.Log("ingester", instance, "during", ingest.APIPathNext, "err", err)
		c.gatherErrors++
		return c.gather
	}
	defer nextResp.Body.Close()
	nextRespBody, err := ioutil.ReadAll(nextResp.Body)
	if err != nil {
		warn.Log("ingester", instance, "during", ingest.APIPathNext, "err", err)
		c.gatherErrors++
		return c.gather
	}
	nextID := strings.TrimSpace(string(nextRespBody))
	if nextResp.StatusCode == http.StatusNotFound {
		// Normal, when the ingester has no more segments to give right now.
		c.gatherErrors++ // after enough of these errors, we should replicate
		return c.gather
	}
	if nextResp.StatusCode != http.StatusOK {
		warn.Log("ingester", instance, "during", ingest.APIPathNext, "returned", nextResp.Status)
		c.gatherErrors++
		return c.gather
	}

	// Mark the segment ID as pending.
	// From this point forward, we must either commit or fail the segment.
	// If we do neither, it will eventually time out, but we should be nice.
	c.pending[instance] = append(c.pending[instance], nextID)

	// Read the segment.
	readResp, err := c.client.Get(fmt.Sprintf("http://%s/ingest%s?id=%s", instance, ingest.APIPathRead, nextID))
	if err != nil {
		// Reading failed, so we can't possibly commit the segment.
		// The simplest thing to do now is to fail everything.
		// TODO(pb): this could be improved i.e. made more granular
		warn.Log("ingester", instance, "during", ingest.APIPathRead, "err", err)
		c.gatherErrors++
		return c.fail // fail everything
	}
	defer readResp.Body.Close()
	if readResp.StatusCode != http.StatusOK {
		warn.Log("ingester", instance, "during", ingest.APIPathRead, "returned", readResp.Status)
		c.gatherErrors++
		return c.fail // fail everything, same as above
	}

	// Merge the segment into our active segment.
	var cw countingWriter
	if _, _, _, err := mergeRecords(c.active, io.TeeReader(readResp.Body, &cw)); err != nil {
		warn.Log("ingester", instance, "during", "mergeRecords", "err", err)
		c.gatherErrors++
		return c.fail // fail everything, same as above
	}
	if c.activeSince.IsZero() {
		c.activeSince = time.Now()
	}

	// Repeat!
	c.consumedSegments.Inc()
	c.consumedBytes.Add(float64(cw.n))
	return c.gather
}

func (c *Consumer) replicate() stateFn {
	var (
		base = log.With(c.logger, "state", "replicate")
		warn = level.Warn(base)
	)

	// Replicate the segment to the cluster.
	var (
		peers      = c.peer.Current(cluster.PeerTypeStore)
		indices    = rand.Perm(len(peers))
		replicated = 0
	)
	if want, have := c.replicationFactor, len(peers); have < want {
		warn.Log("replication_factor", want, "available_peers", have, "err", "replication currently impossible")
		return c.fail // can't do anything here
	}
	for i := 0; i < len(indices) && replicated < c.replicationFactor; i++ {
		var (
			index    = indices[i]
			target   = peers[index]
			uri      = fmt.Sprintf("http://%s/store%s", target, APIPathReplicate)
			bodyType = "application/binary"
			body     = bytes.NewReader(c.active.Bytes())
		)
		resp, err := c.client.Post(uri, bodyType, body)
		if err != nil {
			warn.Log("target", target, "during", APIPathReplicate, "err", err)
			continue // we'll try another one
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			warn.Log("target", target, "during", APIPathReplicate, "got", resp.Status)
			continue // we'll try another one
		}
		replicated++
	}
	if replicated < c.replicationFactor {
		warn.Log("err", "failed to fully replicate", "want", c.replicationFactor, "have", replicated)
		return c.fail // harsh, but OK
	}

	// All good!
	c.replicatedSegments.Inc()
	c.replicatedBytes.Add(float64(c.active.Len()))
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
		base = log.With(c.logger, "state", commitOrFailed)
		warn = level.Warn(base)
	)

	// If commits fail, the segment may be re-replicated; that's OK.
	// If fails fail, the segment will eventually time-out; that's also OK.
	// So we have best-effort semantics, just log the error and move on.
	var wg sync.WaitGroup
	for instance, ids := range c.pending {
		wg.Add(len(ids))
		for _, id := range ids {
			go func(instance, id string) {
				defer wg.Done()
				uri := fmt.Sprintf("http://%s/ingest/%s?id=%s", instance, commitOrFailed, id)
				resp, err := c.client.Post(uri, "text/plain", nil)
				if err != nil {
					warn.Log("instance", instance, "during", "POST", "uri", uri, "err", err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					warn.Log("instance", instance, "during", "POST", "uri", uri, "status", resp.Status)
					return
				}
			}(instance, id)
		}
	}
	wg.Wait()

	// Reset various pending things.
	c.gatherErrors = 0
	c.pending = map[string][]string{}
	c.active.Reset()
	c.activeSince = time.Time{}

	// Back to the beginning.
	return c.gather
}

type countingWriter struct{ n int64 }

func (cw *countingWriter) Write(p []byte) (int, error) {
	cw.n += int64(len(p))
	return len(p), nil
}
