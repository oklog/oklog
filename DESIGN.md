# Design

## Motivation

The motivation here is basically Prometheus for logs.
What are the important properties of Prometheus that make it good, in my opinion?

- Designed to be run yourself: open-source and on-prem
- Designed specifically for highly-dynamic, typically-microservice workloads i.e. "cloud-native"
- Designed to be easy to deploy and operate: local storage, no clustering, pull model
- A complete system out of the box: doesn't need a separate TSDB, web UI, etc. to get something usable
- Scales up and out: 90% of users are satisfied without special effort

No current log system ticks these boxes.

- Splunk, Loggly, etc. are great, but hosted and/or paid
- Elastic/ELK are featureful, but too difficult to operate, and I believe Lucene is the wrong storage format
- Heka suffered from design errors leading to performance problems and is now abandoned
- Ekanite and other syslog systems are needlessly burdened by ancient requirements
- Fluentd, Logstash, etc. solve annotation and forwarding, but aren't complete systems

Ingestion is relatively straightforward.
(Somewhere, cynical and battle hardened logging engineers are laughing at me. I know, I know.)
Still: the task is to get log streams onto disk as efficiently as possible.
Ideally, we want to be bottlenecked by disk speeds on the ingest nodes.
This seems not impossible, especially if we operate at the transport level, agnostic to the log records themselves.
That is: no annotation, structured log parsing, topical routing, etc.
We can declare those as separate concerns.

Querying is a little more interesting.
I wondered the minimum viable query interface could look like.
After asking around, everyone in my peer group seemed to agree: basic grep, with time bounds, would be perfectly acceptable.
More sophisticated use cases could be modeled as ETLs or materialized views to richer systems.
And that an ingest-to-queryable time of single-digit seconds was also just fine.

It seems like there could be an opportunity here.
A logging system that you can operate yourself, without too many headaches.
One that is designed from first principles to be "cloud-native", to plug in to e.g. Kubernetes without friction.
One that absorbs large-scale workloads without special effort.
And a complete system: one that includes querying out of the box.

Let's iterate toward a system design.

## Producers and consumers

On the produer side, we have a large and dynamic set of instances emitting log records to stdout/stderr
 in the [12 Factor tradition](https://12factor.net/logs).
On the consumer side, clients want to search their logs -- somehow. 

```
     +-----------+
P -> |           |
P -> |     ?     | -> C
P -> |           |
     +-----------+
```

The producers are concerned with getting their records consumed and persisted as quickly as possible.
In case of failure, some use cases prefer backpressure (e.g. event logs) and others buffering and dropping (e.g. application logs).
But in both cases, the immediate receiving component should be optimized for fast sequential writes.

The consumers are concerned with getting responses to their queries as quickly and accurately as possible.
Because we've defined queries to require time bounds, we claim we can solve the problem with grep over time-partitioned data files.
So the final format of records on disk should be a global merge of all producer streams, partitioned by time.

```
     +-------------------+
P -> | R                 |
P -> | R     ?     R R R | -> C
P -> | R                 |
     +-------------------+
```

## Operational details

Let's consider some operational details.
We will have many producers, order of thousands.
(A producer for us is the application process, plus a forwarding agent.)
Our log system will necessarily be smaller than the production system it is serving.
So we will have multiple ingesters, and each one will need to handle writes from multiple producers.

Also, we want to serve production systems that generate a lot of log data.
Therefore, we won't make reductive assumptions about data volume.
We assume even a minimum working set of log data will be too large for a single node's storage.
Therefore, consumers will necessarily have to query multiple nodes for results.
This implies the final, time-partitioned data set will be distributed and replicated.

```
          +---+           +---+
P -> F -> | I |           | Q | --.
P -> F -> |   |           +---+   |
          +---+           +---+   '->
          +---+     ?     | Q | ----> C
P -> F -> | I |           +---+   .->
P -> F -> |   |           +---+   |
P -> F -> |   |           | Q | --'
          +---+           +---+
```

We have now introduced distribution, which means we must address coördination.

## Coördination

Coördination is the death of distributed systems.
Our log system will be coördination-free.
Let's see what that requires at each stage.

Producers -- or, more accurately, forwarders -- need to be able to connect to any ingester instance and begin emitting records.
Those records should be persisted directly to that ingester's disk, with as little intermediary processing as possible.
If an ingester dies, its forwarders should be able to simply reconnect to other instances and resume.
(During the transition, they may provide backpressure, buffer, or drop records, according to their configuration.)
This is all to say forwarders must not require knowledge about which is the "correct" ingester.
Any ingester must be equally viable.

> ★ As an optimization, highly-loaded ingesters can shed load (connections) to other ingesters.
> Ingesters will gossip load information between each other, like number of connections, iops, etc.
> Then, highly-loaded ingesters can refuse new connections, and thus redirect forwarders to more-lightly-loaded peers.
> Extremely highly-loaded ingesters can even terminate existing connections, if necessary.

Consumers need to be able to make queries without any prior knowledge about time partitions, replica allocation, etc.
Without that knowledge, this implies the queries will always be scattered to each query node, gathered, and deduplicated.
Query nodes may die at any time, or start up and be empty, so query operations must manage partial results gracefully.

> ★ As an optimization, consumers can perform read-repair.
> A query should return N copies of each matching record, where N is the replication factor.
> Any records with fewer than N copies returned may be under-replicated.
> A new segment with the suspect records can be created and replicated to the cluster.
> As a further optimization, a separate process can perform sequential queries of the timespace, for the explicit purpose of read repair.

The transfer of data between the ingest and query tier also needs care.
Ideally, any ingest node should be able to transfer segments to any/all query nodes arbitrarily,
 and do work that makes forward progress in the system as a whole.
And we must recover gracefully from failure i.e. network partitions at any stage of the transaction.
Let's look at how to shuffle data safely from the ingest tier to the query tier.

## Ingest segments

Ingesters receive N independent streams of records, from N forwarders.
Each record shall be prefixed by the ingester with a time UUID, a [ULID](https://github.com/oklog/ulid).
It's important that each record have a timestamp with reasonable precision, to create some global order.
But it's not important that the clocks are globally synced, or that the records are e.g. strictly linearizable.
Also, it's fine if records that arrive in the same minimum time window to appear out-of-order, as long as that order is stable.

```
          +---+
P -> F -> | I | -> Active: R R R...
P -> F -> |   |
P -> F -> |   |
          +---+
```

Then, we mux the records to a so-called active segment, which is a file on disk.
Once it has written B bytes, or been active for S seconds, the active segment is flushed to disk.

```
          +---+
P -> F -> | I | -> Active:  R R R...
P -> F -> |   |    Flushed: R R R R R R R R R
P -> F -> |   |    Flushed: R R R R R R R R
          +---+
```

> ☛ The ingester consumes records serially from each forwarder connection.
> The next record is consumed when the current record is successfully written to the active segment.
> And the active segment is normally synced once it is flushed.
> But producers can optionally connect to a separate port, whose handler will flush the active segment after each record is written.
> This provides stronger durability, at the expense of throughput.

Ingesters host an API which serves flushed segments.

- GET /next — returns the oldest flushed segment and marks it pending
- POST /commit?id=ID — deletes a pending segment
- POST /failed?id=ID — returns a pending segment to flushed

Segment state is controlled by file extension, and we leverage the filesystem for atomic renames.
The states are .active, .flushed, or .pending, and there is only ever a single active segment at a time per connected forwarder.

```
          +---+                     
P -> F -> | I | Active              +---+
P -> F -> |   | Active              | Q | --.
          |   |  Flushed            +---+   |        
          +---+                     +---+   '->
          +---+              ?      | Q | ----> C
P -> F -> | I | Active              +---+   .->
P -> F -> |   | Active              +---+   |
P -> F -> |   | Active              | Q | --'
          |   |  Flushed            +---+
          |   |  Flushed
          +---+
```

> ☛ Ingesters are stateful, so they should have a graceful shutdown process.
> First, they should terminate connections and close listeners.
> Then, they should wait for all flushed segments to be consumed.
> Finally, they may be shut down.

### Consume segments

The ingesters act as a sort of queue, buffering records to disk in groups called segments.
But that storage should be considered ephemeral.
Segments should be consumed as quickly as possible by the query tier.
Here, we take a page from the Prometheus playbook.
Rather than the ingesters pushing flushed segments to query nodes, the query nodes pull flushed segments from the ingesters.
This enables a coherent model for scaling.
To accept a higher ingest rate, add more ingest nodes, with faster disks.
If the ingest nodes are backing up, add more query nodes to consume from them.

Consumption is modeled with a 3-stage transaction.
The first stage is the read stage.
Each query node regularly asks each ingest node for its oldest flushed segment, via GET /next.
(This can be pure random selection, round-robin, or some more sophisticated algorithm. For now it is pure random.)
Received segments are read record-by-record, and merged into another composite segment.
This process repeats, consuming multiple segments from the ingest tier, and merging them into the same composite segment.

Once the composite segment has reached B bytes, or been active for S seconds, it is closed, and we enter the replication stage.
Replication means writing the composite segment to N distinct query nodes, where N is the replication factor.

Once the segment is confirmed replicated on N nodes, we enter the commit stage.
The query node commits the original segments on all of the ingest nodes, via POST /commit.
If the composite segment fails to replicate for any reason, the query node fails all of the original segments, via POST /failed.
In either case, the transaction is now complete, and the query node can begin its loop again.

```
Q1        I1  I2  I3
--        --  --  --
|-Next--->|   |   |
|-Next------->|   |
|-Next----------->|
|<-S1-----|   |   |
|<-S2---------|   |
|<-S3-------------|
|
|--.
|  | S1∪S2∪S3 = S4     Q2  Q3
|<-'                   --  --
|-S4------------------>|   |
|-S4---------------------->|
|<-OK------------------|   |
|<-OK----------------------|
|
|         I1  I2  I3
|         --  --  --
|-Commit->|   |   |
|-Commit----->|   |
|-Commit--------->|
|<-OK-----|   |   |
|<-OK---------|   |
|<-OK-------------|
```

Let's consider failure at each stage.

If the query node fails during the read stage, pending segments are in limbo until a timeout elapses.
After which they are made available for consumption by another query node.
If the original query node is dead forever, no problem.
If the original query node comes back, it may still have records that have already been consumed and replicated by other nodes.
In which case, duplicate records will be written to the query tier, and one or more final commits will fail.
If this happens, it's OK: the records are over-replicated, but they will be deduplicated at read time, and eventually compacted.
So commit failures should be noted, but can be safely ignored.

If the query node fails during the replication stage, it's a similar story.
Assuming the node doesn't come back, pending ingest segments will timeout and be retried by other nodes.
And if the node does come back, replication will proceed without failure, and one or more final commits will fail.
Same as before, this is OK: over-replicated records will be deduplicated at read time, and eventually compacted.

If the query node fails during the commit stage, one or more ingest segments will be stuck in pending and, again, eventually time out back to flushed.
Same as before, records will become over-replicated, deduplicated at read time, and eventually compacted.

## Query index

All queries are time-bounded, and segments are written in time-order.
But an additional index is necessary to map a time range to matching segments.
Whenever a query node writes a segment for any reason, it shall read the first and last timestamp (time UUID) from that segment.
It can then update an in-memory index, associating that segment to that time range.
An interval tree is a good data structure here.

An alternative approach is to name each file as FROM-TO, with both FROM and TO as smallest and largest time UUID in the file.
Then, given a query time range of T1–T2, segments can be selected with two simple comparisons to detect time range overlap.
Arrange two ranges (A, B) (C, D) so that A <= B, C <= D, and A <= C.
In our case, the first range is the range specified by the query, and the second range is for a given segment file.
Then, the ranges overlap, and we should query the segment file, if B >= C.

```
A--B         B >= C?
  C--D           yes 
  
A--B         B >= C?
     C--D         no
  
A-----B      B >= C?
  C-D            yes
  
A-B          B >= C?
C----D           yes
```

This gives us a way to select segment files for querying.

## Compaction

Compaction serves two purposes: deduplication of records, and de-overlapping of segments.
Duplication of records can occur during failures e.g. network partitions.
But segments will regularly and naturally overlap.
Consider 3 overlapping segments on a given query node.

```
t0             t1
+-------+       |
|   A   |       |
+-------+       |
|  +---------+  |
|  |    B    |  |
|  +---------+  |
|     +---------+
|     |    C    |
|     +---------+
```

Compaction will first merge these overlapping segments into a single übersegment.
Duplicate records can be detected by time UUID and dropped.
Then, it will re-split the übersegment on time boundaries, and produce new, non-overlapping segments.

```
t0             t1
+---------+-----+
|         |     |
|    D    |  E  |
|         |     |
+---------+-----+
```

Compaction reduces the number of segments necessary to read in order to serve queries for a given time.
In the ideal state, each time will map to exactly 1 segment.
This helps query performance by reducing the number of reads.

> ☛ Observe that compaction improves query performance, but doesn't affect either correctness or space utilization.
> Compression can be applied to segments completely orthogonally to the process described here.
> More thought and experimentation is needed.

Since records are individually addressable, read-time deduplication occurs on a per-record basis.
So the mapping of record to segment can be optimized completely independently by each node, without coördination.
And the schedule and aggressiveness of compaction can be tuned; more thought is needed here.

## Querying

Each query node hosts a query API, GET /query.
Queries may be sent to any query node API by users.
Upon receipt, the query is broadcast to every node in the query tier.
Responses are gathered, results are merged and deduplicated, and then returned to the user.

The actual grepping work is done by each query node individually.
Only the matching records are returned (streamed?) back to the query node handling the user request.

The query request has several fields.
(Note this is a work in progress.)

- From, To time.Time — bounds of query
- Q string — regexp to grep for, blank is OK and matches all records
- StatsOnly bool — if true, just return stats, without actual results

The query response has several fields.

- NodeCount int — how many query nodes were queried
- SegmentCount int — how many segments were read
- Size int — file size of segments read to produce results
- Results io.Reader — merged and time-ordered results

Setting StatsOnly true can be used to quickly "explore" a data set, and narrow down a query to a usable result set.

# Component model

This is a working draft of the components of the system.

## Processes

### forward

- ./my_application | forward ingest.mycorp.local:7651
- Should accept multiple ingest host:ports
- Should contain logic to demux a DNS record to individual instances
- Should contain failover logic in case of connection interruption
- Can choose between fast, durable, or chunked writes
- Post-MVP: more sophisticated (HTTP?) forward/ingest protocol; record enhancement

### ingest

- Receives writes from multiple forwarders
- Each record is defined to be newline \n delimited
- Prefixes each record with time UUID upon ingestion
- Appends to active segment
- Flushes active segment to disk at size and time bounds
- Serves segment API to storage tier, polling semantics
- Gossips (via memberlist) with ingest peer instances to share load stats
- Post-MVP: load spreading/shedding; streaming transmission of segments to storage tier

### store

- Polls ingest tier for flushed segments
- Merges ingest segments together
- Replicates merged segments to other store nodes
- Serves query API to clients
- Performs compaction on some interval
- Post-MVP: streaming collection of segments from ingest tier; more advanced query use-cases

## Libraries

### Ingest Log

- Abstraction for segments in the ingest tier
- Operations include create new active segment, flush, mark as pending, commit
- (I've got a reasonable prototype for this one)
- Note that this is effectively a disk-backed queue, short-term durable storage

### Store Log

- Abstraction for segments in the storage tier
- Operations include collect segments, merge, replicate, compact
- Note that this is meant to be long-term durable storage

### Cluster

- Abstraction to get various nodes talking to each other
- Not much data is necessary to share, just node identity and health
- HashiCorp's memberlist fits the bill
