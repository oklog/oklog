# ADR 1: Multitenancy

We propose to add features OK Log to make it suitable for multi-tenant
environments.

## Context

System operators often run shared infrastructure for different workloads. For
example, a single Kubernetes cluster may serve multiple departments in an
organization, none of whom should need to know about the others.

OK Log as it exists today (v0.3.2) assumes all users (both producers and
consumers) are part of the same global namespace, and provides no way to
segment ingestion or querying.

We have at least one interesting use case, with one potential user, where
multi-tenanancy would be a requirement.

## Decision

Motivated by this new use case, we judge that adding multi-tenant features, in
addition to a handful of other longstanding feature requests, would push OK Log
in a useful direction.

The initial set of issues include:

- [Add first-class concept of topics](https://github.com/oklog/oklog/issues/113)
- Separate indexer layer for faster queries
- [Move to length-delimited records](https://github.com/oklog/oklog/issues/112)
- [Extend record identifier](https://github.com/oklog/oklog/issues/114)
- [Long-term storage](https://github.com/oklog/oklog/issues/115)

Additional issues may be filed, and these issues may be refactored or dropped
outright depending on the result of experimentation.

## Status

April 3, 2018: Accepted.

## Consequences

We hope adding these features will make OK Log more broadly useful, even in
non-multi-tenant environments. However, some features will likely introduce
incompatibility between older and newer versions of OK Log, making in-place
upgrades difficult or impossible. We will mark any such changes with a major
version bump.
