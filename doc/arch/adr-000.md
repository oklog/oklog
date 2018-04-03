# ADR 0: OK Log as building block

This isn't a classic [Architectural Decision Record][adr], but rather something
to set the stage for other ADRs, describing OK Log's original motivation.

[adr]: http://thinkrelevance.com/blog/2011/11/15/documenting-architecture-decisions

## Context

If software organizations, service operators, or system operators want to
manage their own log streams, including not only shipping but storage and
querying, there are not many good options in the open-source world. The only
viable system seems to be Elasticsearch, which is difficult to operate, and not
resource-efficient for this task.

## Decision

We will implement OK Log as a comprehensive open-source "base layer", or
building block, for log management. It's most important properties will be:
that it is easy to operate; that it can scale to all reasonable sizes; that it
offers some basic usable query layer; and that it is extensible for more
sophisticated use cases.

A more complete rationale and design document is here: [OK Log][oklog]. 
A talk about the implementation is here: [Evolutionary Optimization][video].

[oklog]: https://peter.bourgon.org/ok-log/
[video]: https://www.youtube.com/watch?v=ha8gdZ27wMo

## Status

Accepted.

## Consequences

OK Log exists and is usable, but is likely not as featureful as most users
would expect, especially at the query layer.

