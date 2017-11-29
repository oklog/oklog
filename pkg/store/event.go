package store

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Event is emitted by store components, typically when things go wrong.
type Event struct {
	Op   string // required
	File string // optional
	Err  error  // optional
	Msg  string // optional
}

// EventReporter can receive (and, presumably, do something with) Events.
type EventReporter interface {
	ReportEvent(Event)
}

// LogReporter is a default implementation of EventReporter that logs events to
// the wrapped logger. By default, events are logged at Warning level; if Err is
// non-nil, events are logged at Error level.
type LogReporter struct{ log.Logger }

// ReportEvent implements EventReporter.
func (r LogReporter) ReportEvent(e Event) {
	if e.Op == "" {
		e.Op = "undefined"
	}
	var (
		levelFunc = level.Warn
		keyvals   = []interface{}{"op", e.Op}
	)
	if e.File != "" {
		keyvals = append(keyvals, "file", e.File)
	}
	if e.Err != nil {
		levelFunc = level.Error
		keyvals = append(keyvals, "err", e.Err)
	}
	if e.Msg != "" {
		keyvals = append(keyvals, "msg", e.Msg)
	}
	levelFunc(r.Logger).Log(keyvals...)
}
