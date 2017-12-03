package store

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Event is emitted by store components, typically when things go wrong.
type Event struct {
	Debug   bool
	Op      string
	File    string
	Error   error
	Warning error
	Msg     string
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
		levelFunc = level.Info
		keyvals   = []interface{}{"op", e.Op}
	)
	if e.Debug {
		levelFunc = level.Debug
	}
	if e.File != "" {
		keyvals = append(keyvals, "file", e.File)
	}
	if e.Warning != nil {
		levelFunc = level.Warn
		keyvals = append(keyvals, "warning", e.Warning)
	}
	if e.Error != nil {
		levelFunc = level.Error
		keyvals = append(keyvals, "error", e.Error)
	}
	if e.Msg != "" {
		keyvals = append(keyvals, "msg", e.Msg)
	}
	levelFunc(r.Logger).Log(keyvals...)
}
