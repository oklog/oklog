// Package group implements an actor-runner with deterministic teardown.
package group

// Group collects actors (functions) and runs them concurrently.
// When one actor (function) returns, the others are interrupted.
// The zero value of a Group is useful.
type Group struct {
	actors []actor
}

// Add an actor (function) to the group. Each actor must be pre-emptable by an
// interrupt function. That is, if interrupt is invoked, execute should return.
// Also, it must be safe to call interrupt even after execute has returned.
//
// To add a general processing function, you can use a cancel chan.
//
//    cancel := make(chan struct{})
//    g.Add(func() error {
//        select {
//        case <-time.After(5 * time.Second):
//            return errors.New("time elapsed")
//        case <-cancel:
//            return errors.New("canceled")
//        }
//    }, func(error) {
//        close(cancel)
//    })
//
// To add an e.g. HTTP server, you'll need to provide an explicit listener, so
// that it may be interrupted.
//
//    ln, _ := net.Listen("tcp", "0.0.0.0:8080")
//    g.Add(func() error {
//        return http.Serve(ln, http.NewServeMux())
//    }, func(error) {
//        ln.Close()
//    })
//
func (g *Group) Add(execute func() error, interrupt func(error)) {
	g.actors = append(g.actors, actor{execute, interrupt})
}

// Run all actors (functions) concurrently.
// When the first actor returns, all others are interrupted.
// Run only returns when all actors (functions) have exited.
func (g *Group) Run() error {
	if len(g.actors) == 0 {
		return nil
	}

	// Run each actor.
	errors := make(chan error, len(g.actors))
	for _, a := range g.actors {
		go func(a actor) {
			errors <- a.execute()
		}(a)
	}

	// Wait for the first one to stop.
	err := <-errors

	// Signal all others to stop.
	for _, a := range g.actors {
		a.interrupt(err)
	}

	// Wait for them all to stop.
	for i := 1; i < cap(errors); i++ {
		<-errors
	}

	// Return the original error.
	return err
}

type actor struct {
	execute   func() error
	interrupt func(error)
}
