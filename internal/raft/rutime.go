package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrRuntimeStopped    = errors.New("raft runtime stopped")
	ErrRuntimeBusy       = errors.New("raft runtime queue full")
	ErrUnsupportedEvent  = errors.New("raft event handler not implemented")
	ErrRuntimeNotStarted = errors.New("raft runtime not started")
	ErrRuntimeStarted    = errors.New("raft runtime already started")
)

type eventEnvelope struct {
	event Event
	reply chan eventResult
}

type eventResult struct {
	effects Effects
	err     error
}

// Runtime owns a serialized queue of Raft events.
//
// Lifecycle:
//   - NewRuntime creates the runtime.
//   - Start launches one event-processing goroutine.
//   - Submit admits events while the runtime is active.
//   - Stop prevents new submissions and terminates the loop.
//
// The events channel is never closed by Submit or Stop.
type Runtime struct {
	raft *Raft

	events chan eventEnvelope

	mu      sync.Mutex
	started bool
	stopped bool

	// Closed once shutdown begins.
	stopCh chan struct{}

	// Closed when the event-processing goroutine exits,
	// or immediately if stopped before startup.
	done chan struct{}
}

// NewRuntime creates a runtime without starting it.
func NewRuntime(r *Raft, queueSize int) (*Runtime, error) {
	if r == nil {
		return nil, errors.New("raft instance cannot be nil")
	}

	if queueSize < 0 {
		return nil, fmt.Errorf(
			"raft queue size cannot be negative: %d",
			queueSize,
		)
	}

	return &Runtime{
		raft:   r,
		events: make(chan eventEnvelope, queueSize),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}, nil
}

// Start launches exactly one event-processing goroutine.
func (rt *Runtime) Start() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.stopped {
		return ErrRuntimeStopped
	}

	if rt.started {
		return ErrRuntimeStarted
	}

	rt.started = true

	go rt.run()

	return nil
}

// Submit validates and submits an event.
//
// Admission and shutdown are serialized by rt.mu.
//
// A successfully admitted event may still be canceled
// before processing begins.
//
// Context cancellation does not undo any Raft transition
// that has already started.
func (rt *Runtime) Submit(
	ctx context.Context,
	event Event,
) (Effects, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Reject canceled requests before admission.
	if err := ctx.Err(); err != nil {
		return Effects{}, err
	}

	// Validate outside the lifecycle lock.
	if err := validateEvent(event); err != nil {
		return Effects{}, err
	}

	reply := make(chan eventResult, 1)

	envelope := eventEnvelope{
		event: event,
		reply: reply,
	}

	// Serialize event admission against Stop().
	rt.mu.Lock()

	if rt.stopped {
		rt.mu.Unlock()
		return Effects{}, ErrRuntimeStopped
	}

	if !rt.started {
		rt.mu.Unlock()
		return Effects{}, ErrRuntimeNotStarted
	}

	// Nonblocking admission while holding rt.mu.
	//
	// Stop cannot mark the runtime stopped between
	// this lifecycle check and queue admission.
	select {
	case rt.events <- envelope:
		// Event accepted.

	default:
		rt.mu.Unlock()
		return Effects{}, ErrRuntimeBusy
	}

	rt.mu.Unlock()

	// Wait for the processing result.
	//
	// The buffered reply channel allows the worker to
	// respond even if the caller has already canceled.
	select {
	case result := <-reply:
		return result.effects, result.err

	case <-ctx.Done():
		return Effects{}, ctx.Err()

	case <-rt.stopCh:
		// Prefer an available processing result.
		select {
		case result := <-reply:
			return result.effects, result.err
		default:
			return Effects{}, ErrRuntimeStopped
		}
	}
}

// run processes events sequentially.
//
// Until Task 27D-3 implements dispatching, accepted
// events receive ErrUnsupportedEvent.
//
// During shutdown, remaining queued events are rejected.
func (rt *Runtime) run() {
	defer close(rt.done)

	for {
		// Prioritize shutdown before selecting another event.
		select {
		case <-rt.stopCh:
			rt.rejectPendingEvents()
			return
		default:
		}

		select {
		case <-rt.stopCh:
			rt.rejectPendingEvents()
			return

		case envelope := <-rt.events:
			// Shutdown might have started after the
			// event was selected.
			select {
			case <-rt.stopCh:
				envelope.reply <- eventResult{
					err: ErrRuntimeStopped,
				}
				rt.rejectPendingEvents()
				return

			default:
			}

			// Placeholder until event handlers exist.
			envelope.reply <- eventResult{
				err: ErrUnsupportedEvent,
			}
		}
	}
}

// rejectPendingEvents responds to every queued event
// that has not begun processing.
//
// Stop() prevents new admission before closing stopCh,
// so no new events can enter while this queue is drained.
func (rt *Runtime) rejectPendingEvents() {
	for {
		select {
		case envelope := <-rt.events:
			envelope.reply <- eventResult{
				err: ErrRuntimeStopped,
			}

		default:
			return
		}
	}
}

// Stop prevents new submissions and waits for the event
// loop to exit.
//
// Stop is idempotent.
//
// If Stop is called before Start, the runtime transitions
// directly to the stopped state and closes done.
//
// If called after Start, the worker finishes its current
// operation and rejects pending events.
func (rt *Runtime) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	rt.mu.Lock()

	if !rt.stopped {
		rt.stopped = true
		close(rt.stopCh)

		if !rt.started {
			close(rt.done)
		}
	}

	rt.mu.Unlock()

	select {
	case <-rt.done:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}
