package raft

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrEffectQueueClosed = errors.New("raft effect queue closed")
	ErrEffectQueueFull   = errors.New("raft effect queue full")
)

type EffectHandler func(Effects) error

// EffectQueue delivers effect batches in admission order.
//
// The handler runs on a dedicated goroutine, never on the
// Raft event-processing goroutine.
type EffectQueue struct {
	mu sync.Mutex

	queue  chan Effects
	done   chan struct{}
	handle EffectHandler

	closed bool
	err    error
}

func NewEffectQueue(
	capacity int,
	handler EffectHandler,
) (*EffectQueue, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf(
			"effect queue capacity must be positive",
		)
	}

	if handler == nil {
		return nil, errors.New("effect handler cannot be nil")
	}

	q := &EffectQueue{
		queue:  make(chan Effects, capacity),
		done:   make(chan struct{}),
		handle: handler,
	}

	go q.run()
	return q, nil
}

func (q *EffectQueue) Enqueue(effects Effects) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrEffectQueueClosed
	}

	if q.err != nil {
		return q.err
	}

	// Persistence was already performed by
	// processTransition(). Never repeat it here.
	if effects.Persist != nil {
		return errors.New(
			"effect queue cannot execute persistence effects",
		)
	}

	copied := cloneEffects(effects)

	select {
	case q.queue <- copied:
		return nil

	default:
		return ErrEffectQueueFull
	}
}

func (q *EffectQueue) run() {
	defer close(q.done)

	for effects := range q.queue {
		q.mu.Lock()
		failed := q.err != nil
		q.mu.Unlock()

		// Once a handler fails, do not execute any
		// later effect batches.
		if failed {
			continue
		}

		if err := q.handle(effects); err != nil {
			q.mu.Lock()
			if q.err == nil {
				q.err = fmt.Errorf(
					"execute raft effects: %w",
					err,
				)
			}
			q.mu.Unlock()
		}
	}
}

// Err returns the first execution failure, if any.
func (q *EffectQueue) Err() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

// Close stops admission and drains the queue.
//
// The current handler must finish for Close to return.
func (q *EffectQueue) Close() error {
	q.mu.Lock()

	if !q.closed {
		q.closed = true
		close(q.queue)
	}

	q.mu.Unlock()

	<-q.done
	return q.Err()
}

// cloneEffects prevents callers from modifying effect
// payloads after they have been queued.
func cloneEffects(src Effects) Effects {
	dst := src

	dst.Messages = make(
		[]OutboundMessage,
		len(src.Messages),
	)

	for i, message := range src.Messages {
		dst.Messages[i] = message

		if message.RequestVote != nil {
			req := *message.RequestVote
			dst.Messages[i].RequestVote = &req
		}

		if message.RequestVoteResponse != nil {
			resp := *message.RequestVoteResponse
			dst.Messages[i].RequestVoteResponse = &resp
		}

		if message.AppendEntries != nil {
			req := *message.AppendEntries

			req.Entries = append(
				[]LogEntry(nil),
				req.Entries...,
			)

			dst.Messages[i].AppendEntries = &req
		}

		if message.AppendEntriesResponse != nil {
			resp := *message.AppendEntriesResponse
			dst.Messages[i].AppendEntriesResponse = &resp
		}
	}

	dst.Apply = append(
		[]ApplyEffect(nil),
		src.Apply...,
	)

	if src.Persist != nil {
		persist := *src.Persist

		if persist.HardState != nil {
			hardState := *persist.HardState
			persist.HardState = &hardState
		}

		persist.Entries = append(
			[]LogEntry(nil),
			persist.Entries...,
		)

		dst.Persist = &persist
	}

	return dst
}
