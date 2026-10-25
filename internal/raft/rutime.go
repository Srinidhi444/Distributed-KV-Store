package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	ErrRuntimeStopped    = errors.New("raft runtime stopped")
	ErrRuntimeBusy       = errors.New("raft runtime queue full")
	ErrUnsupportedEvent  = errors.New("raft event handler not implemented")
	ErrRuntimeNotStarted = errors.New("raft runtime not started")
	ErrRuntimeStarted    = errors.New("raft runtime already started")
)

const (
	minElectionTimeout = 300 * time.Millisecond
	maxElectionTimeout = 600 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond
)

type eventEnvelope struct {
	event Event
	reply chan eventResult
}

type eventResult struct {
	effects        Effects
	proposalIndex  uint64
	proposalTerm   uint64
	voteResponse   *RequestVoteResponse
	appendResponse *AppendEntriesResponse
	err            error
}

// Runtime processes Raft events sequentially and manages
// election timers and ordered effect delivery.
type Runtime struct {
	raft  *Raft
	peers []string

	events      chan eventEnvelope
	effectQueue *EffectQueue
	applyFn     func(LogEntry) error

	mu      sync.Mutex
	started bool
	stopped bool

	stopCh chan struct{}
	done   chan struct{}

	timerReset    chan struct{}
	timerDone     chan struct{}
	heartbeatDone chan struct{}
}

// NewRuntime creates an unstarted runtime with a bounded
// event queue and initialized lifecycle channels.
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
		raft:          r,
		events:        make(chan eventEnvelope, queueSize),
		stopCh:        make(chan struct{}),
		done:          make(chan struct{}),
		timerReset:    make(chan struct{}, 1),
		timerDone:     make(chan struct{}),
		heartbeatDone: make(chan struct{}),
	}, nil
}

// NewElectionRuntime creates a runtime with a fixed set
// of voting peers, excluding the local node.
func NewElectionRuntime(
	r *Raft,
	peers []string,
	queueSize int,
) (*Runtime, error) {
	if r == nil {
		return nil, errors.New("raft instance cannot be nil")
	}

	if err := validateElectionMembers(r.ID(), peers); err != nil {
		return nil, err
	}

	rt, err := NewRuntime(r, queueSize)
	if err != nil {
		return nil, err
	}

	rt.peers = append([]string(nil), peers...)
	return rt, nil
}

// SetEffectQueue attaches the effect executor before
// startup. The executor cannot be replaced while running.
func (rt *Runtime) SetEffectQueue(q *EffectQueue) error {
	if q == nil {
		return errors.New("effect queue cannot be nil")
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.stopped {
		return ErrRuntimeStopped
	}

	if rt.started {
		return ErrRuntimeStarted
	}

	if rt.effectQueue != nil {
		return errors.New("effect queue already configured")
	}

	rt.effectQueue = q
	return nil
}

// Start launches one event-processing goroutine and one
// randomized election-timer goroutine.
func (rt *Runtime) Start() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.stopped {
		return ErrRuntimeStopped
	}

	if rt.started {
		return ErrRuntimeStarted
	}

	if rt.effectQueue == nil {
		return errors.New("raft runtime requires an effect queue")
	}

	// Recover the committed state machine before accepting new RPCs.
	if err := rt.applyCommitted(); err != nil {
		return fmt.Errorf("recover committed entries: %w", err)
	}
	rt.started = true

	go rt.run()
	go rt.runElectionTimer()
	go rt.runHeartbeatTicker()

	return nil
}

// submit validates an event, admits it to the queue,
// and waits for processing, cancellation, or shutdown.
//
// Cancellation after admission does not undo a
// successfully persisted Raft transition.
func (rt *Runtime) submit(
	ctx context.Context,
	event Event,
) (eventResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := ctx.Err(); err != nil {
		return eventResult{}, err
	}

	if err := validateEvent(event); err != nil {
		return eventResult{}, err
	}

	envelope := eventEnvelope{
		event: event,
		reply: make(chan eventResult, 1),
	}

	rt.mu.Lock()

	if rt.stopped {
		rt.mu.Unlock()
		return eventResult{}, ErrRuntimeStopped
	}

	if !rt.started {
		rt.mu.Unlock()
		return eventResult{}, ErrRuntimeNotStarted
	}

	// Admission and shutdown are serialized by mu.
	select {
	case rt.events <- envelope:
		// Accepted.
	default:
		rt.mu.Unlock()
		return eventResult{}, ErrRuntimeBusy
	}

	rt.mu.Unlock()

	select {
	case result := <-envelope.reply:
		return result, result.err

	case <-ctx.Done():
		return eventResult{}, ctx.Err()

	case <-rt.stopCh:
		// Prefer a result already produced by the worker.
		select {
		case result := <-envelope.reply:
			return result, result.err
		default:
			return eventResult{}, ErrRuntimeStopped
		}
	}
}

// Submit handles internal events that do not require
// a synchronous RPC response.
func (rt *Runtime) Submit(
	ctx context.Context,
	event Event,
) (Effects, error) {
	if event.Type == EventRequestVote || event.Type == EventAppendEntries {
		return Effects{}, fmt.Errorf(
			"%w: use the dedicated RPC submission method",
			ErrInvalidEvent,
		)
	}

	result, err := rt.submit(ctx, event)
	if err != nil {
		return Effects{}, err
	}

	if result.voteResponse != nil || result.appendResponse != nil {
		return Effects{}, fmt.Errorf(
			"%w: unexpected RPC response",
			ErrInvalidTransition,
		)
	}

	return result.effects, nil
}

// SubmitRequestVote processes an incoming vote request
// and returns its response after durable state changes
// have completed.
func (rt *Runtime) SubmitRequestVote(
	ctx context.Context,
	from string,
	request RequestVoteRequest,
) (RequestVoteResponse, Effects, error) {
	result, err := rt.submit(ctx, Event{
		Type:        EventRequestVote,
		From:        from,
		RequestVote: &request,
	})
	if err != nil {
		return RequestVoteResponse{}, Effects{}, err
	}

	if result.voteResponse == nil {
		return RequestVoteResponse{}, Effects{},
			fmt.Errorf(
				"%w: missing RequestVote response",
				ErrInvalidTransition,
			)
	}

	return *result.voteResponse, result.effects, nil
}

// SubmitAppendEntries processes an incoming AppendEntries RPC
// through the serialized event loop. The response is returned
// only after required Raft persistence has succeeded.
func (rt *Runtime) SubmitAppendEntries(
	ctx context.Context,
	from string,
	request AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	result, err := rt.submit(ctx, Event{
		Type:          EventAppendEntries,
		From:          from,
		AppendEntries: &request,
	})
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	if result.appendResponse == nil {
		return AppendEntriesResponse{}, fmt.Errorf(
			"%w: missing AppendEntries response",
			ErrInvalidTransition,
		)
	}

	return *result.appendResponse, nil
}

// run processes one Raft event at a time, queues its
// effects in transition order, and returns the result.
//
// A failure to enqueue effects disables further Raft
// transitions because state may already be persisted.
func (rt *Runtime) run() {
	defer close(rt.done)

	for {
		// Check shutdown before beginning another event.
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
			// Failure of the external effect worker makes further Raft
			// operations unsafe even when queue admission succeeded earlier.
			if err := rt.effectQueue.Err(); err != nil {
				rt.raft.mu.Lock()
				rt.raft.failed = true
				rt.raft.mu.Unlock()
				envelope.reply <- eventResult{err: err}
				continue
			}
			select {
			case <-rt.stopCh:
				envelope.reply <- eventResult{
					err: ErrRuntimeStopped,
				}
				rt.rejectPendingEvents()
				return
			default:
			}

			outcome, err := rt.raft.dispatchElectionEvent(
				envelope.event,
				rt.peers,
			)
			if err != nil {
				envelope.reply <- eventResult{err: err}
				continue
			}

			// Apply committed entries in log order before acknowledging.
			if err := rt.applyCommitted(); err != nil {
				rt.raft.mu.Lock()
				rt.raft.failed = true
				rt.raft.mu.Unlock()
				envelope.reply <- eventResult{err: fmt.Errorf("apply committed Raft entries: %w", err)}
				continue
			}

			// Persistence has already been executed by
			// processTransition(). Queue other effects
			// before accepting the next transition.
			if err := rt.effectQueue.Enqueue(outcome.Effects); err != nil {
				rt.raft.mu.Lock()
				rt.raft.failed = true
				rt.raft.mu.Unlock()

				envelope.reply <- eventResult{
					err: fmt.Errorf(
						"enqueue raft effects: %w",
						err,
					),
				}
				continue
			}

			// Apply timer changes immediately instead
			// of waiting for the effect worker.
			if outcome.Effects.ResetElectionTimer {
				rt.resetElectionTimer()
			}

			// Effects are owned by the queue now.
			envelope.reply <- eventResult{
				voteResponse:   outcome.VoteResponse,
				appendResponse: outcome.AppendResponse,
				proposalIndex:  outcome.ProposalIndex,
				proposalTerm:   outcome.ProposalTerm,
			}
		}
	}
}

// resetElectionTimer requests a new randomized election
// deadline without blocking the event-processing loop.
//
// Multiple pending resets are coalesced into one.
func (rt *Runtime) resetElectionTimer() {
	select {
	case rt.timerReset <- struct{}{}:
	default:
	}
}

// runElectionTimer triggers election-timeout events when
// the node has not received a timer reset.
//
// Leaders skip election timeouts. Followers and
// candidates may start or retry an election.
func (rt *Runtime) runElectionTimer() {
	defer close(rt.timerDone)

	randomTimeout := func() time.Duration {
		rangeSize := int64(maxElectionTimeout - minElectionTimeout)

		return minElectionTimeout +
			time.Duration(rand.Int64N(rangeSize))
	}

	timer := time.NewTimer(randomTimeout())
	defer timer.Stop()

	for {
		select {
		case <-rt.stopCh:
			return

		case <-rt.timerReset:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomTimeout())

		case <-timer.C:
			// Only followers and candidates initiate
			// elections after a timeout.
			if rt.raft.Role() != Leader {
				ctx, cancel := context.WithTimeout(
					context.Background(),
					time.Second,
				)

				_, err := rt.Submit(ctx, Event{
					Type: EventElectionTimeout,
				})

				cancel()

				if err != nil &&
					!errors.Is(err, ErrRuntimeStopped) &&
					!errors.Is(err, context.Canceled) {
					// This minimal runtime retries on the
					// next election timeout. Fatal failures
					// remain recorded in the Raft engine.
				}
			}

			timer.Reset(randomTimeout())
		}
	}
}

// runHeartbeatTicker schedules heartbeats while this node is leader.
// Actual messages are constructed by dispatchElectionEvent and sent
// asynchronously by the effect handler.
func (rt *Runtime) runHeartbeatTicker() {
	defer close(rt.heartbeatDone)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rt.stopCh:
			return
		case <-ticker.C:
			if rt.raft.Role() != Leader {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), heartbeatInterval)
			_, _ = rt.Submit(ctx, Event{Type: EventHeartbeatTimeout})
			cancel()
		}
	}
}

// rejectPendingEvents replies with ErrRuntimeStopped
// for every queued event that has not started processing.
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

// Stop prevents new admissions, signals both workers,
// and waits until they have exited.
//
// It is idempotent and supports stopping before Start.
// It does not close the externally owned EffectQueue.
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
			close(rt.timerDone)
			close(rt.heartbeatDone)
		}
	}

	rt.mu.Unlock()

	select {
	case <-rt.done:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-rt.timerDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-rt.heartbeatDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetApplyFunc configures the ordered state-machine callback before startup.
// The callback must be idempotent: committed entries are replayed on restart.
func (rt *Runtime) SetApplyFunc(fn func(LogEntry) error) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.started || rt.stopped {
		return ErrRuntimeStarted
	}
	rt.applyFn = fn
	return nil
}

// applyCommitted executes only committed entries, including replay on restart.
// Called by the single runtime worker, or Start before workers exist.
func (rt *Runtime) applyCommitted() error {
	for {
		rt.raft.mu.RLock()
		if rt.raft.failed {
			rt.raft.mu.RUnlock()
			return ErrStorageFailed
		}
		if rt.raft.lastApplied >= rt.raft.commitIndex {
			rt.raft.mu.RUnlock()
			return nil
		}
		index := rt.raft.lastApplied + 1
		entry := rt.raft.log[index-1]
		rt.raft.mu.RUnlock()
		if entry.Command.Type != CommandNoop {
			if rt.applyFn == nil {
				return errors.New("committed KV command requires SetApplyFunc")
			}
			if err := rt.applyFn(entry); err != nil {
				return err
			}
		}
		rt.raft.mu.Lock()
		if rt.raft.lastApplied+1 == index {
			rt.raft.lastApplied = index
		}
		rt.raft.mu.Unlock()
	}
}

// Propose appends a client command and waits for majority commitment and local
// state-machine application. Context expiration makes the outcome uncertain.
func (rt *Runtime) Propose(ctx context.Context, cmd Command) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cmd.Type == CommandNoop {
		return 0, fmt.Errorf("%w: NOOP is reserved", ErrInvalidEvent)
	}
	return rt.proposeEntry(ctx, cmd)
}

// proposeEntry submits an internal or client command and waits for its
// commitment and ordered application. Internal barriers may use NOOP.
func (rt *Runtime) proposeEntry(ctx context.Context, cmd Command) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := rt.submit(ctx, Event{Type: EventClientProposal, Proposal: &cmd})
	if err != nil {
		return 0, err
	}
	if result.proposalIndex == 0 {
		return 0, ErrInvalidTransition
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		rt.raft.mu.RLock()
		applied := rt.raft.lastApplied >= result.proposalIndex
		matching := result.proposalIndex <= uint64(len(rt.raft.log)) &&
			rt.raft.log[result.proposalIndex-1].Term == result.proposalTerm &&
			rt.raft.log[result.proposalIndex-1].Command == cmd
		role, failed := rt.raft.role, rt.raft.failed
		rt.raft.mu.RUnlock()
		if failed {
			return result.proposalIndex, ErrStorageFailed
		}
		if applied && matching {
			return result.proposalIndex, nil
		}
		if role != Leader || !matching {
			return result.proposalIndex, ErrNotLeader
		}
		select {
		case <-ctx.Done():
			return result.proposalIndex, ctx.Err()
		case <-rt.stopCh:
			return result.proposalIndex, ErrRuntimeStopped
		case <-ticker.C:
		}
	}
}

// ReadBarrier commits a current-term NOOP through a majority before allowing a
// leader read. Callers must perform the KV lookup promptly and check leadership.
func (rt *Runtime) ReadBarrier(ctx context.Context) error {
	_, err := rt.proposeEntry(ctx, Command{Type: CommandNoop})
	return err
}

// LinearizableRead performs a quorum barrier and then reads the applied KV
// state. A value returned by read must come from the same synchronized state
// machine configured through SetApplyFunc.
func (rt *Runtime) LinearizableRead(ctx context.Context, read func() (string, bool)) (string, bool, error) {
	if read == nil {
		return "", false, errors.New("nil read callback")
	}
	if err := rt.ReadBarrier(ctx); err != nil {
		return "", false, err
	}
	term := rt.raft.CurrentTerm()
	value, found := read()
	rt.raft.mu.RLock()
	valid := !rt.raft.failed && rt.raft.role == Leader && rt.raft.currentTerm == term
	rt.raft.mu.RUnlock()
	if !valid {
		return "", false, ErrNotLeader
	}
	return value, found, nil
}
