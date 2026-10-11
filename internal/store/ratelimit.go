package store

import (
	"errors"
	"sync"
	"time"

	"driftnode/internal/core"
)

// ErrRateLimitQueueFull is returned when the post queue is full. The incoming
// event is rejected, not the queued ones.
var ErrRateLimitQueueFull = errors.New("rate-limit queue full")

// rateQueueCap is the maximum number of events the post queue holds. A small
// footprint that absorbs bursts so the first rateQueueCap callers are not
// blocked; the (rateQueueCap+1)th is rejected immediately.
const rateQueueCap = 10

// signResult is the result of stamping, signing, and appending a queued
// event, sent back on the item's result channel.
type signResult struct {
	SE  *core.SignedEvent
	ID  core.EventID
	Err error
}

// queueItem is an event waiting to be stamped, signed, and appended by the
// background goroutine.
type queueItem struct {
	kp     *core.KeyPair
	log    core.LogName
	ev     core.Event
	result chan signResult
}

// postQueue decouples event production from event persistence. The caller
// enqueues an unsigned event and blocks on its result channel; a background
// goroutine drains the queue at 1/sec per kind, stamps with the real wall
// clock, signs, appends, and sends the result back. The bounded buffer
// absorbs bursts so the first rateQueueCap callers are not blocked; the
// (rateQueueCap+1)th is rejected.
//
// The goroutine is the sole writer for PostLog own events, so sequence
// numbers cannot collide between concurrent callers.
type postQueue struct {
	ch   chan queueItem
	stop chan struct{}
	done chan struct{}

	// s is the store, used to assign sequence numbers and append events.
	s *Store

	// mu protects lastSec, which is read by tryEnqueue (caller goroutine)
	// and written by process (background goroutine).
	mu sync.Mutex

	// lastSec tracks the second of the most recent event per kind, so the
	// goroutine knows when to wait vs. stamp immediately.
	lastSec map[core.Kind]int64
}

func newPostQueue(s *Store) *postQueue {
	pq := &postQueue{
		ch:      make(chan queueItem, rateQueueCap),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		s:       s,
		lastSec: make(map[core.Kind]int64),
	}
	go pq.run()
	return pq
}

// tryEnqueue attempts to add an event to the queue. Returns the result
// channel the caller should block on, or ErrRateLimitQueueFull if the queue
// is full. lastStoredSec seeds the per-kind second tracking from the own log
// on first use for that kind.
func (pq *postQueue) tryEnqueue(kp *core.KeyPair, log core.LogName, ev core.Event, lastStoredSec int64) (chan signResult, error) {
	pq.mu.Lock()
	if _, ok := pq.lastSec[ev.Kind]; !ok {
		pq.lastSec[ev.Kind] = lastStoredSec
	}
	pq.mu.Unlock()
	item := queueItem{
		kp:     kp,
		log:    log,
		ev:     ev,
		result: make(chan signResult, 1),
	}
	select {
	case pq.ch <- item:
		return item.result, nil
	default:
		return nil, ErrRateLimitQueueFull
	}
}

// run is the background goroutine that drains the queue at 1/sec per kind.
func (pq *postQueue) run() {
	defer close(pq.done)
	for {
		select {
		case <-pq.stop:
			return
		case item := <-pq.ch:
			pq.process(item)
		}
	}
}

// process stamps, signs, and appends one queued event, waiting until the
// next free second for its kind if the current second is already taken.
func (pq *postQueue) process(item queueItem) {
	pq.mu.Lock()
	last := pq.lastSec[item.ev.Kind]
	pq.mu.Unlock()
	nowSec := time.Now().Unix()
	if nowSec <= last {
		time.Sleep(time.Until(time.Unix(last+1, 0)))
	}

	se, id, err := pq.s.stampSignAppend(item.kp, item.log, item.ev)
	if err != nil {
		item.result <- signResult{Err: err}
		return
	}
	pq.mu.Lock()
	pq.lastSec[item.ev.Kind] = secondOf(se.Event.Timestamp)
	pq.mu.Unlock()
	item.result <- signResult{SE: se, ID: id}
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (pq *postQueue) Stop() {
	close(pq.stop)
	<-pq.done
}
