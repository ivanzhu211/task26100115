package ledger

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// empVersion is one immutable entry in an employee's version chain.
// clock is assigned before the version is published; prev may be trimmed
// by the garbage collector once no registered reader can need the tail.
type empVersion struct {
	clock  uint64
	period int
	state  *employeeState
	prev   atomic.Pointer[empVersion]
}

// advanceRecord marks one period transition in the immutable advance log.
type advanceRecord struct {
	clock  uint64
	period int
	prev   *advanceRecord
}

// empSlot is one employee's slot: its own mutex plus its published version.
type empSlot struct {
	mu     sync.Mutex
	latest atomic.Pointer[empVersion]
}

// Ledger is the in-process welfare credit ledger.
//
// Concurrency model:
//   - Each employee is guarded by its own mutex; single-employee operations
//     never touch other employees' state or any org-wide index.
//   - The global publishMu critical section only orders publishes (period
//     check, pointer store, clock bump). It never covers funds computation,
//     employee iteration, or waiting on employee locks.
//   - Advance is a lazy, atomic append to the advance log; it takes no
//     employee locks. Migration is a pure function applied on access.
//   - Snapshot reads a consistent clock cut over the immutable version
//     chains without locking employees or stopping writers.
type Ledger struct {
	employees []empSlot

	// publishMu serializes only the atomic publish step.
	publishMu sync.Mutex
	// clock is bumped once per publish, before the data is stored.
	clock atomic.Uint64
	// advances is the head of the immutable advance log.
	advances atomic.Pointer[advanceRecord]

	// readerMu guards the snapshot reader registry used for GC.
	readerMu     sync.Mutex
	readers      map[uint64]uint64
	nextReaderID uint64
	// minReaderCut caches the minimum registered reader cut (MaxUint64 if none).
	minReaderCut atomic.Uint64

	// Test hooks. Set only before concurrent use; nil in production.
	hookPrePublish  func(emp int)
	hookPreAdvance  func()
	hookSnapshotCut func()
}

// New creates a ledger with employeeCount employees in period 1.
func New(employeeCount int) *Ledger {
	if employeeCount <= 0 {
		panic(ErrInvalidSize)
	}
	l := &Ledger{
		employees: make([]empSlot, employeeCount),
		readers:   map[uint64]uint64{},
	}
	l.minReaderCut.Store(math.MaxUint64)
	l.advances.Store(&advanceRecord{clock: 0, period: 1})
	for i := range l.employees {
		l.employees[i].latest.Store(&empVersion{clock: 0, period: 1, state: initialEmployeeState()})
	}
	return l
}

// Submit creates a pending claim for amount and returns its ID within the
// employee's scope. If available funds are insufficient, the state is left
// unchanged and ErrInsufficientFunds is returned.
func (l *Ledger) Submit(emp int, amount int64) (ClaimID, error) {
	if emp < 0 || emp >= len(l.employees) {
		return 0, ErrInvalidEmployee
	}
	if amount <= 0 {
		return 0, ErrInvalidAmount
	}
	var id ClaimID
	err := l.mutate(emp, func(s *employeeState) (*employeeState, error) {
		ns, cid, ok := applySubmit(s, amount)
		if !ok {
			return nil, ErrInsufficientFunds
		}
		id = cid
		return ns, nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Resolve applies a decision (approve, reject, cancel) to a pending claim.
func (l *Ledger) Resolve(emp int, id ClaimID, d Decision) error {
	if emp < 0 || emp >= len(l.employees) {
		return ErrInvalidEmployee
	}
	if d != DecisionApprove && d != DecisionReject && d != DecisionCancel {
		return ErrInvalidDecision
	}
	return l.mutate(emp, func(s *employeeState) (*employeeState, error) {
		return applyResolve(s, id, d)
	})
}

// mutate applies a pure state transition to one employee and publishes it.
// The computation runs outside any global lock; only the final
// check-and-publish enters the tiny global critical section. If the period
// advanced concurrently, the transition is recomputed against the migrated
// state and retried internally.
func (l *Ledger) mutate(emp int, apply func(s *employeeState) (*employeeState, error)) error {
	slot := &l.employees[emp]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	for {
		head := l.advances.Load()
		cur := slot.latest.Load()
		base := migrate(cur.state, head.period)
		ns, err := apply(base)
		if err != nil {
			return err
		}
		if l.hookPrePublish != nil {
			l.hookPrePublish(emp)
		}
		l.publishMu.Lock()
		if l.advances.Load() != head {
			// The period advanced while computing; recompute against the
			// migrated state instead of publishing a stale result.
			l.publishMu.Unlock()
			continue
		}
		v := &empVersion{clock: l.clock.Add(1), period: head.period, state: ns}
		v.prev.Store(cur)
		slot.latest.Store(v)
		l.gcLocked(v)
		l.publishMu.Unlock()
		return nil
	}
}

// gcLocked trims the version chain below the oldest cut any registered
// snapshot reader can still need. It keeps every version newer than the
// minimum reader cut plus the newest version at or below it. The trim runs
// under readerMu so that it is atomic with respect to reader registration:
// a reader that registers after this trim picks a cut at or above every
// surviving version's clock, and a reader registered before it is protected
// by its own cut.
func (l *Ledger) gcLocked(head *empVersion) {
	l.readerMu.Lock()
	defer l.readerMu.Unlock()
	cut := l.minReaderCut.Load()
	n := head
	for n != nil && n.clock > cut {
		n = n.prev.Load()
	}
	if n != nil {
		n.prev.Store(nil)
	}
}

// Advance moves the organization exactly one period forward. It is a single
// atomic append to the advance log and never touches employee locks; the
// per-employee migration is applied lazily and purely on next access.
func (l *Ledger) Advance() int {
	if l.hookPreAdvance != nil {
		l.hookPreAdvance()
	}
	l.publishMu.Lock()
	head := l.advances.Load()
	rec := &advanceRecord{clock: l.clock.Add(1), period: head.period + 1, prev: head}
	l.advances.Store(rec)
	l.publishMu.Unlock()
	return rec.period
}

// Snapshot returns a consistent view of the whole organization as of a
// single clock cut taken during the call. It takes no employee locks and
// never blocks or is blocked by concurrent writes.
func (l *Ledger) Snapshot() Snapshot {
	id, cut := l.registerReader()
	defer l.unregisterReader(id)
	if l.hookSnapshotCut != nil {
		l.hookSnapshotCut()
	}
	period := periodAt(l.advances.Load(), cut)
	snap := Snapshot{Period: period, Employees: make([]EmployeeSnapshot, len(l.employees))}
	for i := range l.employees {
		v := l.employees[i].latest.Load()
		for v.clock > cut {
			v = v.prev.Load()
		}
		snap.Employees[i] = buildEmployeeSnapshot(migrate(v.state, period))
	}
	return snap
}

// periodAt finds the period in effect at the given clock cut.
func periodAt(rec *advanceRecord, cut uint64) int {
	for rec.clock > cut {
		rec = rec.prev
	}
	return rec.period
}

// registerReader pins the current clock as this reader's cut for GC.
func (l *Ledger) registerReader() (uint64, uint64) {
	l.readerMu.Lock()
	l.nextReaderID++
	id := l.nextReaderID
	cut := l.clock.Load()
	l.readers[id] = cut
	l.recomputeMinCutLocked()
	l.readerMu.Unlock()
	return id, cut
}

func (l *Ledger) unregisterReader(id uint64) {
	l.readerMu.Lock()
	delete(l.readers, id)
	l.recomputeMinCutLocked()
	l.readerMu.Unlock()
}

func (l *Ledger) recomputeMinCutLocked() {
	m := uint64(math.MaxUint64)
	for _, c := range l.readers {
		if c < m {
			m = c
		}
	}
	l.minReaderCut.Store(m)
}

// buildEmployeeSnapshot converts an immutable state into the public view.
func buildEmployeeSnapshot(s *employeeState) EmployeeSnapshot {
	es := EmployeeSnapshot{
		Sources: make(map[int]SourceAmounts, len(s.sources)),
		Claims:  make([]ClaimSnapshot, 0, len(s.claims)),
	}
	for o, b := range s.sources {
		es.Sources[o] = SourceAmounts{
			Available: b.available,
			Held:      b.held,
			Consumed:  b.consumed,
			Expired:   b.expired,
		}
	}
	ids := make([]ClaimID, 0, len(s.claims))
	for id := range s.claims {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		c := s.claims[id]
		allocs := make([]Allocation, len(c.allocs))
		copy(allocs, c.allocs)
		es.Claims = append(es.Claims, ClaimSnapshot{
			ID:          c.id,
			Status:      c.status,
			Amount:      c.amount,
			Allocations: allocs,
		})
	}
	return es
}
