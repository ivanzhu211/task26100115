package ledger

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// gateHook returns a hook that fires once: it closes entered and blocks
// until release is closed. It lets tests pause a real ledger operation
// inside its critical path, never at the call entry and never via sleep.
func gateHook(entered, release chan struct{}) (func(), func(emp int)) {
	var once sync.Once
	fire := func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	return func() { fire() }, func(emp int) { fire() }
}

// TestConcurrentSubmitsOverLimit submits several claims whose total exceeds
// the employee's funds concurrently and verifies exactly the affordable
// subset succeeds with no overdraft.
func TestConcurrentSubmitsOverLimit(t *testing.T) {
	l := New(4)
	entered := make(chan struct{})
	release := make(chan struct{})
	_, empHook := gateHook(entered, release)
	l.hookPrePublish = func(emp int) {
		if emp == 0 {
			empHook(emp)
		}
	}

	type result struct {
		id  ClaimID
		err error
	}
	results := make(chan result, 3)
	submit := func() { id, err := l.Submit(0, 6000); results <- result{id, err} }

	go submit()
	<-entered // first submit holds employee 0's lock mid-publish
	go submit()
	go submit()
	close(release)

	succeeded := 0
	var claimID ClaimID
	for i := 0; i < 3; i++ {
		r := <-results
		if r.err == nil {
			succeeded++
			claimID = r.id
		} else if !errors.Is(r.err, ErrInsufficientFunds) {
			t.Fatalf("unexpected submit error: %v", r.err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("%d submits succeeded, want exactly 1 (funds 10000, claims 6000)", succeeded)
	}
	snap := l.Snapshot()
	if sa := snap.Employees[0].Sources[1]; sa.Held != 6000 || sa.Available != 4000 {
		t.Fatalf("after submits: %+v, want held 6000 available 4000 (no overdraft)", sa)
	}
	checkInvariants(t, snap, 4)

	// Concurrent approve and cancel of the same pending claim: approve is
	// paused inside its critical section, cancel queues on the employee lock.
	entered2 := make(chan struct{})
	release2 := make(chan struct{})
	_, empHook2 := gateHook(entered2, release2)
	l.hookPrePublish = func(emp int) {
		if emp == 0 {
			empHook2(emp)
		}
	}
	approveErr := make(chan error, 1)
	cancelErr := make(chan error, 1)
	go func() { approveErr <- l.Resolve(0, claimID, DecisionApprove) }()
	<-entered2 // approve holds the lock; cancel cannot compute on stale state
	go func() { cancelErr <- l.Resolve(0, claimID, DecisionCancel) }()
	close(release2)
	if err := <-approveErr; err != nil {
		t.Fatalf("approve: %v", err)
	}
	if err := <-cancelErr; !errors.Is(err, ErrClaimNotPending) {
		t.Fatalf("cancel after approve: %v, want ErrClaimNotPending", err)
	}
	snap = l.Snapshot()
	if sa := snap.Employees[0].Sources[1]; sa.Consumed != 6000 || sa.Held != 0 {
		t.Fatalf("after approve: %+v, want consumed 6000 held 0 (no double release)", sa)
	}
	checkInvariants(t, snap, 4)

	// Reverse order: cancel wins, approve loses.
	claimID2 := mustSubmit(t, l, 0, 4000)
	entered3 := make(chan struct{})
	release3 := make(chan struct{})
	_, empHook3 := gateHook(entered3, release3)
	l.hookPrePublish = func(emp int) {
		if emp == 0 {
			empHook3(emp)
		}
	}
	go func() { cancelErr <- l.Resolve(0, claimID2, DecisionCancel) }()
	<-entered3
	go func() { approveErr <- l.Resolve(0, claimID2, DecisionApprove) }()
	close(release3)
	if err := <-cancelErr; err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := <-approveErr; !errors.Is(err, ErrClaimNotPending) {
		t.Fatalf("approve after cancel: %v, want ErrClaimNotPending", err)
	}
	snap = l.Snapshot()
	if sa := snap.Employees[0].Sources[1]; sa.Available != 4000 || sa.Consumed != 6000 || sa.Held != 0 {
		t.Fatalf("final: %+v, want available 4000 consumed 6000 held 0", sa)
	}
	checkInvariants(t, snap, 4)
}

// setupSpanningClaim builds a period-2 ledger where employee 0 has one
// pending claim spanning the old (period 1) and new (period 2) sources and
// one pending claim funded only by the new source.
func setupSpanningClaim(t *testing.T) (*Ledger, ClaimID, ClaimID) {
	t.Helper()
	l := New(2)
	l.Advance()                            // period 2
	spanning := mustSubmit(t, l, 0, 15000) // src1:10000 + src2:5000
	survivor := mustSubmit(t, l, 0, 3000)  // src2:3000
	return l, spanning, survivor
}

// TestAdvanceFirstCancelsSpanningClaim interleaves Advance between the
// resolve's computation and its publish: the resolve must internally retry
// against the migrated state and find the claim auto-cancelled.
func TestAdvanceFirstCancelsSpanningClaim(t *testing.T) {
	l, spanning, survivor := setupSpanningClaim(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	_, empHook := gateHook(entered, release)
	l.hookPrePublish = func(emp int) {
		if emp == 0 {
			empHook(emp)
		}
	}
	resolveErr := make(chan error, 1)
	go func() { resolveErr <- l.Resolve(0, spanning, DecisionApprove) }()
	<-entered // resolve computed the approval against period 2

	// The period switches while the resolve is paused mid-publish.
	if p := l.Advance(); p != 3 {
		t.Fatalf("period %d, want 3", p)
	}
	close(release)
	if err := <-resolveErr; !errors.Is(err, ErrClaimNotPending) {
		t.Fatalf("resolve after advance: %v, want ErrClaimNotPending", err)
	}

	snap := l.Snapshot()
	checkInvariants(t, snap, 2)
	es := snap.Employees[0]
	if c := findClaim(t, es, spanning); c.Status != ClaimCanceled {
		t.Fatalf("spanning claim status %d, want canceled", c.Status)
	}
	if sa := es.Sources[1]; sa.Expired != 10000 || sa.Held != 0 {
		t.Fatalf("src1 %+v, want expired 10000 held 0", sa)
	}
	if sa := es.Sources[2]; sa.Available != 7000 || sa.Held != 3000 {
		t.Fatalf("src2 %+v, want available 7000 held 3000", sa)
	}
	if c := findClaim(t, es, survivor); c.Status != ClaimPending {
		t.Fatalf("survivor claim status %d, want pending (allocation preserved)", c.Status)
	}
	if sa := es.Sources[3]; sa.Available != 10000 {
		t.Fatalf("src3 %+v, want available 10000", sa)
	}
}

// TestResolveFirstThenAdvance interleaves the resolve before the period
// switch: the approval lands in period 2 and the consumed amounts survive
// the advance untouched.
func TestResolveFirstThenAdvance(t *testing.T) {
	l, spanning, survivor := setupSpanningClaim(t)

	advEntered := make(chan struct{})
	advRelease := make(chan struct{})
	advHook, _ := gateHook(advEntered, advRelease)
	l.hookPreAdvance = advHook
	advDone := make(chan int, 1)
	go func() { advDone <- l.Advance() }()
	<-advEntered // advance is in flight but has not switched the period

	// The resolve completes entirely before the period switch.
	mustResolve(t, l, 0, spanning, DecisionApprove)
	close(advRelease)
	if p := <-advDone; p != 3 {
		t.Fatalf("period %d, want 3", p)
	}

	snap := l.Snapshot()
	checkInvariants(t, snap, 2)
	es := snap.Employees[0]
	if c := findClaim(t, es, spanning); c.Status != ClaimApproved {
		t.Fatalf("spanning claim status %d, want approved", c.Status)
	}
	if sa := es.Sources[1]; sa.Consumed != 10000 || sa.Expired != 0 {
		t.Fatalf("src1 %+v, want consumed 10000 expired 0 (consumed never expires)", sa)
	}
	if sa := es.Sources[2]; sa.Available != 2000 || sa.Consumed != 5000 || sa.Held != 3000 {
		t.Fatalf("src2 %+v, want available 2000 consumed 5000 held 3000", sa)
	}
	if c := findClaim(t, es, survivor); c.Status != ClaimPending {
		t.Fatalf("survivor claim status %d, want pending", c.Status)
	}
}

// TestSnapshotConsistentCut forces employee A's update to complete before a
// snapshot's cut and employee B's update to complete after it, and rejects
// the torn combination of stale A with fresh B.
func TestSnapshotConsistentCut(t *testing.T) {
	l := New(4)
	mustSubmit(t, l, 0, 1000) // A's update completes before the snapshot starts

	cutTaken := make(chan struct{})
	releaseSnap := make(chan struct{})
	snapHook, _ := gateHook(cutTaken, releaseSnap)
	l.hookSnapshotCut = snapHook
	snapCh := make(chan Snapshot, 1)
	go func() { snapCh <- l.Snapshot() }()
	<-cutTaken // the snapshot's clock cut is fixed here

	// B's update starts and completes while the snapshot is in flight;
	// writers are never blocked by a snapshot.
	mustSubmit(t, l, 1, 2000)
	close(releaseSnap)
	snap := <-snapCh

	// The only consistent outcome: fresh A (completed before the cut) with
	// stale B (completed after it). Stale A with fresh B would be a tear.
	if got := len(snap.Employees[0].Claims); got != 1 {
		t.Fatalf("employee A: %d claims, want 1 (update completed before cut)", got)
	}
	if got := len(snap.Employees[1].Claims); got != 0 {
		t.Fatalf("employee B: %d claims, want 0 (update completed after cut)", got)
	}
	checkInvariants(t, snap, 4)

	// A later snapshot observes both updates.
	l.hookSnapshotCut = nil
	snap = l.Snapshot()
	if len(snap.Employees[0].Claims) != 1 || len(snap.Employees[1].Claims) != 1 {
		t.Fatalf("later snapshot: claims %d/%d, want 1/1",
			len(snap.Employees[0].Claims), len(snap.Employees[1].Claims))
	}
	checkInvariants(t, snap, 4)
}

// TestPausedEmployeeDoesNotBlockOthers pauses one employee's funds
// computation mid-publish and verifies that other employees' Submit and
// Resolve, plus Advance and Snapshot, all complete.
func TestPausedEmployeeDoesNotBlockOthers(t *testing.T) {
	l := New(4)
	entered := make(chan struct{})
	release := make(chan struct{})
	_, empHook := gateHook(entered, release)
	l.hookPrePublish = func(emp int) {
		if emp == 0 {
			empHook(emp)
		}
	}
	type submitResult struct {
		id  ClaimID
		err error
	}
	subRes := make(chan submitResult, 1)
	go func() { id, err := l.Submit(0, 5000); subRes <- submitResult{id, err} }()
	<-entered // employee 0's computation is done; it is paused holding its lock

	// Other employees' Submit and Resolve complete.
	id1 := mustSubmit(t, l, 1, 1000)
	mustResolve(t, l, 1, id1, DecisionApprove)

	// Advance completes without waiting for the paused employee.
	if p := l.Advance(); p != 2 {
		t.Fatalf("period %d, want 2", p)
	}

	// Snapshot completes and consistently lacks employee 0's unfinished op.
	snap := l.Snapshot()
	if snap.Period != 2 {
		t.Fatalf("snapshot period %d, want 2", snap.Period)
	}
	if got := len(snap.Employees[0].Claims); got != 0 {
		t.Fatalf("employee 0: %d claims visible before its publish landed, want 0", got)
	}
	if got := len(snap.Employees[1].Claims); got != 1 {
		t.Fatalf("employee 1: %d claims, want 1", got)
	}
	checkInvariants(t, snap, 4)

	close(release)
	res := <-subRes
	if res.err != nil {
		t.Fatalf("paused submit: %v", res.err)
	}

	// The paused submit retried across the period boundary and was applied
	// against the migrated period-2 state, drawing from the oldest source.
	snap = l.Snapshot()
	checkInvariants(t, snap, 4)
	c := findClaim(t, snap.Employees[0], res.id)
	if c.Status != ClaimPending {
		t.Fatalf("claim status %d, want pending", c.Status)
	}
	want := []Allocation{{SourcePeriod: 1, Amount: 5000}}
	if len(c.Allocations) != 1 || c.Allocations[0] != want[0] {
		t.Fatalf("allocations %+v, want %+v", c.Allocations, want)
	}
	if sa := snap.Employees[0].Sources[1]; sa.Held != 5000 || sa.Available != 5000 {
		t.Fatalf("emp0 src1 %+v, want held 5000 available 5000", sa)
	}
}

// TestConcurrentAdvancesAdvanceExactly verifies that concurrent Advance
// calls each move the organization forward by exactly one period.
func TestConcurrentAdvancesAdvanceExactly(t *testing.T) {
	l := New(3)
	const n = 5
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Advance()
		}()
	}
	wg.Wait()
	snap := l.Snapshot()
	if snap.Period != 1+n {
		t.Fatalf("period %d, want %d", snap.Period, 1+n)
	}
	checkInvariants(t, snap, 3)
}

// TestSnapshotUnderConcurrentWrites hammers the ledger with cross-employee
// writes, advances and snapshots, verifying every snapshot is a consistent,
// conservation-preserving cut.
func TestSnapshotUnderConcurrentWrites(t *testing.T) {
	l := New(6)
	const advances = 5
	errCh := make(chan error, 128)
	var wg sync.WaitGroup

	for e := 0; e < 6; e++ {
		wg.Add(1)
		go func(emp int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id, err := l.Submit(emp, 10)
				if err != nil {
					continue // funds drained by earlier approvals
				}
				_ = l.Resolve(emp, id, Decision(i%3))
			}
		}(e)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < advances; i++ {
			l.Advance()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := verifyInvariants(l.Snapshot(), 6); err != nil {
				errCh <- fmt.Errorf("snapshot %d: %w", i, err)
			}
		}
	}()
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	snap := l.Snapshot()
	if snap.Period != 1+advances {
		t.Fatalf("final period %d, want %d", snap.Period, 1+advances)
	}
	checkInvariants(t, snap, 6)
}
