package ledger

import (
	"errors"
	"fmt"
	"testing"
)

// checkInvariants verifies the org-wide conservation laws on a snapshot.
func checkInvariants(t *testing.T, snap Snapshot, numEmps int) {
	t.Helper()
	if err := verifyInvariants(snap, numEmps); err != nil {
		t.Fatal(err)
	}
}

// verifyInvariants checks the org-wide conservation laws on a snapshot:
//   - every issued source has non-negative amounts summing to 10000
//   - sources exist for exactly periods 1..Period
//   - per-source held equals the sum of pending claims' allocations
func verifyInvariants(snap Snapshot, numEmps int) error {
	if len(snap.Employees) != numEmps {
		return fmt.Errorf("snapshot has %d employees, want %d", len(snap.Employees), numEmps)
	}
	for i := range snap.Employees {
		es := snap.Employees[i]
		if len(es.Sources) != snap.Period {
			return fmt.Errorf("emp %d: %d sources, want %d (periods 1..%d)",
				i, len(es.Sources), snap.Period, snap.Period)
		}
		for p := 1; p <= snap.Period; p++ {
			sa, ok := es.Sources[p]
			if !ok {
				return fmt.Errorf("emp %d: missing source period %d", i, p)
			}
			if sa.Available < 0 || sa.Held < 0 || sa.Consumed < 0 || sa.Expired < 0 {
				return fmt.Errorf("emp %d src %d: negative amount %+v", i, p, sa)
			}
			if sum := sa.Available + sa.Held + sa.Consumed + sa.Expired; sum != grantAmount {
				return fmt.Errorf("emp %d src %d: amounts sum to %d, want %d", i, p, sum, grantAmount)
			}
		}
		held := map[int]int64{}
		for _, c := range es.Claims {
			var tot int64
			for _, a := range c.Allocations {
				tot += a.Amount
				if c.Status == ClaimPending {
					held[a.SourcePeriod] += a.Amount
				}
			}
			if tot != c.Amount {
				return fmt.Errorf("emp %d claim %d: allocations sum to %d, want %d",
					i, c.ID, tot, c.Amount)
			}
		}
		for p, sa := range es.Sources {
			if sa.Held != held[p] {
				return fmt.Errorf("emp %d src %d: held %d, pending allocations %d",
					i, p, sa.Held, held[p])
			}
		}
	}
	return nil
}

func mustSubmit(t *testing.T, l *Ledger, emp int, amount int64) ClaimID {
	t.Helper()
	id, err := l.Submit(emp, amount)
	if err != nil {
		t.Fatalf("Submit(%d, %d): %v", emp, amount, err)
	}
	return id
}

func mustResolve(t *testing.T, l *Ledger, emp int, id ClaimID, d Decision) {
	t.Helper()
	if err := l.Resolve(emp, id, d); err != nil {
		t.Fatalf("Resolve(%d, %d, %d): %v", emp, id, d, err)
	}
}

func findClaim(t *testing.T, es EmployeeSnapshot, id ClaimID) ClaimSnapshot {
	t.Helper()
	for _, c := range es.Claims {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("claim %d not found", id)
	return ClaimSnapshot{}
}

func TestBasicLifecycle(t *testing.T) {
	l := New(3)

	snap := l.Snapshot()
	if snap.Period != 1 {
		t.Fatalf("initial period %d, want 1", snap.Period)
	}
	checkInvariants(t, snap, 3)
	for i := range snap.Employees {
		if got := snap.Employees[i].Sources[1].Available; got != grantAmount {
			t.Fatalf("emp %d initial available %d, want %d", i, got, grantAmount)
		}
	}

	id1 := mustSubmit(t, l, 0, 4000)
	if id1 != 1 {
		t.Fatalf("first claim id %d, want 1", id1)
	}
	snap = l.Snapshot()
	if sa := snap.Employees[0].Sources[1]; sa.Available != 6000 || sa.Held != 4000 {
		t.Fatalf("after submit: %+v, want available 6000 held 4000", sa)
	}

	mustResolve(t, l, 0, id1, DecisionApprove)
	snap = l.Snapshot()
	if sa := snap.Employees[0].Sources[1]; sa.Available != 6000 || sa.Consumed != 4000 {
		t.Fatalf("after approve: %+v, want available 6000 consumed 4000", sa)
	}
	if c := findClaim(t, snap.Employees[0], id1); c.Status != ClaimApproved {
		t.Fatalf("claim status %d, want approved", c.Status)
	}

	// Insufficient funds: no state change.
	if _, err := l.Submit(0, 6001); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("oversubmit err %v, want ErrInsufficientFunds", err)
	}
	id2 := mustSubmit(t, l, 0, 6000)
	mustResolve(t, l, 0, id2, DecisionReject)
	snap = l.Snapshot()
	if sa := snap.Employees[0].Sources[1]; sa.Available != 6000 || sa.Consumed != 4000 {
		t.Fatalf("after reject: %+v, want available 6000 consumed 4000", sa)
	}
	checkInvariants(t, snap, 3)
}

func TestTerminalStateImmutable(t *testing.T) {
	l := New(2)
	id := mustSubmit(t, l, 0, 1000)
	mustResolve(t, l, 0, id, DecisionCancel)
	if err := l.Resolve(0, id, DecisionApprove); !errors.Is(err, ErrClaimNotPending) {
		t.Fatalf("re-resolve err %v, want ErrClaimNotPending", err)
	}
	if err := l.Resolve(0, id, DecisionCancel); !errors.Is(err, ErrClaimNotPending) {
		t.Fatalf("re-cancel err %v, want ErrClaimNotPending", err)
	}
	if err := l.Resolve(0, 999, DecisionApprove); !errors.Is(err, ErrClaimNotFound) {
		t.Fatalf("unknown claim err %v, want ErrClaimNotFound", err)
	}
	snap := l.Snapshot()
	if sa := snap.Employees[0].Sources[1]; sa.Available != 10000 || sa.Held != 0 {
		t.Fatalf("after cancel: %+v, want available 10000 held 0", sa)
	}
	checkInvariants(t, snap, 2)
}

func TestInvalidInputs(t *testing.T) {
	l := New(2)
	if _, err := l.Submit(-1, 100); !errors.Is(err, ErrInvalidEmployee) {
		t.Fatalf("err %v, want ErrInvalidEmployee", err)
	}
	if _, err := l.Submit(2, 100); !errors.Is(err, ErrInvalidEmployee) {
		t.Fatalf("err %v, want ErrInvalidEmployee", err)
	}
	if _, err := l.Submit(0, 0); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("err %v, want ErrInvalidAmount", err)
	}
	if _, err := l.Submit(0, -5); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("err %v, want ErrInvalidAmount", err)
	}
	if err := l.Resolve(0, 1, Decision(42)); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("err %v, want ErrInvalidDecision", err)
	}
	if err := l.Resolve(7, 1, DecisionApprove); !errors.Is(err, ErrInvalidEmployee) {
		t.Fatalf("err %v, want ErrInvalidEmployee", err)
	}
}

func TestOldestSourceFirst(t *testing.T) {
	l := New(2)
	l.Advance() // period 2: sources 1 and 2, 10000 each
	id := mustSubmit(t, l, 0, 15000)
	snap := l.Snapshot()
	c := findClaim(t, snap.Employees[0], id)
	want := []Allocation{{SourcePeriod: 1, Amount: 10000}, {SourcePeriod: 2, Amount: 5000}}
	if len(c.Allocations) != len(want) {
		t.Fatalf("allocations %+v, want %+v", c.Allocations, want)
	}
	for i, a := range c.Allocations {
		if a != want[i] {
			t.Fatalf("allocations %+v, want %+v", c.Allocations, want)
		}
	}
	if sa := snap.Employees[0].Sources[1]; sa.Available != 0 || sa.Held != 10000 {
		t.Fatalf("src1 %+v, want available 0 held 10000", sa)
	}
	if sa := snap.Employees[0].Sources[2]; sa.Available != 5000 || sa.Held != 5000 {
		t.Fatalf("src2 %+v, want available 5000 held 5000", sa)
	}
	checkInvariants(t, snap, 2)
}

func TestExpiryAndAutoCancel(t *testing.T) {
	l := New(2)
	l.Advance()                            // period 2
	spanning := mustSubmit(t, l, 0, 15000) // src1:10000 + src2:5000
	survivor := mustSubmit(t, l, 0, 3000)  // src2:3000 only

	if p := l.Advance(); p != 3 {
		t.Fatalf("period %d, want 3", p)
	}
	snap := l.Snapshot()
	checkInvariants(t, snap, 2)
	es := snap.Employees[0]

	// The spanning claim is auto-cancelled as a whole: the expired part is
	// counted as expired, the still-valid part returns to its source.
	if c := findClaim(t, es, spanning); c.Status != ClaimCanceled {
		t.Fatalf("spanning claim status %d, want canceled", c.Status)
	}
	if sa := es.Sources[1]; sa.Expired != 10000 || sa.Available != 0 || sa.Held != 0 {
		t.Fatalf("src1 %+v, want expired 10000", sa)
	}
	if sa := es.Sources[2]; sa.Available != 7000 || sa.Held != 3000 {
		t.Fatalf("src2 %+v, want available 7000 held 3000", sa)
	}
	// The other pending claim keeps its allocation.
	if c := findClaim(t, es, survivor); c.Status != ClaimPending {
		t.Fatalf("survivor claim status %d, want pending", c.Status)
	}
	if sa := es.Sources[3]; sa.Available != 10000 {
		t.Fatalf("src3 %+v, want available 10000", sa)
	}
}

func TestConsumedNeverExpires(t *testing.T) {
	l := New(1)
	id := mustSubmit(t, l, 0, 10000)
	mustResolve(t, l, 0, id, DecisionApprove)
	l.Advance()
	l.Advance()
	l.Advance() // period 4; sources 1 and 2 can no longer be spent
	snap := l.Snapshot()
	checkInvariants(t, snap, 1)
	if sa := snap.Employees[0].Sources[1]; sa.Consumed != 10000 || sa.Expired != 0 {
		t.Fatalf("src1 %+v, want consumed 10000 expired 0", sa)
	}
	if sa := snap.Employees[0].Sources[2]; sa.Expired != 10000 {
		t.Fatalf("src2 %+v, want expired 10000", sa)
	}
	if sa := snap.Employees[0].Sources[3]; sa.Available != 10000 {
		t.Fatalf("src3 %+v, want available 10000", sa)
	}
	if sa := snap.Employees[0].Sources[4]; sa.Available != 10000 {
		t.Fatalf("src4 %+v, want available 10000", sa)
	}
}

func TestLazyMigrationAcrossPeriods(t *testing.T) {
	l := New(2)
	l.Advance()
	l.Advance()
	l.Advance() // period 4; employee 0 never touched
	snap := l.Snapshot()
	checkInvariants(t, snap, 2)
	es := snap.Employees[0]
	if es.Sources[1].Expired != 10000 || es.Sources[2].Expired != 10000 {
		t.Fatalf("old sources not expired: %+v", es.Sources)
	}
	if es.Sources[3].Available != 10000 || es.Sources[4].Available != 10000 {
		t.Fatalf("usable sources wrong: %+v", es.Sources)
	}
	// A submit after lazy migration draws from the oldest usable source.
	id := mustSubmit(t, l, 0, 15000)
	snap = l.Snapshot()
	c := findClaim(t, snap.Employees[0], id)
	want := []Allocation{{SourcePeriod: 3, Amount: 10000}, {SourcePeriod: 4, Amount: 5000}}
	for i, a := range c.Allocations {
		if a != want[i] {
			t.Fatalf("allocations %+v, want %+v", c.Allocations, want)
		}
	}
	checkInvariants(t, snap, 2)
}
