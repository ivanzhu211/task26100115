package ledger

import "sort"

// grantAmount is the per-employee grant issued every period.
const grantAmount int64 = 10000

// sourceBucket holds the four amount classes for one funding source.
type sourceBucket struct {
	available int64
	held      int64
	consumed  int64
	expired   int64
}

// claim is an immutable record of one application.
type claim struct {
	id     ClaimID
	status ClaimStatus
	amount int64
	allocs []Allocation
}

// employeeState is an immutable value describing one employee as of period.
// Published states are never mutated; every update builds a fresh value.
type employeeState struct {
	period      int
	sources     map[int]*sourceBucket
	claims      map[ClaimID]*claim
	nextClaimID ClaimID
}

// initialEmployeeState returns the state of an employee in period 1.
func initialEmployeeState() *employeeState {
	return &employeeState{
		period:      1,
		sources:     map[int]*sourceBucket{1: {available: grantAmount}},
		claims:      map[ClaimID]*claim{},
		nextClaimID: 1,
	}
}

// cloneSources deep-copies a sources map.
func cloneSources(src map[int]*sourceBucket) map[int]*sourceBucket {
	out := make(map[int]*sourceBucket, len(src)+1)
	for k, v := range src {
		c := *v
		out[k] = &c
	}
	return out
}

// cloneClaims deep-copies a claims map.
func cloneClaims(src map[ClaimID]*claim) map[ClaimID]*claim {
	out := make(map[ClaimID]*claim, len(src))
	for k, v := range src {
		c := *v
		out[k] = &c
	}
	return out
}

// migrate returns the employee state advanced from its own period to the
// target period, applying expiry, auto-cancellation and new grants.
// It is a pure function: the input is never modified.
func migrate(s *employeeState, target int) *employeeState {
	for s.period < target {
		s = migrateOne(s)
	}
	return s
}

// migrateOne advances the state by exactly one period.
func migrateOne(s *employeeState) *employeeState {
	p := s.period
	ns := &employeeState{
		period:      p + 1,
		sources:     cloneSources(s.sources),
		claims:      cloneClaims(s.claims),
		nextClaimID: s.nextClaimID,
	}
	// Funds issued in period p-1 expire when entering period p+1.
	if b, ok := ns.sources[p-1]; ok {
		b.expired += b.available
		b.available = 0
	}
	// Pending claims touching an expired source are cancelled as a whole:
	// the still-valid part returns to its source, the expired part is
	// counted as expired. Other pending claims keep their allocation.
	for _, c := range ns.claims {
		if c.status != ClaimPending {
			continue
		}
		touchesExpired := false
		for _, a := range c.allocs {
			if a.SourcePeriod <= p-1 {
				touchesExpired = true
				break
			}
		}
		if !touchesExpired {
			continue
		}
		c.status = ClaimCanceled
		for _, a := range c.allocs {
			b := ns.sources[a.SourcePeriod]
			b.held -= a.Amount
			if a.SourcePeriod <= p-1 {
				b.expired += a.Amount
			} else {
				b.available += a.Amount
			}
		}
	}
	// Issue the new period's grant.
	ns.sources[p+1] = &sourceBucket{available: grantAmount}
	return ns
}

// allocate builds the per-source allocation for amount, oldest source first.
// It reports whether the available funds suffice.
func allocate(s *employeeState, amount int64) ([]Allocation, bool) {
	origins := make([]int, 0, len(s.sources))
	var total int64
	for o, b := range s.sources {
		origins = append(origins, o)
		total += b.available
	}
	if total < amount {
		return nil, false
	}
	sort.Ints(origins)
	var allocs []Allocation
	remaining := amount
	for _, o := range origins {
		if remaining == 0 {
			break
		}
		b := s.sources[o]
		if b.available == 0 {
			continue
		}
		take := b.available
		if take > remaining {
			take = remaining
		}
		allocs = append(allocs, Allocation{SourcePeriod: o, Amount: take})
		remaining -= take
	}
	return allocs, true
}

// applySubmit returns a new state with a fresh pending claim holding amount,
// or ok=false if funds are insufficient.
func applySubmit(s *employeeState, amount int64) (ns *employeeState, id ClaimID, ok bool) {
	allocs, ok := allocate(s, amount)
	if !ok {
		return nil, 0, false
	}
	ns = &employeeState{
		period:      s.period,
		sources:     cloneSources(s.sources),
		claims:      cloneClaims(s.claims),
		nextClaimID: s.nextClaimID + 1,
	}
	for _, a := range allocs {
		b := ns.sources[a.SourcePeriod]
		b.available -= a.Amount
		b.held += a.Amount
	}
	id = s.nextClaimID
	ns.claims[id] = &claim{id: id, status: ClaimPending, amount: amount, allocs: allocs}
	return ns, id, true
}

// applyResolve returns a new state with the decision applied to the claim.
func applyResolve(s *employeeState, id ClaimID, d Decision) (*employeeState, error) {
	c, ok := s.claims[id]
	if !ok {
		return nil, ErrClaimNotFound
	}
	if c.status != ClaimPending {
		return nil, ErrClaimNotPending
	}
	ns := &employeeState{
		period:      s.period,
		sources:     cloneSources(s.sources),
		claims:      cloneClaims(s.claims),
		nextClaimID: s.nextClaimID,
	}
	nc := ns.claims[id]
	switch d {
	case DecisionApprove:
		nc.status = ClaimApproved
		for _, a := range c.allocs {
			b := ns.sources[a.SourcePeriod]
			b.held -= a.Amount
			b.consumed += a.Amount
		}
	case DecisionReject:
		nc.status = ClaimRejected
		for _, a := range c.allocs {
			b := ns.sources[a.SourcePeriod]
			b.held -= a.Amount
			b.available += a.Amount
		}
	case DecisionCancel:
		nc.status = ClaimCanceled
		for _, a := range c.allocs {
			b := ns.sources[a.SourcePeriod]
			b.held -= a.Amount
			b.available += a.Amount
		}
	default:
		return nil, ErrInvalidDecision
	}
	return ns, nil
}
