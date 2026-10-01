// Package ledger implements an in-process welfare credit ledger for a single
// organization with a fixed set of employees and a freely spendable pool.
//
// All amounts are integer points. Each employee is granted 10000 points per
// period. Funds issued in period p are usable only in periods p and p+1.
package ledger

import "errors"

// ClaimID identifies a claim within the scope of a single employee.
type ClaimID uint64

// Decision is the resolution applied to a pending claim.
type Decision int

const (
	DecisionApprove Decision = iota
	DecisionReject
	DecisionCancel
)

// ClaimStatus is the lifecycle state of a claim.
type ClaimStatus int

const (
	ClaimPending ClaimStatus = iota
	ClaimApproved
	ClaimRejected
	ClaimCanceled
)

var (
	ErrInsufficientFunds = errors.New("ledger: insufficient available funds")
	ErrClaimNotFound     = errors.New("ledger: claim not found")
	ErrClaimNotPending   = errors.New("ledger: claim is not pending")
	ErrInvalidEmployee   = errors.New("ledger: invalid employee")
	ErrInvalidAmount     = errors.New("ledger: amount must be positive")
	ErrInvalidDecision   = errors.New("ledger: invalid decision")
	ErrInvalidSize       = errors.New("ledger: employee count must be positive")
)

// SourceAmounts breaks down one employee's balance for a single funding
// source (identified by the period in which it was issued).
type SourceAmounts struct {
	Available int64
	Held      int64
	Consumed  int64
	Expired   int64
}

// Allocation records how much of a claim is funded by a given source period.
type Allocation struct {
	SourcePeriod int
	Amount       int64
}

// ClaimSnapshot reports a claim's status and its per-source allocation.
type ClaimSnapshot struct {
	ID          ClaimID
	Status      ClaimStatus
	Amount      int64
	Allocations []Allocation
}

// EmployeeSnapshot is one employee's full state at the snapshot instant.
type EmployeeSnapshot struct {
	// Sources maps a source period to its amounts. Every source ever issued
	// to this employee is present and its four amounts sum to 10000.
	Sources map[int]SourceAmounts
	// Claims lists all claims ever submitted, ordered by ClaimID.
	Claims []ClaimSnapshot
}

// Snapshot is a consistent, linearizable view of the whole organization.
type Snapshot struct {
	Period    int
	Employees []EmployeeSnapshot
}
