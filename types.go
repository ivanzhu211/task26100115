package ledger

// 固定规模：一个组织 500 名员工，每人每期发放 10000 分。
const (
	EmployeeCount = 500
	PeriodGrant   = 10000
)

// Decision 是 Resolve 对一笔待处理申请的裁决。
type Decision int

const (
	Approve Decision = iota // 批准：预占转为已消费
	Reject                  // 拒绝：释放预占
	Cancel                  // 取消：释放预占
)

// ClaimStatus 是申请的终态机状态。
type ClaimStatus int

const (
	Pending ClaimStatus = iota
	Approved
	Rejected
	Canceled
)

// SourceBucket 是单个资金来源（某一期发放）的四类金额拆分。
type SourceBucket struct {
	Period    int
	Available int64
	Reserved  int64
	Consumed  int64
	Expired   int64
}

// ClaimInfo 是一笔申请的状态与来源分配。
type ClaimInfo struct {
	ID     uint64
	Amount int64
	Status ClaimStatus
	Alloc  map[int]int64 // 来源期号 -> 预占金额（仅非零项）
}

// EmployeeSnapshot 是单个员工在快照瞬间的完整状态。
type EmployeeSnapshot struct {
	Sources []SourceBucket // 按期号升序
	Claims  []ClaimInfo    // 按申请标识升序
}

// Snapshot 是同一瞬间的全组织一致状态。
type Snapshot struct {
	Period    int
	Employees []EmployeeSnapshot
}
