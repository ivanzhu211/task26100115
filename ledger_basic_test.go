package ledger

import "testing"

// checkConservation 校验全账本的守恒不变量：
// 每个员工每个来源的四类金额非负且合计 10000；
// 每个来源的预占等于所有待处理申请在该来源上的分配之和。
func checkConservation(t *testing.T, snap Snapshot) {
	t.Helper()
	if len(snap.Employees) != EmployeeCount {
		t.Fatalf("员工数 = %d，期望 %d", len(snap.Employees), EmployeeCount)
	}
	for ei, emp := range snap.Employees {
		reserved := make(map[int]int64)
		for _, c := range emp.Claims {
			if c.Status != Pending {
				continue
			}
			var sum int64
			for p, a := range c.Alloc {
				reserved[p] += a
				sum += a
			}
			if sum != c.Amount {
				t.Fatalf("员工 %d 申请 %d：分配合计 %d != 金额 %d", ei, c.ID, sum, c.Amount)
			}
		}
		for _, s := range emp.Sources {
			if s.Available < 0 || s.Reserved < 0 || s.Consumed < 0 || s.Expired < 0 {
				t.Fatalf("员工 %d 来源 %d 出现负额：%+v", ei, s.Period, s)
			}
			total := s.Available + s.Reserved + s.Consumed + s.Expired
			if total != PeriodGrant {
				t.Fatalf("员工 %d 来源 %d 合计 %d != %d", ei, s.Period, total, PeriodGrant)
			}
			if s.Reserved != reserved[s.Period] {
				t.Fatalf("员工 %d 来源 %d 预占 %d != 待处理分配 %d", ei, s.Period, s.Reserved, reserved[s.Period])
			}
		}
	}
}

func bucketOf(t *testing.T, emp EmployeeSnapshot, period int) SourceBucket {
	t.Helper()
	for _, s := range emp.Sources {
		if s.Period == period {
			return s
		}
	}
	t.Fatalf("来源 %d 不存在", period)
	return SourceBucket{}
}

func claimOf(t *testing.T, emp EmployeeSnapshot, id uint64) ClaimInfo {
	t.Helper()
	for _, c := range emp.Claims {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("申请 %d 不存在", id)
	return ClaimInfo{}
}

func TestBasicLifecycle(t *testing.T) {
	l := NewLedger()

	snap := l.Snapshot()
	if snap.Period != 1 {
		t.Fatalf("初始期号 = %d，期望 1", snap.Period)
	}
	checkConservation(t, snap)
	if got := bucketOf(t, snap.Employees[7], 1).Available; got != PeriodGrant {
		t.Fatalf("初始可用 = %d，期望 %d", got, PeriodGrant)
	}

	id, ok := l.Submit(7, 3000)
	if !ok || id != 1 {
		t.Fatalf("Submit = (%d, %v)，期望 (1, true)", id, ok)
	}
	snap = l.Snapshot()
	b := bucketOf(t, snap.Employees[7], 1)
	if b.Available != 7000 || b.Reserved != 3000 {
		t.Fatalf("预占后 = %+v", b)
	}

	if !l.Resolve(7, id, Approve) {
		t.Fatal("Resolve(Approve) 失败")
	}
	if l.Resolve(7, id, Cancel) {
		t.Fatal("终态后仍可 Resolve")
	}
	snap = l.Snapshot()
	b = bucketOf(t, snap.Employees[7], 1)
	if b.Consumed != 3000 || b.Reserved != 0 {
		t.Fatalf("批准后 = %+v", b)
	}
	checkConservation(t, snap)
}

func TestCrossPeriodAllocationAndAutoCancel(t *testing.T) {
	l := NewLedger()
	l.Advance() // 第 2 期：来源 1、2 各 10000

	id, ok := l.Submit(3, 15000)
	if !ok {
		t.Fatal("Submit 15000 失败")
	}
	snap := l.Snapshot()
	c := claimOf(t, snap.Employees[3], id)
	if c.Alloc[1] != 10000 || c.Alloc[2] != 5000 {
		t.Fatalf("较早来源优先分配 = %v，期望 {1:10000, 2:5000}", c.Alloc)
	}

	l.Advance() // 第 3 期：来源 1 过期，整笔申请自动取消
	snap = l.Snapshot()
	if snap.Period != 3 {
		t.Fatalf("期号 = %d，期望 3", snap.Period)
	}
	c = claimOf(t, snap.Employees[3], id)
	if c.Status != Canceled {
		t.Fatalf("跨期后申请状态 = %v，期望 Canceled", c.Status)
	}
	b1 := bucketOf(t, snap.Employees[3], 1)
	if b1.Expired != 10000 || b1.Available != 0 || b1.Reserved != 0 {
		t.Fatalf("过期来源 = %+v，期望 expired=10000", b1)
	}
	b2 := bucketOf(t, snap.Employees[3], 2)
	if b2.Available != 10000 || b2.Reserved != 0 {
		t.Fatalf("仍有效部分应返还来源 2：%+v", b2)
	}
	b3 := bucketOf(t, snap.Employees[3], 3)
	if b3.Available != 10000 {
		t.Fatalf("新一期发放 = %+v", b3)
	}
	checkConservation(t, snap)
}

func TestConsumedNeverExpires(t *testing.T) {
	l := NewLedger()
	l.Advance()
	id, ok := l.Submit(5, 15000) // {1:10000, 2:5000}
	if !ok {
		t.Fatal("Submit 失败")
	}
	if !l.Resolve(5, id, Approve) {
		t.Fatal("Approve 失败")
	}
	l.Advance() // 来源 1 过期，但已消费不过期
	snap := l.Snapshot()
	b1 := bucketOf(t, snap.Employees[5], 1)
	if b1.Consumed != 10000 || b1.Expired != 0 {
		t.Fatalf("已消费不应过期：%+v", b1)
	}
	checkConservation(t, snap)
}

func TestInsufficientFundsNoStateChange(t *testing.T) {
	l := NewLedger()
	if _, ok := l.Submit(9, 10001); ok {
		t.Fatal("超额 Submit 不应成功")
	}
	id, ok := l.Submit(9, 4000)
	if !ok {
		t.Fatal("Submit 4000 失败")
	}
	if _, ok := l.Submit(9, 6001); ok {
		t.Fatal("可用不足时应失败")
	}
	snap := l.Snapshot()
	if len(snap.Employees[9].Claims) != 1 {
		t.Fatalf("失败的 Submit 不应留下申请，claims = %d", len(snap.Employees[9].Claims))
	}
	if !l.Resolve(9, id, Reject) {
		t.Fatal("Reject 失败")
	}
	snap = l.Snapshot()
	b := bucketOf(t, snap.Employees[9], 1)
	if b.Available != 10000 || b.Reserved != 0 {
		t.Fatalf("拒绝后应全额释放：%+v", b)
	}
	checkConservation(t, snap)
}

func TestInvalidInput(t *testing.T) {
	l := NewLedger()
	if _, ok := l.Submit(-1, 100); ok {
		t.Fatal("非法员工")
	}
	if _, ok := l.Submit(EmployeeCount, 100); ok {
		t.Fatal("非法员工")
	}
	if _, ok := l.Submit(0, 0); ok {
		t.Fatal("非正金额")
	}
	if l.Resolve(0, 999, Approve) {
		t.Fatal("不存在的申请")
	}
	if l.Resolve(0, 1, Decision(99)) {
		t.Fatal("非法裁决")
	}
}
