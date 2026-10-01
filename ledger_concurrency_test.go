package ledger

import (
	"sync"
	"testing"
	"time"
)

// mustComplete 运行 fn 并要求其在限定时间内完成；超时仅用于发现死锁，
// 不用于构造任何操作顺序。
func mustComplete(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s 未在限定时间内完成，疑似死锁", name)
	}
}

// TestConcurrentOversubmit 同一员工并发提交总额超限的多笔申请：
// 恰好 10 笔成功，无透支、无伪造的余额不足。
func TestConcurrentOversubmit(t *testing.T) {
	l := NewLedger()
	const n = 20
	type result struct {
		id uint64
		ok bool
	}
	results := make(chan result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, ok := l.Submit(0, 1000)
			results <- result{id, ok}
		}()
	}
	wg.Wait()
	close(results)

	succeeded := make(map[uint64]bool)
	for r := range results {
		if r.ok {
			if succeeded[r.id] {
				t.Fatalf("申请标识 %d 被重复使用", r.id)
			}
			succeeded[r.id] = true
		}
	}
	if len(succeeded) != 10 {
		t.Fatalf("成功 %d 笔，期望恰好 10 笔", len(succeeded))
	}
	snap := l.Snapshot()
	b := bucketOf(t, snap.Employees[0], 1)
	if b.Reserved != 10000 || b.Available != 0 {
		t.Fatalf("预占 %d、可用 %d，期望 10000 / 0", b.Reserved, b.Available)
	}
	if _, ok := l.Submit(0, 1); ok {
		t.Fatal("额度用尽后不应再提交成功")
	}
	checkConservation(t, snap)
}

// TestConcurrentResolveSameClaim 并发批准与取消同一待处理申请：
// 恰好一个裁决生效，无重复消费或释放。
func TestConcurrentResolveSameClaim(t *testing.T) {
	l := NewLedger()
	id, ok := l.Submit(0, 4000)
	if !ok {
		t.Fatal("Submit 失败")
	}

	const n = 16
	decisions := make([]Decision, n)
	for i := range decisions {
		if i%2 == 0 {
			decisions[i] = Approve
		} else {
			decisions[i] = Cancel
		}
	}
	results := make(chan bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(d Decision) {
			defer wg.Done()
			results <- l.Resolve(0, id, d)
		}(decisions[i])
	}
	wg.Wait()
	close(results)

	wins := 0
	for r := range results {
		if r {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("生效裁决 %d 个，期望恰好 1 个", wins)
	}

	snap := l.Snapshot()
	c := claimOf(t, snap.Employees[0], id)
	b := bucketOf(t, snap.Employees[0], 1)
	switch c.Status {
	case Approved:
		if b.Consumed != 4000 || b.Reserved != 0 || b.Available != 6000 {
			t.Fatalf("批准后账目错误：%+v", b)
		}
	case Canceled:
		if b.Consumed != 0 || b.Reserved != 0 || b.Available != 10000 {
			t.Fatalf("取消后账目错误：%+v", b)
		}
	default:
		t.Fatalf("申请应处于终态，实际 %v", c.Status)
	}
	checkConservation(t, snap)
}

// 准备一笔同时预占旧结转（来源 1）和本期（来源 2）资金的申请。
func setupSpanningClaim(t *testing.T, l *Ledger, emp int) uint64 {
	t.Helper()
	l.Advance() // 第 2 期：来源 1、2 各 10000
	id, ok := l.Submit(emp, 15000)
	if !ok {
		t.Fatalf("Submit 15000 失败")
	}
	snap := l.Snapshot()
	c := claimOf(t, snap.Employees[emp], id)
	if c.Alloc[1] != 10000 || c.Alloc[2] != 5000 {
		t.Fatalf("分配 = %v，期望 {1:10000, 2:5000}", c.Alloc)
	}
	return id
}

// TestResolveBeforeAdvance 裁决先发生：批准完成后才跨期，
// 已消费金额不再过期。
func TestResolveBeforeAdvance(t *testing.T) {
	l := NewLedger()
	id := setupSpanningClaim(t, l, 0)

	if !l.Resolve(0, id, Approve) {
		t.Fatal("Approve 失败")
	}
	l.Advance() // 来源 1 到期

	snap := l.Snapshot()
	c := claimOf(t, snap.Employees[0], id)
	if c.Status != Approved {
		t.Fatalf("状态 = %v，期望 Approved", c.Status)
	}
	b1 := bucketOf(t, snap.Employees[0], 1)
	if b1.Consumed != 10000 || b1.Expired != 0 {
		t.Fatalf("已消费不应过期：%+v", b1)
	}
	b2 := bucketOf(t, snap.Employees[0], 2)
	if b2.Consumed != 5000 || b2.Available != 5000 {
		t.Fatalf("来源 2 账目错误：%+v", b2)
	}
	checkConservation(t, snap)
}

// TestAdvanceDuringResolve 跨期先发生：Resolve 已持有员工锁但尚未读期号时
// Advance 完成，Resolve 读到新期号后触发整笔自动取消，裁决失败。
func TestAdvanceDuringResolve(t *testing.T) {
	l := NewLedger()
	id := setupSpanningClaim(t, l, 0)

	entered := make(chan struct{})
	release := make(chan struct{})
	testHookBeforeCompute = func(emp int) {
		if emp == 0 {
			close(entered)
			<-release
		}
	}
	defer func() { testHookBeforeCompute = nil }()

	resolveOK := make(chan bool, 1)
	go func() {
		resolveOK <- l.Resolve(0, id, Approve)
	}()
	<-entered // Resolve 已暂停在资金计算前

	mustComplete(t, "Advance", l.Advance) // 第 3 期，来源 1 到期
	close(release)

	if <-resolveOK {
		t.Fatal("跨期后 Resolve 不应生效")
	}

	snap := l.Snapshot()
	if snap.Period != 3 {
		t.Fatalf("期号 = %d，期望 3", snap.Period)
	}
	c := claimOf(t, snap.Employees[0], id)
	if c.Status != Canceled {
		t.Fatalf("状态 = %v，期望 Canceled（整笔自动取消）", c.Status)
	}
	b1 := bucketOf(t, snap.Employees[0], 1)
	if b1.Expired != 10000 || b1.Reserved != 0 || b1.Available != 0 {
		t.Fatalf("过期部分应计入已过期：%+v", b1)
	}
	b2 := bucketOf(t, snap.Employees[0], 2)
	if b2.Available != 10000 || b2.Reserved != 0 {
		t.Fatalf("仍有效部分应返还来源 2：%+v", b2)
	}
	checkConservation(t, snap)
}

// TestResolveReadsEpochBeforeAdvance Resolve 先读到旧期号，Advance 在其
// 资金计算期间完成：Resolve 线性化在 Advance 之前，批准依然生效。
func TestResolveReadsEpochBeforeAdvance(t *testing.T) {
	l := NewLedger()
	id := setupSpanningClaim(t, l, 0)

	entered := make(chan struct{})
	release := make(chan struct{})
	testHookAfterEpoch = func(emp int) {
		if emp == 0 {
			close(entered)
			<-release
		}
	}
	defer func() { testHookAfterEpoch = nil }()

	resolveOK := make(chan bool, 1)
	go func() {
		resolveOK <- l.Resolve(0, id, Approve)
	}()
	<-entered // Resolve 已读到第 2 期

	mustComplete(t, "Advance", l.Advance) // 第 3 期
	close(release)

	if !<-resolveOK {
		t.Fatal("Resolve 先读期号，批准应生效")
	}
	snap := l.Snapshot()
	c := claimOf(t, snap.Employees[0], id)
	if c.Status != Approved {
		t.Fatalf("状态 = %v，期望 Approved", c.Status)
	}
	b1 := bucketOf(t, snap.Employees[0], 1)
	if b1.Consumed != 10000 || b1.Expired != 0 {
		t.Fatalf("已消费不应过期：%+v", b1)
	}
	checkConservation(t, snap)
}
