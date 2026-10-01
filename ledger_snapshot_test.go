package ledger

import (
	"sync"
	"testing"
)

// TestSnapshotConsistentCut 构造员工 A 更新完成后员工 B 才更新的次序：
// B 的新版本已发布（但调用尚未返回）时读取 Snapshot，
// 结果必须同时看到 A、B 的新状态，拒绝旧 A 搭配新 B 的撕裂结果。
func TestSnapshotConsistentCut(t *testing.T) {
	l := NewLedger()

	idA, ok := l.Submit(0, 100) // A 的更新先完成（实时顺序）
	if !ok {
		t.Fatal("Submit A 失败")
	}

	published := make(chan struct{})
	release := make(chan struct{})
	testHookAfterPublish = func(emp int) {
		if emp == 1 {
			close(published)
			<-release
		}
	}
	defer func() { testHookAfterPublish = nil }()

	submitDone := make(chan struct{})
	go func() {
		defer close(submitDone)
		if _, ok := l.Submit(1, 200); !ok {
			t.Error("Submit B 失败")
		}
	}()
	<-published // B 的新版本已发布，Submit 尚未返回

	snap := l.Snapshot()
	close(release)
	<-submitDone

	bB := bucketOf(t, snap.Employees[1], 1)
	if bB.Reserved != 200 {
		t.Fatalf("快照必须看到 B 已发布的新状态，reserved = %d", bB.Reserved)
	}
	bA := bucketOf(t, snap.Employees[0], 1)
	if bA.Reserved != 100 {
		t.Fatalf("撕裂结果：看到了新 B 却搭配旧 A，A.reserved = %d", bA.Reserved)
	}
	if got := claimOf(t, snap.Employees[0], idA).Status; got != Pending {
		t.Fatalf("A 的申请状态 = %v，期望 Pending", got)
	}
	checkConservation(t, snap)
}

// TestPausedEmployeeDoesNotBlockOthers 员工 A 的资金计算已开始后被暂停：
// 其他员工的 Submit、Resolve 以及全组织的 Advance、Snapshot 都必须能完成。
func TestPausedEmployeeDoesNotBlockOthers(t *testing.T) {
	l := NewLedger()

	paused := make(chan struct{})
	release := make(chan struct{})
	testHookBeforeCompute = func(emp int) {
		if emp == 0 {
			close(paused)
			<-release
		}
	}
	defer func() { testHookBeforeCompute = nil }()

	submitA := make(chan bool, 1)
	go func() {
		_, ok := l.Submit(0, 10)
		submitA <- ok
	}()
	<-paused // A 持有员工锁，资金计算已暂停

	var idB uint64
	mustComplete(t, "员工B Submit", func() {
		var ok bool
		idB, ok = l.Submit(1, 10)
		if !ok {
			t.Error("员工B Submit 失败")
		}
	})
	mustComplete(t, "员工B Resolve", func() {
		if !l.Resolve(1, idB, Approve) {
			t.Error("员工B Resolve 失败")
		}
	})
	mustComplete(t, "Advance", l.Advance)
	mustComplete(t, "Snapshot", func() {
		snap := l.Snapshot()
		if snap.Period != 2 {
			t.Errorf("快照期号 = %d，期望 2", snap.Period)
		}
	})

	close(release)
	if !<-submitA {
		t.Fatal("员工A Submit 应成功")
	}
	checkConservation(t, l.Snapshot())
}

// TestConcurrentMixedStress 多员工并发提交/裁决、并发 Advance 与并发
// Snapshot 的混合压力测试：所有快照在测试主 goroutine 中统一核对守恒。
func TestConcurrentMixedStress(t *testing.T) {
	l := NewLedger()
	const workers = 8

	snaps := make(chan Snapshot, 64)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(emp int) {
			defer wg.Done()
			var pending []uint64
			for i := 0; i < 50; i++ {
				if id, ok := l.Submit(emp, 100); ok {
					pending = append(pending, id)
				}
				if len(pending) > 0 && i%3 == 0 {
					id := pending[0]
					pending = pending[1:]
					if i%2 == 0 {
						l.Resolve(emp, id, Approve)
					} else {
						l.Resolve(emp, id, Cancel)
					}
				}
			}
			for _, id := range pending {
				l.Resolve(emp, id, Approve)
			}
		}(w)
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Advance()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			snaps <- l.Snapshot()
		}
	}()

	wg.Wait()
	close(snaps)
	for snap := range snaps {
		checkConservation(t, snap)
	}
	checkConservation(t, l.Snapshot())
}
