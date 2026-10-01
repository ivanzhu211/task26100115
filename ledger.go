package ledger

import (
	"sync"
	"sync/atomic"
)

// sourceState 是单个资金来源的内部四类金额。
type sourceState struct {
	available int64
	reserved  int64
	consumed  int64
	expired   int64
}

// claim 是一笔申请的内部状态。发布后不可变，状态变更通过复制完成。
type claim struct {
	id     uint64
	amount int64
	status ClaimStatus
	alloc  map[int]int64
}

// employeeState 是单个员工的不可变状态快照（写时复制）。
type employeeState struct {
	period      int
	sources     map[int]sourceState
	claims      map[uint64]*claim
	nextClaimID uint64
}

// versionedState 把一份不可变状态绑定到全局时钟版本上，供 MVCC 快照读取。
type versionedState struct {
	version uint64
	state   *employeeState
}

// employee 是单个员工的并发单元：一把员工级互斥锁 + 原子发布的版本历史。
type employee struct {
	mu   sync.Mutex
	hist atomic.Pointer[[]versionedState]
}

// snapshotRegistry 登记进行中的 Snapshot 读版本，用于历史版本回收。
// 临界区只做 map 增删与最小值维护，不涉及资金计算或员工遍历。
type snapshotRegistry struct {
	mu     sync.Mutex
	active map[uint64]int
	min    uint64
}

// Ledger 是进程内福利额度账本。
type Ledger struct {
	epoch     atomic.Int64  // 当前期号，Advance 的唯一写点
	clock     atomic.Uint64 // 全局版本时钟，仅原子自增
	registry  snapshotRegistry
	employees [EmployeeCount]employee
}

// 测试钩子：默认 nil，仅测试注入，用于确定性地控制内部交错。
var (
	testHookBeforeCompute func(emp int) // 持有员工锁、资金计算开始前
	testHookAfterEpoch    func(emp int) // 已读期号、滚动计算前
	testHookAfterPublish  func(emp int) // 新版本已发布、员工锁释放前
)

// NewLedger 创建账本：第 1 期，每名员工 10000 分可用。
func NewLedger() *Ledger {
	l := &Ledger{}
	l.epoch.Store(1)
	l.registry.active = make(map[uint64]int)
	l.registry.min = ^uint64(0)
	for i := range l.employees {
		st := &employeeState{
			period:      1,
			sources:     map[int]sourceState{1: {available: PeriodGrant}},
			claims:      make(map[uint64]*claim),
			nextClaimID: 1,
		}
		hist := []versionedState{{version: 0, state: st}}
		l.employees[i].hist.Store(&hist)
	}
	return l
}

// cloneState 深拷贝员工状态（来源 map、申请 map、每笔申请的分配 map）。
func cloneState(st *employeeState) *employeeState {
	out := &employeeState{
		period:      st.period,
		sources:     make(map[int]sourceState, len(st.sources)+1),
		claims:      make(map[uint64]*claim, len(st.claims)),
		nextClaimID: st.nextClaimID,
	}
	for p, s := range st.sources {
		out.sources[p] = s
	}
	for id, c := range st.claims {
		alloc := make(map[int]int64, len(c.alloc))
		for p, a := range c.alloc {
			alloc[p] = a
		}
		cp := *c
		cp.alloc = alloc
		out.claims[id] = &cp
	}
	return out
}

// rollOne 把状态从第 p 期滚动到第 p+1 期（原地修改传入的可变副本）；
// 调用方必须保证传入的是可写的私有副本。
func rollOne(st *employeeState) {
	expiring := st.period - 1 // 第 p 期可用的最早来源，跨入 p+1 后过期
	if _, ok := st.sources[expiring]; ok {
		// 任一资金来源拖到过期来源的待处理申请整笔自动取消：
		// 仍有效部分返还原来源，过期部分计入已过期。
		for _, c := range st.claims {
			if c.status != Pending {
				continue
			}
			expiredPart, hit := c.alloc[expiring]
			if !hit || expiredPart == 0 {
				continue
			}
			nc := *c
			nc.status = Canceled
			st.claims[c.id] = &nc
			for p, a := range c.alloc {
				s := st.sources[p]
				s.reserved -= a
				if p == expiring {
					s.expired += a
				} else {
					s.available += a
				}
				st.sources[p] = s
			}
		}
		// 未预占的过期余额计入已过期；已消费不再结转或过期。
		// 注意重新读取：上面的整笔取消已更新过该来源。
		src := st.sources[expiring]
		src.expired += src.available
		src.available = 0
		st.sources[expiring] = src
	}
	st.period++
	st.sources[st.period] = sourceState{available: PeriodGrant}
}

// rollForward 把状态确定性滚动到 target 期（纯函数，返回深拷贝）。
func rollForward(st *employeeState, target int) *employeeState {
	out := cloneState(st)
	for out.period < target {
		rollOne(out)
	}
	return out
}

// latest 返回员工当前最新版本的状态。
func (e *employee) latest() *employeeState {
	h := e.hist.Load()
	return (*h)[len(*h)-1].state
}
