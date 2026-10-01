package ledger

import (
	"sort"
	"sync/atomic"
)

// acquire 在同一个临界区内读取全局时钟并登记读版本：
// 保证任何晚于该时钟读取的变更，其历史回收都能看到本次登记。
func (r *snapshotRegistry) acquire(clock *atomic.Uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := clock.Load()
	r.active[c]++
	if c < r.min {
		r.min = c
	}
	return c
}

func (r *snapshotRegistry) release(c uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active[c]--
	if r.active[c] == 0 {
		delete(r.active, c)
	}
	if c == r.min {
		r.min = ^uint64(0)
		for v := range r.active {
			if v < r.min {
				r.min = v
			}
		}
	}
}

// floor 返回当前进行中快照的最小读版本；无快照时返回最大 uint64。
func (r *snapshotRegistry) floor() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.min
}

// beginEmployeeOp 锁定单个员工并返回滚动到当前期的可写私有副本。
// 只触碰该员工的状态，不遍历其他员工。
func (l *Ledger) beginEmployeeOp(emp int) (*employee, *employeeState) {
	e := &l.employees[emp]
	e.mu.Lock()
	if h := testHookBeforeCompute; h != nil {
		h(emp)
	}
	period := int(l.epoch.Load())
	if h := testHookAfterEpoch; h != nil {
		h(emp)
	}
	return e, rollForward(e.latest(), period)
}

// publish 把新状态绑定到新时钟版本并原子发布，同时回收不再被任何
// 进行中快照引用的历史版本。调用方必须持有员工锁。
func (l *Ledger) publish(e *employee, emp int, st *employeeState) {
	v := l.clock.Add(1)
	floor := l.registry.floor()
	old := e.hist.Load()
	start := 0
	for i, vs := range *old {
		if vs.version <= floor {
			start = i
		}
	}
	nh := make([]versionedState, 0, len(*old)-start+1)
	nh = append(nh, (*old)[start:]...)
	nh = append(nh, versionedState{version: v, state: st})
	e.hist.Store(&nh)
	if h := testHookAfterPublish; h != nil {
		h(emp)
	}
}

func sortedSourcePeriods(st *employeeState) []int {
	periods := make([]int, 0, len(st.sources))
	for p := range st.sources {
		periods = append(periods, p)
	}
	sort.Ints(periods)
	return periods
}

// Submit 新建待处理申请并立即预占，优先使用较早来源。
// 返回该员工范围内的申请标识；可用金额不足时不改变状态。
func (l *Ledger) Submit(emp int, amount int64) (uint64, bool) {
	if emp < 0 || emp >= EmployeeCount || amount <= 0 {
		return 0, false
	}
	e, st := l.beginEmployeeOp(emp)
	defer e.mu.Unlock()

	periods := sortedSourcePeriods(st)
	var avail int64
	for _, p := range periods {
		avail += st.sources[p].available
	}
	if amount > avail {
		return 0, false // 不发布任何变更
	}

	alloc := make(map[int]int64)
	rem := amount
	for _, p := range periods { // 期号升序即较早来源优先
		if rem == 0 {
			break
		}
		s := st.sources[p]
		take := min(s.available, rem)
		if take == 0 {
			continue
		}
		s.available -= take
		s.reserved += take
		st.sources[p] = s
		alloc[p] = take
		rem -= take
	}

	id := st.nextClaimID
	st.nextClaimID++
	st.claims[id] = &claim{id: id, amount: amount, status: Pending, alloc: alloc}
	l.publish(e, emp, st)
	return id, true
}

// Resolve 仅对待处理申请执行批准、拒绝或取消；终态不可再变。
func (l *Ledger) Resolve(emp int, claimID uint64, d Decision) bool {
	if emp < 0 || emp >= EmployeeCount || d < Approve || d > Cancel {
		return false
	}
	e, st := l.beginEmployeeOp(emp)
	defer e.mu.Unlock()

	c, ok := st.claims[claimID]
	if !ok || c.status != Pending {
		return false
	}
	nc := *c
	switch d {
	case Approve:
		nc.status = Approved
	case Reject:
		nc.status = Rejected
	case Cancel:
		nc.status = Canceled
	}
	st.claims[claimID] = &nc
	for p, a := range c.alloc {
		s := st.sources[p]
		s.reserved -= a
		if d == Approve {
			s.consumed += a
		} else {
			s.available += a
		}
		st.sources[p] = s
	}
	l.publish(e, emp, st)
	return true
}

// Advance 推进一期：旧资金过期处理与新一期发放。
// 每次成功调用恰好推进一期，是全组织的一次原子切换；
// 各员工的滚动在下次被触碰时确定性完成，因此 Advance 本身
// 不接触任何员工状态，也从不被单个员工阻塞。
func (l *Ledger) Advance() {
	l.epoch.Add(1)
}

// Snapshot 返回调用到返回之间同一瞬间的全组织一致状态。
// 不加任何员工锁：按读版本取每名员工已发布的最新历史版本，
// 再纯函数滚动到读到的期号，因此不会拼接不同时间的员工状态。
func (l *Ledger) Snapshot() Snapshot {
	c := l.registry.acquire(&l.clock)
	defer l.registry.release(c)

	period := int(l.epoch.Load())
	out := Snapshot{Period: period, Employees: make([]EmployeeSnapshot, EmployeeCount)}
	for i := range l.employees {
		h := l.employees[i].hist.Load()
		idx := sort.Search(len(*h), func(j int) bool { return (*h)[j].version > c }) - 1
		st := rollForward((*h)[idx].state, period)
		out.Employees[i] = buildEmployeeSnapshot(st)
	}
	return out
}

func buildEmployeeSnapshot(st *employeeState) EmployeeSnapshot {
	periods := sortedSourcePeriods(st)
	sources := make([]SourceBucket, 0, len(periods))
	for _, p := range periods {
		s := st.sources[p]
		sources = append(sources, SourceBucket{
			Period:    p,
			Available: s.available,
			Reserved:  s.reserved,
			Consumed:  s.consumed,
			Expired:   s.expired,
		})
	}
	ids := make([]int, 0, len(st.claims))
	for id := range st.claims {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	claims := make([]ClaimInfo, 0, len(ids))
	for _, id := range ids {
		cl := st.claims[uint64(id)]
		alloc := make(map[int]int64, len(cl.alloc))
		for p, a := range cl.alloc {
			alloc[p] = a
		}
		claims = append(claims, ClaimInfo{
			ID:     cl.id,
			Amount: cl.amount,
			Status: cl.status,
			Alloc:  alloc,
		})
	}
	return EmployeeSnapshot{Sources: sources, Claims: claims}
}
