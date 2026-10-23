package outrunner

import (
	"context"
	"fmt"
	"sync"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

// GroupScheduler coordinates capacity shared by runner scale sets in one process.
// Poll limits guide GitHub routing; leases provide the hard local capacity gate.
type GroupScheduler struct {
	mu       sync.Mutex
	max      int
	pools    map[string]*groupPool
	order    []string
	turn     int
	pollTurn int
}

type groupPool struct {
	max        int
	demand     int
	leases     int
	cleaning   int
	acked      int
	ackedKnown bool
	inflight   int
	blocked    bool
	wake       chan struct{}
}

type groupLease struct {
	group    *GroupScheduler
	pool     string
	once     sync.Once
	cleaning bool
}

// NewGroupScheduler creates a process-local coordinator with a hard capacity.
func NewGroupScheduler(maxRunners int) (*GroupScheduler, error) {
	if maxRunners <= 0 {
		return nil, fmt.Errorf("group max_runners must be greater than zero")
	}
	return &GroupScheduler{max: maxRunners, pools: make(map[string]*groupPool)}, nil
}

// RegisterPool registers a scale set and its independent runner limit.
func (g *GroupScheduler) RegisterPool(name string, maxRunners int) error {
	if name == "" || maxRunners <= 0 {
		return fmt.Errorf("pool name and max_runners must be set")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if p, exists := g.pools[name]; exists {
		if p.max != maxRunners {
			return fmt.Errorf("pool %q registered with inconsistent max_runners", name)
		}
		return nil
	}
	// Preserve potentially advertised capacity from the prior session until a
	// lower limit is acknowledged in a successful poll response.
	g.pools[name] = &groupPool{max: maxRunners, acked: maxRunners, wake: make(chan struct{}, 1)}
	g.order = append(g.order, name)
	return nil
}

// SetWake installs a non-blocking notification channel for a pool.
func (g *GroupScheduler) SetWake(name string, wake chan struct{}) {
	g.mu.Lock()
	if p := g.pools[name]; p != nil {
		p.wake = wake
	}
	g.mu.Unlock()
}

func (g *GroupScheduler) notifyLocked() {
	for _, p := range g.pools {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
}

// UpdateDemand records the latest authoritative TotalAssignedJobs count.
func (g *GroupScheduler) UpdateDemand(name string, count int) {
	g.mu.Lock()
	if p := g.pools[name]; p != nil {
		p.demand = max(0, count)
		g.notifyLocked()
	}
	g.mu.Unlock()
}

func (g *GroupScheduler) BlockPool(name string) {
	g.mu.Lock()
	if p := g.pools[name]; p != nil {
		p.blocked = true
		g.notifyLocked()
	}
	g.mu.Unlock()
}

func (g *GroupScheduler) needLocked(p *groupPool) int {
	if p.blocked {
		return p.leases + p.cleaning
	}
	return p.demand + p.cleaning
}

func (g *GroupScheduler) usageLocked(p *groupPool) int {
	if p.ackedKnown {
		return max(p.acked, p.inflight, p.leases, g.needLocked(p))
	}
	return max(p.inflight, p.leases, g.needLocked(p))
}

func (g *GroupScheduler) occupiedByOthersLocked(name string) int {
	total := 0
	for otherName, p := range g.pools {
		if otherName != name {
			total += g.usageLocked(p)
		}
	}
	return total
}

func (g *GroupScheduler) nextEligibleLocked() string {
	if len(g.order) == 0 {
		return ""
	}
	for offset := range len(g.order) {
		name := g.order[(g.turn+offset)%len(g.order)]
		p := g.pools[name]
		if p.blocked || p.leases >= p.max || p.leases-p.cleaning >= p.demand {
			continue
		}
		if g.occupiedByOthersLocked(name)+p.leases+1 <= g.max {
			return name
		}
	}
	return ""
}

func (g *GroupScheduler) advanceTurnLocked(name string) {
	for i, pool := range g.order {
		if pool == name {
			g.turn = (i + 1) % len(g.order)
			return
		}
	}
}

func (g *GroupScheduler) acquire(name string) *groupLease {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := g.pools[name]
	if p == nil || p.blocked || p.leases >= p.max || p.leases-p.cleaning >= p.demand {
		return nil
	}
	eligible := g.nextEligibleLocked()
	if eligible != "" && eligible != name {
		return nil
	}
	if g.occupiedByOthersLocked(name)+p.leases+1 > g.max {
		return nil
	}
	p.leases++
	g.advanceTurnLocked(name)
	return &groupLease{group: g, pool: name}
}

func (l *groupLease) MarkCleaning() {
	if l == nil {
		return
	}
	g := l.group
	g.mu.Lock()
	if p := g.pools[l.pool]; p != nil && !l.cleaning {
		l.cleaning = true
		p.cleaning++
	}
	g.mu.Unlock()
}

func (l *groupLease) UnmarkCleaning() {
	if l == nil {
		return
	}
	g := l.group
	g.mu.Lock()
	if p := g.pools[l.pool]; p != nil && l.cleaning {
		l.cleaning = false
		if p.cleaning > 0 {
			p.cleaning--
		}
	}
	g.mu.Unlock()
}

func (l *groupLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		g := l.group
		g.mu.Lock()
		if p := g.pools[l.pool]; p != nil {
			if l.cleaning && p.cleaning > 0 {
				p.cleaning--
			}
			if p.leases > 0 {
				p.leases--
			}
			g.notifyLocked()
		}
		g.mu.Unlock()
	})
}

func (g *GroupScheduler) pollOfferLocked(name string) int {
	p := g.pools[name]
	if p == nil || p.blocked || len(g.order) == 0 {
		return 0
	}
	// First acknowledge zero capacity for every pool before offering new work.
	for _, pool := range g.pools {
		if !pool.ackedKnown {
			return 0
		}
	}
	available := g.max - g.occupiedByOthersLocked(name)
	if available <= 0 {
		return 0
	}
	holderName := g.order[g.pollTurn%len(g.order)]
	holder := g.pools[holderName]
	share := (g.max + len(g.order) - 1) / len(g.order)
	need := g.needLocked(p)
	target := 0
	if name == holderName {
		target = max(1, share, need+1)
	} else if need > 0 {
		target = share
	} else if len(g.order) <= g.max && holder.ackedKnown && holder.acked > g.needLocked(holder) {
		target = min(share, p.max)
	}
	return max(0, min(target, p.max, available))
}

func (g *GroupScheduler) beginPoll(name string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := g.pools[name]
	if p == nil {
		return 0
	}
	offer := g.pollOfferLocked(name)
	p.inflight = max(p.inflight, offer)
	return offer
}

func (g *GroupScheduler) finishPoll(name string, offer int, msg *scaleset.RunnerScaleSetMessage, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := g.pools[name]
	if p == nil || err != nil {
		return
	}
	p.inflight = 0
	p.acked = offer
	p.ackedKnown = true
	if msg != nil && msg.Statistics != nil {
		p.demand = max(0, msg.Statistics.TotalAssignedJobs)
	}
	assigned := msg != nil && len(msg.JobAssignedMessages) > 0
	holder := len(g.order) > 0 && g.order[g.pollTurn%len(g.order)] == name
	if holder && offer > 0 && !assigned && g.needLocked(p) <= p.leases {
		g.pollTurn = (g.pollTurn + 1) % len(g.order)
	}
	g.notifyLocked()
}

// GroupMessageClient applies shared limits to listener polls.
type GroupMessageClient struct {
	inner listener.Client
	group *GroupScheduler
	pool  string
}

func NewGroupMessageClient(inner listener.Client, group *GroupScheduler, pool string) *GroupMessageClient {
	return &GroupMessageClient{inner: inner, group: group, pool: pool}
}

func (c *GroupMessageClient) Session() scaleset.RunnerScaleSetSession {
	session := c.inner.Session()
	if session.Statistics != nil {
		c.group.UpdateDemand(c.pool, session.Statistics.TotalAssignedJobs)
	}
	return session
}

func (c *GroupMessageClient) GetMessage(ctx context.Context, lastMessageID, _ int) (*scaleset.RunnerScaleSetMessage, error) {
	offer := c.group.beginPoll(c.pool)
	msg, err := c.inner.GetMessage(ctx, lastMessageID, offer)
	c.group.finishPoll(c.pool, offer, msg, err)
	return msg, err
}

func (c *GroupMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	return c.inner.DeleteMessage(ctx, messageID)
}
