package outrunner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

const consumerPollTimeout = 5 * time.Second

type messagePollRequest struct {
	lastMessageID int
	capacity      int
}

type messagePollResponse struct {
	message *scaleset.RunnerScaleSetMessage
	err     error
}

// blockingMessageClient lets tests observe each real wrapped poll before
// delivering its listener response.
type blockingMessageClient struct {
	requests        chan messagePollRequest
	responses       chan chan messagePollResponse
	demand          atomic.Int64
	attempts        atomic.Int64
	messageOnCancel *scaleset.RunnerScaleSetMessage
	errorOnCancel   error
}

func newBlockingMessageClient() *blockingMessageClient {
	return &blockingMessageClient{
		requests:  make(chan messagePollRequest),
		responses: make(chan chan messagePollResponse),
	}
}

func (c *blockingMessageClient) GetMessage(ctx context.Context, lastMessageID, capacity int) (*scaleset.RunnerScaleSetMessage, error) {
	c.attempts.Add(1)
	select {
	case c.requests <- messagePollRequest{lastMessageID: lastMessageID, capacity: capacity}:
	case <-ctx.Done():
		if c.errorOnCancel != nil {
			return nil, c.errorOnCancel
		}
		if c.messageOnCancel != nil {
			return c.messageOnCancel, nil
		}
		return nil, ctx.Err()
	}
	response := make(chan messagePollResponse, 1)
	select {
	case c.responses <- response:
	case <-ctx.Done():
		if c.errorOnCancel != nil {
			return nil, c.errorOnCancel
		}
		if c.messageOnCancel != nil {
			return c.messageOnCancel, nil
		}
		return nil, ctx.Err()
	}
	select {
	case result := <-response:
		return result.message, result.err
	case <-ctx.Done():
		if c.errorOnCancel != nil {
			return nil, c.errorOnCancel
		}
		if c.messageOnCancel != nil {
			return c.messageOnCancel, nil
		}
		return nil, ctx.Err()
	}
}
func (c *blockingMessageClient) DeleteMessage(context.Context, int) error { return nil }

func (c *blockingMessageClient) Session() scaleset.RunnerScaleSetSession {
	return scaleset.RunnerScaleSetSession{
		Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: int(c.demand.Load())},
	}
}

type testPoolClient struct {
	client *GroupMessageClient
	inner  *blockingMessageClient
	group  *GroupScheduler
}

func newTestGroup(t *testing.T, capacity int, names ...string) *GroupScheduler {
	t.Helper()
	group, err := NewGroupScheduler(capacity)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if err := group.RegisterPool(name, capacity); err != nil {
			t.Fatal(err)
		}
	}
	return group
}

func newTestPoolClient(group *GroupScheduler, name string) *testPoolClient {
	inner := newBlockingMessageClient()
	return &testPoolClient{client: NewGroupMessageClient(inner, group, name), inner: inner, group: group}
}

func (p *testPoolClient) deliverDemand(count int) {
	p.inner.demand.Store(int64(count))
	p.client.Session()
}

type pendingConsumerPoll struct {
	cancel  context.CancelFunc
	done    chan error
	message *scaleset.RunnerScaleSetMessage
}

func beginConsumerPoll(t *testing.T, p *testPoolClient) (*pendingConsumerPoll, int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	pending := &pendingConsumerPoll{cancel: cancel, done: make(chan error, 1)}
	go func() {
		message, err := p.client.GetMessage(ctx, 17, 99)
		pending.message = message
		pending.done <- err
	}()
	return waitForConsumerPoll(t, p, pending)
}

func beginPollWithContext(t *testing.T, p *testPoolClient, ctx context.Context) (*pendingConsumerPoll, int) {
	t.Helper()
	pending := &pendingConsumerPoll{done: make(chan error, 1)}
	go func() {
		message, err := p.client.GetMessage(ctx, 17, 99)
		pending.message = message
		pending.done <- err
	}()
	return waitForConsumerPoll(t, p, pending)
}

func waitForConsumerPoll(t *testing.T, p *testPoolClient, pending *pendingConsumerPoll) (*pendingConsumerPoll, int) {
	t.Helper()
	timer := time.NewTimer(consumerPollTimeout)
	defer timer.Stop()
	select {
	case request := <-p.inner.requests:
		if request.lastMessageID != 17 {
			if pending.cancel != nil {
				pending.cancel()
			}
			t.Fatalf("listener last message ID = %d, want 17", request.lastMessageID)
		}
		return pending, request.capacity
	case <-timer.C:
		if pending.cancel != nil {
			pending.cancel()
		}
		t.Fatal("timed out waiting for wrapped listener poll")
		return nil, 0
	}
}

func completeConsumerPoll(t *testing.T, p *testPoolClient, pending *pendingConsumerPoll, message *scaleset.RunnerScaleSetMessage, err error) error {
	t.Helper()
	if pending.cancel != nil {
		defer pending.cancel()
	}
	timer := time.NewTimer(consumerPollTimeout)
	defer timer.Stop()
	var response chan messagePollResponse
	select {
	case response = <-p.inner.responses:
	case <-timer.C:
		t.Fatal("timed out waiting for listener poll response channel")
	}
	select {
	case response <- messagePollResponse{message: message, err: err}:
	case <-timer.C:
		t.Fatal("timed out delivering listener poll response")
	}
	select {
	case pollErr := <-pending.done:
		return pollErr
	case <-timer.C:
		t.Fatal("timed out waiting for wrapped listener poll to finish")
		return nil
	}
}

func pollMessage(assigned int) *scaleset.RunnerScaleSetMessage {
	return &scaleset.RunnerScaleSetMessage{
		Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: assigned},
	}
}

func acknowledgeStartupAtZero(t *testing.T, pools ...*testPoolClient) {
	t.Helper()
	for _, pool := range pools {
		pending, capacity := beginConsumerPoll(t, pool)
		if capacity != 0 {
			completeConsumerPoll(t, pool, pending, nil, nil)
			t.Fatalf("startup poll capacity = %d, want 0 until every pool acknowledges zero", capacity)
		}
		if err := completeConsumerPoll(t, pool, pending, pollMessage(0), nil); err != nil {
			t.Fatalf("startup poll failed: %v", err)
		}
	}
}

func TestGroupPollsZeroUntilEveryPoolAcknowledgesZero(t *testing.T) {
	group := newTestGroup(t, 2, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")

	pending, capacity := beginConsumerPoll(t, a)
	if capacity != 0 {
		t.Fatalf("first A poll capacity = %d, want 0", capacity)
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}

	pending, capacity = beginConsumerPoll(t, b)
	if capacity != 0 {
		t.Fatalf("first B poll capacity = %d, want 0", capacity)
	}
	pollErr := errors.New("initial poll failed")
	if err := completeConsumerPoll(t, b, pending, nil, pollErr); !errors.Is(err, pollErr) {
		t.Fatalf("failed startup poll error = %v, want %v", err, pollErr)
	}

	pending, capacity = beginConsumerPoll(t, a)
	if capacity != 0 {
		completeConsumerPoll(t, a, pending, nil, nil)
		t.Fatalf("A capacity before B's successful zero acknowledgement = %d, want 0", capacity)
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}

	pending, capacity = beginConsumerPoll(t, b)
	if capacity != 0 {
		completeConsumerPoll(t, b, pending, nil, nil)
		t.Fatalf("B retry capacity = %d, want 0", capacity)
	}
	if err := completeConsumerPoll(t, b, pending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}

	pending, capacity = beginConsumerPoll(t, a)
	if capacity == 0 {
		completeConsumerPoll(t, a, pending, nil, nil)
		t.Fatal("A received no offer after both pools acknowledged zero")
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}
}

func TestGroupCountsInFlightOfferBeforePollResponse(t *testing.T) {
	group := newTestGroup(t, 3, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)

	aPending, aCapacity := beginConsumerPoll(t, a)
	if aCapacity != 2 {
		t.Fatalf("A's first offer = %d, want 2", aCapacity)
	}
	// A has not received a response; B's offer must count A's in-flight offer.
	b.deliverDemand(1)
	bPending, bCapacity := beginConsumerPoll(t, b)
	if bCapacity != 1 {
		completeConsumerPoll(t, b, bPending, nil, nil)
		t.Fatalf("B offer while A's 2-slot poll is in flight = %d, want 1", bCapacity)
	}
	if err := completeConsumerPoll(t, b, bPending, pollMessage(1), nil); err != nil {
		t.Fatal(err)
	}
	if err := completeConsumerPoll(t, a, aPending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}
}

func TestGroupFailedDowngradeKeepsAcknowledgedReservation(t *testing.T) {
	group := newTestGroup(t, 3, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)

	a.deliverDemand(1)
	pending, capacity := beginConsumerPoll(t, a)
	if capacity != 2 {
		t.Fatalf("A initial demand offer = %d, want 2", capacity)
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(1), nil); err != nil {
		t.Fatal(err)
	}

	// B's successful zero-capacity response acknowledges the reservation.
	pending, capacity = beginConsumerPoll(t, b)
	if capacity != 1 {
		t.Fatalf("B initial poll offer = %d, want 1", capacity)
	}
	if err := completeConsumerPoll(t, b, pending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}

	b.deliverDemand(2)
	pending, capacity = beginConsumerPoll(t, a)
	if capacity != 1 {
		t.Fatalf("A reduced offer = %d, want 1", capacity)
	}
	pollErr := errors.New("downgrade poll failed")
	if err := completeConsumerPoll(t, a, pending, nil, pollErr); !errors.Is(err, pollErr) {
		t.Fatalf("failed downgrade error = %v, want %v", err, pollErr)
	}

	pending, capacity = beginConsumerPoll(t, b)
	if capacity != 1 {
		completeConsumerPoll(t, b, pending, nil, nil)
		t.Fatalf("B offer after A's failed downgrade = %d, want 1 while A's old 2-slot reservation remains", capacity)
	}
	if err := completeConsumerPoll(t, b, pending, pollMessage(2), nil); err != nil {
		t.Fatal(err)
	}
}

func TestGroupAssignedResponseKeepsPollTurnAtomic(t *testing.T) {
	group := newTestGroup(t, 2, "a", "b", "c")
	pools := map[string]*testPoolClient{
		"a": newTestPoolClient(group, "a"),
		"b": newTestPoolClient(group, "b"),
		"c": newTestPoolClient(group, "c"),
	}
	acknowledgeStartupAtZero(t, pools["a"], pools["b"], pools["c"])

	pending, capacity := beginConsumerPoll(t, pools["a"])
	if capacity != 1 {
		t.Fatalf("A discovery offer = %d, want 1", capacity)
	}
	assigned := &scaleset.RunnerScaleSetMessage{
		Statistics:          &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAssignedMessages: []*scaleset.JobAssigned{{}},
	}
	if err := completeConsumerPoll(t, pools["a"], pending, assigned, nil); err != nil {
		t.Fatal(err)
	}

	pending, capacity = beginConsumerPoll(t, pools["b"])
	if capacity != 0 {
		completeConsumerPoll(t, pools["b"], pending, nil, nil)
		t.Fatalf("B offer after A received an assignment = %d, want 0 while A keeps the poll turn", capacity)
	}
	if err := completeConsumerPoll(t, pools["b"], pending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}

	pending, capacity = beginConsumerPoll(t, pools["c"])
	if capacity != 0 {
		completeConsumerPoll(t, pools["c"], pending, nil, nil)
		t.Fatalf("C offer after A received an assignment = %d, want 0 until A acknowledges its assignment", capacity)
	}
	if err := completeConsumerPoll(t, pools["c"], pending, pollMessage(0), nil); err != nil {
		t.Fatal(err)
	}
}

func TestGroupCleaningAndAssignedDemandBothReserveCapacity(t *testing.T) {
	group, err := NewGroupScheduler(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := group.RegisterPool("a", 1); err != nil {
		t.Fatal(err)
	}
	if err := group.RegisterPool("b", 3); err != nil {
		t.Fatal(err)
	}
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)
	a.deliverDemand(1)
	b.deliverDemand(1)

	lease := group.acquire("a")
	if lease == nil {
		t.Fatal("A failed to acquire its assigned runner")
	}
	defer lease.Release()
	lease.MarkCleaning()

	pending, capacity := beginConsumerPoll(t, a)
	if capacity != 1 {
		t.Fatalf("A's per-pool limited offer = %d, want 1", capacity)
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(1), nil); err != nil {
		t.Fatal(err)
	}

	pending, capacity = beginConsumerPoll(t, b)
	if capacity != 1 {
		completeConsumerPoll(t, b, pending, nil, nil)
		t.Fatalf("B offer with A's assigned and cleaning slots = %d, want 1", capacity)
	}
	if err := completeConsumerPoll(t, b, pending, pollMessage(1), nil); err != nil {
		t.Fatal(err)
	}
}

func TestGroupPollTurnProgressesWhenPoolsOutnumberCapacity(t *testing.T) {
	group := newTestGroup(t, 2, "a", "b", "c")
	pools := map[string]*testPoolClient{
		"a": newTestPoolClient(group, "a"),
		"b": newTestPoolClient(group, "b"),
		"c": newTestPoolClient(group, "c"),
	}
	acknowledgeStartupAtZero(t, pools["a"], pools["b"], pools["c"])

	seen := make(map[string]bool)
	for cycle := range 4 {
		for _, name := range []string{"a", "b", "c"} {
			pending, capacity := beginConsumerPoll(t, pools[name])
			if capacity > 1 {
				completeConsumerPoll(t, pools[name], pending, nil, nil)
				t.Fatalf("cycle %d pool %s offer = %d, exceeds ceil(group capacity / pool count) = 1", cycle+1, name, capacity)
			}
			if capacity > 0 {
				seen[name] = true
			}
			if err := completeConsumerPoll(t, pools[name], pending, pollMessage(0), nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range []string{"a", "b", "c"} {
		if !seen[name] {
			t.Errorf("pool %q never received a cyclic discovery offer: %#v", name, seen)
		}
	}
}

func TestGroupUtilizesCapacityForDeliveredDemand(t *testing.T) {
	tests := []struct {
		name    string
		demandA int
		demandB int
	}{
		{name: "two plus zero", demandA: 2},
		{name: "one plus one", demandA: 1, demandB: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			group := newTestGroup(t, 2, "a", "b")
			a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
			acknowledgeStartupAtZero(t, a, b)
			a.deliverDemand(test.demandA)
			b.deliverDemand(test.demandB)

			pending, capacity := beginConsumerPoll(t, a)
			if capacity > test.demandA {
				completeConsumerPoll(t, a, pending, nil, nil)
				t.Fatalf("A offer with demand %d+%d = %d, exceeds A's delivered demand", test.demandA, test.demandB, capacity)
			}
			if capacity == 0 && test.demandA > 0 {
				completeConsumerPoll(t, a, pending, nil, nil)
				t.Fatalf("A received no capacity for delivered demand %d+%d", test.demandA, test.demandB)
			}
			if err := completeConsumerPoll(t, a, pending, pollMessage(test.demandA), nil); err != nil {
				t.Fatal(err)
			}

			leasesA := make([]*groupLease, 0, test.demandA)
			for range test.demandA {
				lease := group.acquire("a")
				if lease == nil {
					t.Fatalf("A could not utilize delivered demand %d+%d", test.demandA, test.demandB)
				}
				leasesA = append(leasesA, lease)
			}
			defer func() {
				for _, lease := range leasesA {
					lease.Release()
				}
			}()

			if test.demandB == 1 {
				pending, capacity = beginConsumerPoll(t, b)
				if capacity != 1 {
					completeConsumerPoll(t, b, pending, nil, nil)
					t.Fatalf("B offer with one slot already used by A = %d, want 1", capacity)
				}
				if err := completeConsumerPoll(t, b, pending, pollMessage(1), nil); err != nil {
					t.Fatal(err)
				}
				lease := group.acquire("b")
				if lease == nil {
					t.Fatal("B could not use its delivered one-job demand alongside A")
				}
				defer lease.Release()
			}
		})
	}
}

func TestGroupBusyPoolYieldsPollTokenToWaitingPool(t *testing.T) {
	group := newTestGroup(t, 3, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)
	a.deliverDemand(2)
	b.deliverDemand(1)

	pending, capacity := beginConsumerPoll(t, a)
	if capacity != 2 {
		t.Fatalf("A discovery offer = %d, want 2 with B's demand reserved", capacity)
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(2), nil); err != nil {
		t.Fatal(err)
	}

	first := group.acquire("a")
	initialB := group.acquire("b")
	second := group.acquire("a")
	if first == nil || second == nil || initialB == nil {
		t.Fatal("round-robin admission did not fill two A slots and one B slot")
	}
	defer first.Release()
	defer second.Release()
	initialB.Release()

	pending, capacity = beginConsumerPoll(t, a)
	if capacity != 2 {
		completeConsumerPoll(t, a, pending, nil, nil)
		t.Fatalf("busy A poll offer = %d, want 2", capacity)
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(2), nil); err != nil {
		t.Fatal(err)
	}

	pending, capacity = beginConsumerPoll(t, b)
	if capacity != 1 {
		completeConsumerPoll(t, b, pending, nil, nil)
		t.Fatalf("waiting B offer while A has two busy runners = %d, want remaining slot", capacity)
	}
	if err := completeConsumerPoll(t, b, pending, pollMessage(1), nil); err != nil {
		t.Fatal(err)
	}
	waiting := group.acquire("b")
	if waiting == nil {
		t.Fatal("waiting B could not acquire the slot yielded by A's poll turn")
	}
	defer waiting.Release()
}

func TestGroupLeaseReleaseIsIdempotent(t *testing.T) {
	group := newTestGroup(t, 2, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)
	a.deliverDemand(1)
	lease := group.acquire("a")
	if lease == nil {
		t.Fatal("failed to acquire first lease")
	}
	lease.MarkCleaning()
	lease.MarkCleaning()
	lease.Release()
	lease.Release()
	group.mu.Lock()
	gotLeases, gotCleaning := group.pools["a"].leases, group.pools["a"].cleaning
	group.mu.Unlock()
	if gotLeases != 0 || gotCleaning != 0 {
		t.Fatalf("after duplicate release: leases=%d cleaning=%d", gotLeases, gotCleaning)
	}
}

func TestGroupDiscoversPendingJobAfterCancellation(t *testing.T) {
	group := newTestGroup(t, 2, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)
	a.deliverDemand(2)
	pending, capacity := beginConsumerPoll(t, a)
	if capacity != 2 {
		t.Fatalf("initial capacity = %d, want 2", capacity)
	}
	if err := completeConsumerPoll(t, a, pending, pollMessage(2), nil); err != nil {
		t.Fatal(err)
	}
	first, second := group.acquire("a"), group.acquire("a")
	if first == nil || second == nil {
		t.Fatal("could not start both jobs")
	}
	defer first.Release()
	defer second.Release()
	// The cancellation removes one assigned job; its lease survives cleanup.
	a.deliverDemand(1)
	second.MarkCleaning()
	second.Release()
	// A capped poll has no assignment because its one remaining job fills it.
	pending, capacity = beginConsumerPoll(t, a)
	if err := completeConsumerPoll(t, a, pending, pollMessage(1), nil); err != nil {
		t.Fatal(err)
	}
	// Exercise both listeners: B has no jobs, while A needs to discover its
	// still-unassigned third job without requiring a new statistics message.
	discovered := false
	for range 6 {
		for _, pool := range []*testPoolClient{b, a} {
			pending, capacity = beginConsumerPoll(t, pool)
			demand := 0
			if pool == a {
				demand = 1
				if capacity == 2 {
					discovered = true
				}
			}
			if err := completeConsumerPoll(t, pool, pending, pollMessage(demand), nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !discovered {
		t.Fatal("freed slot was never advertised to the pool with a running job")
	}
}

func TestGroupStalePollIsCancelledAndReissuedWithReducedOffer(t *testing.T) {
	group := newTestGroup(t, 3, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)
	a.deliverDemand(1)
	aPoll, aOffer := beginConsumerPoll(t, a)
	if aOffer != 2 {
		t.Fatalf("A initial offer = %d, want 2", aOffer)
	}
	if err := completeConsumerPoll(t, a, aPoll, pollMessage(1), nil); err != nil {
		t.Fatal(err)
	}
	lease := group.acquire("a")
	if lease == nil {
		t.Fatal("failed to seed A's active lease")
	}
	defer lease.Release()
	b.deliverDemand(1)
	bPoll, bOffer := beginConsumerPoll(t, b)
	if bOffer != 1 {
		t.Fatalf("B offer = %d, want 1", bOffer)
	}
	aPoll, aOffer = beginConsumerPoll(t, a)
	if aOffer != 2 {
		t.Fatalf("A active offer = %d, want 2", aOffer)
	}
	if err := completeConsumerPoll(t, b, bPoll, pollMessage(2), nil); err != nil {
		t.Fatal(err)
	}
	group.mu.Lock()
	acked, inflight := group.pools["a"].acked, group.pools["a"].inflight
	group.mu.Unlock()
	if acked != 2 || inflight != 2 {
		t.Fatalf("A reservation awaiting downgrade acknowledgement = %d/%d, want 2/2", acked, inflight)
	}
	select {
	case request := <-a.inner.requests:
		if request.capacity != 1 {
			t.Fatalf("reissued A offer = %d, want 1", request.capacity)
		}
	case <-time.After(consumerPollTimeout):
		t.Fatal("stale A poll was not cancelled and reissued")
	}
	var response chan messagePollResponse
	select {
	case response = <-a.inner.responses:
	case <-time.After(consumerPollTimeout):
		t.Fatal("reissued poll did not accept a response")
	}
	select {
	case response <- messagePollResponse{message: pollMessage(1)}:
	case <-time.After(consumerPollTimeout):
		t.Fatal("timed out delivering reissued A response")
	}
	if err := <-aPoll.done; err != nil {
		t.Fatal(err)
	}
}

func TestGroupStablePollDoesNotInterruptOnEmptyResponse(t *testing.T) {
	group := newTestGroup(t, 2, "a", "b")
	a, b := newTestPoolClient(group, "a"), newTestPoolClient(group, "b")
	acknowledgeStartupAtZero(t, a, b)
	a.deliverDemand(1)
	pending, offer := beginConsumerPoll(t, a)
	if offer != 2 {
		t.Fatalf("stable A offer = %d, want 2", offer)
	}
	before := a.inner.attempts.Load()
	a.deliverDemand(1)
	if a.inner.attempts.Load() != before {
		t.Fatal("unchanged demand restarted poll")
	}
	if err := completeConsumerPoll(t, a, pending, nil, nil); err != nil {
		t.Fatal(err)
	}
	if a.inner.attempts.Load() != before {
		t.Fatal("empty response caused poll churn")
	}
}

func TestGroupParentPollCancellationIsNotRetried(t *testing.T) {
	group := newTestGroup(t, 1, "a")
	a := newTestPoolClient(group, "a")
	acknowledgeStartupAtZero(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	pending, _ := beginPollWithContext(t, a, ctx)
	cancel()
	select {
	case err := <-pending.done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancel returned %v, want context.Canceled", err)
		}
	case <-time.After(consumerPollTimeout):
		t.Fatal("parent cancellation did not return")
	}
	if a.inner.attempts.Load() != 2 {
		t.Fatal("parent cancellation caused a retry")
	}
}

func TestGroupPollOrdinaryErrorPropagates(t *testing.T) {
	group := newTestGroup(t, 1, "a")
	a := newTestPoolClient(group, "a")
	acknowledgeStartupAtZero(t, a)
	pending, _ := beginConsumerPoll(t, a)
	want := errors.New("ordinary poll error")
	if err := completeConsumerPoll(t, a, pending, nil, want); !errors.Is(err, want) {
		t.Fatalf("poll error = %v, want %v", err, want)
	}
}

func TestGroupLocalCancellationDoesNotMaskOrdinaryError(t *testing.T) {
	group := newTestGroup(t, 1, "a")
	a := newTestPoolClient(group, "a")
	acknowledgeStartupAtZero(t, a)
	want := errors.New("ordinary poll error")
	a.inner.errorOnCancel = want
	pending, _ := beginConsumerPoll(t, a)
	group.BlockPool("a")
	select {
	case err := <-pending.done:
		if !errors.Is(err, want) {
			t.Fatalf("poll error after local cancellation = %v, want %v", err, want)
		}
	case <-time.After(consumerPollTimeout):
		pending.cancel()
		t.Fatal("ordinary error was swallowed after local cancellation")
	}
}

func TestGroupMessageReturnedWithLocalCancelIsNotLost(t *testing.T) {
	group := newTestGroup(t, 1, "a")
	a := newTestPoolClient(group, "a")
	acknowledgeStartupAtZero(t, a)
	message := pollMessage(0)
	a.inner.messageOnCancel = message
	ctx, cancel := context.WithCancel(context.Background())
	pending, offer := beginPollWithContext(t, a, ctx)
	group.BlockPool("a")
	select {
	case err := <-pending.done:
		cancel()
		if err != nil || pending.message != message {
			t.Fatalf("message/cancel result = (%p, %v), want (%p, nil)", pending.message, err, message)
		}
	case <-time.After(consumerPollTimeout):
		cancel()
		t.Fatal("message accompanying local cancellation was not returned")
	}
	group.mu.Lock()
	acked, inflight := group.pools["a"].acked, group.pools["a"].inflight
	group.mu.Unlock()
	if acked != offer || inflight != 0 {
		t.Fatalf("successful message accounting = %d/%d, want %d/0", acked, inflight, offer)
	}
}
