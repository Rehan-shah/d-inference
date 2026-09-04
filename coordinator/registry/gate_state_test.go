package registry

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eigeninference/d-inference/coordinator/attestation"
)

// Tests for the per-identity gate index itself (gate_state.go): resolution,
// identity migration with cached pointers, the sweep's liveness rule, the
// probe claim's per-identity exclusivity and the wait observer. The trackers'
// semantics are covered by their own files; the commit lock mode lives in
// reserve_commit_test.go.

// A recorder that resolved the session's gate just before attestation bound
// the stable identity must land its outcome on the identity's gate, not on
// the orphaned session gate: lockGate follows the forward set by the
// migration, and the orphan is gone from the index.
func TestStaleGatePointerFollowsIdentityMigration(t *testing.T) {
	reg := New(testLogger())
	p := makeSchedulerProvider(t, reg, "sess-stale", "m", 100)
	stale := reg.gateForSession(p.ID)
	if stale == nil || stale.key != p.ID {
		t.Fatalf("pre-bind gate = %+v, want the session-keyed gate", stale)
	}

	p.SetAttestationResult(&attestation.VerificationResult{Valid: true, SerialNumber: "SER-STALE"})
	target := reg.lookupGateForKey("serial:SER-STALE")
	if target == nil || target.key != "serial:SER-STALE" {
		t.Fatalf("bind did not file the identity's gate: %+v", target)
	}
	if rawGateForKey(reg, p.ID) != nil {
		t.Fatal("the orphaned session gate must leave the index")
	}
	if stale.resolve() != target {
		t.Fatal("resolve() on the stale pointer must follow the migration forward")
	}
	hold := reg.lockGate(stale, "test")
	if hold.g != target {
		hold.unlock()
		t.Fatal("lockGate on the stale pointer must lock the live gate")
	}
	hold.g.breakerTrips++
	hold.g.updatedLocked(time.Now())
	hold.unlock()
	if got := providerBreakerTripsOf(reg, p.ID); got != 1 {
		t.Fatalf("mutation through the stale pointer landed elsewhere: trips=%d", got)
	}
	if p.gate.Load() != target {
		t.Fatal("the provider's cached gate must point at the identity's gate")
	}
}

// During a live re-attestation that changes the identity, a lock-free reader
// must never see the identity's fault state vanish: the orphaned session gate
// keeps its (conservative) atomics — it is never republished after the reset
// — and the provider's cached pointer is repointed only once the target
// carries the merged state.
func TestMigrationNeverExposesAnEmptyGateToLockFreeReaders(t *testing.T) {
	reg := New(testLogger())
	p := makeSchedulerProvider(t, reg, "sess-mig-view", "m", 100)
	for i := 0; i < providerBreakerConsecTrip; i++ {
		reg.RecordProviderOutcome(p.ID, false, 500, "internal error")
	}
	stale := p.gate.Load()
	nowNS := time.Now().UnixNano()
	if !stale.breakerOpenAt(nowNS) {
		t.Fatal("precondition: breaker open on the session gate")
	}

	p.SetAttestationResult(&attestation.VerificationResult{Valid: true, SerialNumber: "SER-MIG-VIEW"})

	target := p.gate.Load()
	if target == stale || target.key != "serial:SER-MIG-VIEW" {
		t.Fatalf("cached gate after bind = %+v, want the identity's gate", target)
	}
	if !target.breakerOpenAt(nowNS) {
		t.Fatal("the target must carry the merged breaker state when the pointer is repointed")
	}
	if !stale.breakerOpenAt(nowNS) {
		t.Fatal("the orphaned gate's atomics must stay conservative (never republished after the reset)")
	}
	if stale.forwardTo.Load() != target || !stale.resolve().breakerOpenAt(nowNS) {
		t.Fatal("the orphan must forward to the live gate")
	}
	if !reg.ProviderBreakerOpen(p.ID) {
		t.Fatal("the breaker must read open through the session after the rebind")
	}
}

// A rebind that moves ONE of two sessions bound to the same identity must not
// orphan the shared gate: the other session still points at it, so it stays
// in the index (emptied, as the map-keyed implementation left the old key).
func TestRebindKeepsSharedIdentityGateForOtherSessions(t *testing.T) {
	reg := New(testLogger())
	p1 := makeSchedulerProvider(t, reg, "sess-shared-1", "m", 100)
	p2 := makeSchedulerProvider(t, reg, "sess-shared-2", "m", 100)
	p1.SetAttestationResult(&attestation.VerificationResult{Valid: true, PublicKey: "PK-SHARED"})
	p2.SetAttestationResult(&attestation.VerificationResult{Valid: true, PublicKey: "PK-SHARED"})
	shared := reg.lookupGateForKey("sekey:PK-SHARED")
	if shared == nil || p1.gate.Load() != shared || p2.gate.Load() != shared {
		t.Fatal("both sessions must share the identity's gate")
	}
	reg.RecordProviderOutcome(p1.ID, false, 500, "internal error")

	// p1 enriches to a serial; p2 stays on the SE key.
	p1.SetAttestationResult(&attestation.VerificationResult{Valid: true, PublicKey: "PK-SHARED", SerialNumber: "SER-ONE"})
	if rawGateForKey(reg, "sekey:PK-SHARED") != shared {
		t.Fatal("the shared gate must stay in the index while another session is bound to it")
	}
	if shared.forwardTo.Load() != nil {
		t.Fatal("a gate with a live session must not be forwarded")
	}
	if p2.gate.Load() != shared {
		t.Fatal("the other session's cached gate must be untouched")
	}
	if !gateHasBreakerWindow(reg, "serial:SER-ONE") {
		t.Fatal("the fault history must have moved to the enriched identity")
	}
	if gateHasBreakerWindow(reg, "sekey:PK-SHARED") {
		t.Fatal("the source identity must start from nothing after the migration")
	}
	reg.gatesMu.RLock()
	live := shared.live
	reg.gatesMu.RUnlock()
	if live != 1 {
		t.Fatalf("shared gate live sessions = %d, want 1", live)
	}
}

// The sweep never drops a gate a connected session references, drops an
// identity-less session's gate at Disconnect, and drops a disconnected
// identity's gate only once it is idle AND past the idle grace.
func TestGateSweepLivenessRule(t *testing.T) {
	reg := New(testLogger())
	anon := makeSchedulerProvider(t, reg, "sess-anon", "m", 100)
	attested := attestSchedulerProvider(t, reg, "sess-att", "m", "SER-SWEEP", 100)
	reg.RecordProviderOutcome(attested.ID, false, 500, "internal error")

	reg.sweepGates(time.Now().Add(24 * time.Hour))
	if rawGateForKey(reg, anon.ID) == nil || rawGateForKey(reg, "serial:SER-SWEEP") == nil {
		t.Fatal("gates with a connected session must survive any sweep")
	}

	reg.Disconnect(anon.ID)
	if rawGateForKey(reg, anon.ID) != nil {
		t.Fatal("an identity-less session's gate must be dropped at Disconnect")
	}

	reg.Disconnect(attested.ID)
	if rawGateForKey(reg, "serial:SER-SWEEP") == nil {
		t.Fatal("a stable identity's gate must survive Disconnect")
	}
	// Inside the breaker window / idle grace: kept.
	reg.sweepGates(time.Now().Add(time.Minute))
	if rawGateForKey(reg, "serial:SER-SWEEP") == nil {
		t.Fatal("a recently active identity must not be swept")
	}
	// Past every window and the grace: gone.
	reg.sweepGates(time.Now().Add(gateIdleGrace + providerBreakerWindow + time.Minute))
	if rawGateForKey(reg, "serial:SER-SWEEP") != nil {
		t.Fatal("an idle disconnected identity must be swept after the grace")
	}
}

// The trailing pending-request flush runs after Disconnect removed the session:
// its faults must still resolve to the stable identity's gate through the
// disconnect cache, exactly as the map-keyed faultKeyLocked fallback did.
func TestDisconnectedTrailingFlushResolvesIdentityGate(t *testing.T) {
	reg := New(testLogger())
	p := attestSchedulerProvider(t, reg, "sess-flush", "m", "SER-FLUSH", 100)
	reg.Disconnect(p.ID)
	if got := reg.faultKeyForSession(p.ID); got != "serial:SER-FLUSH" {
		t.Fatalf("post-disconnect fault key = %q, want the cached identity", got)
	}
	for i := 0; i < providerBreakerConsecTrip; i++ {
		reg.RecordProviderOutcome(p.ID, false, 502, "provider disconnected")
	}
	if !gateHasBreakerWindow(reg, "serial:SER-FLUSH") || rawGateForKey(reg, p.ID) != nil {
		t.Fatal("trailing-flush faults must land on the identity's gate, not a session gate")
	}
	if !reg.ProviderBreakerOpen(p.ID) {
		t.Fatal("the identity's breaker must be open via the disconnected session id")
	}
}

// Two sessions of one identity racing for the single half-open probe: the
// check-and-claim under gate.mu lets exactly one through, and the gate reads
// closed for both afterwards.
func TestTryClaimCapacityProbeIsExclusivePerIdentity(t *testing.T) {
	reg := New(testLogger())
	const model = "m"
	p1 := attestSchedulerProvider(t, reg, "sess-probe-1", model, "SER-PROBE", 100)
	p2 := attestSchedulerProvider(t, reg, "sess-probe-2", model, "SER-PROBE", 100)
	if reg.gateOf(p1) != reg.gateOf(p2) {
		t.Fatal("sessions of one identity must share a gate")
	}
	for i := 0; i < reg.capacityCooldownCfg.Threshold; i++ {
		reg.RecordCapacityReject(p1.ID, model)
	}
	if !reg.CapacityCooldownActive(p2.ID, model) {
		t.Fatal("the cooldown must be visible through the sibling session")
	}
	expireCapacityCooldown(reg, p1.ID, model)
	if reg.CapacityCooldownActive(p1.ID, model) {
		t.Fatal("an expired, unclaimed cooldown must read open")
	}

	now := time.Now()
	var claimed atomic.Int32
	var wg sync.WaitGroup
	for _, p := range []*Provider{p1, p2, p1, p2} {
		wg.Add(1)
		go func(p *Provider) {
			defer wg.Done()
			if reg.gateOf(p).tryClaimCapacityProbe(model, now) {
				claimed.Add(1)
			}
		}(p)
	}
	wg.Wait()
	if claimed.Load() != 1 {
		t.Fatalf("probe claims = %d, want exactly 1", claimed.Load())
	}
	if !reg.CapacityCooldownActive(p1.ID, model) || !reg.CapacityCooldownActive(p2.ID, model) {
		t.Fatal("the claimed probe must close the gate for every session of the identity")
	}
	// No cooldown entry at all: the claim is a lock-free no-op that admits.
	if !reg.gateOf(p1).tryClaimCapacityProbe("other-model", now) {
		t.Fatal("a pair with no cooldown entry must always claim")
	}
}

// Gate reads on a Provider that was never registered (bare test objects) and
// on a nil gate are "no state", never a panic.
func TestGateReadsAreNilSafe(t *testing.T) {
	reg := New(testLogger())
	bare := &Provider{ID: "bare"}
	g := reg.gateOf(bare)
	now := time.Now()
	if g != nil {
		t.Fatalf("bare provider resolved to a gate: %+v", g)
	}
	if g.breakerOpenAt(now.UnixNano()) || g.ejectedAt(now.UnixNano()) || g.dispatchLoadCooled("m", now) ||
		g.inferenceErrorCooled("m", "base", now) || g.capacityCooled("m", now) ||
		g.budgetClampActive(reg.budgetClampCfg, "m", now, 0, false, now) {
		t.Fatal("a nil gate must read as no state")
	}
	if pen, rate := g.capacityRatePenalty(reg.capacityRateCfg, "m", now); pen != 0 || rate != 0 {
		t.Fatal("a nil gate must carry no rate penalty")
	}
	if !g.tryClaimCapacityProbe("m", now) {
		t.Fatal("a nil gate must admit the probe claim")
	}
	if reg.ejectionOpenFor(g, "serial:none", now.UnixNano()) {
		t.Fatal("an unknown identity must not read as ejected")
	}
}

// A gate.mu wait above the threshold reaches the observer tagged by site — and
// only then: uncontended recorders report nothing.
func TestGateWaitObserverReportsLongWaits(t *testing.T) {
	reg := New(testLogger())
	p := makeSchedulerProvider(t, reg, "sess-wait", "m", 100)
	type seen struct {
		site string
		wait time.Duration
	}
	var mu sync.Mutex
	var got []seen
	reg.SetGateWaitObserver(func(site string, wait time.Duration) {
		mu.Lock()
		got = append(got, seen{site, wait})
		mu.Unlock()
	})

	reg.RecordProviderOutcome(p.ID, true, 200, "")
	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("uncontended recorder reported a wait: %+v", got)
	}

	release := reg.HoldGateForTest(p.ID)
	go func() {
		time.Sleep(20 * time.Millisecond)
		release()
	}()
	reg.RecordProviderOutcome(p.ID, true, 200, "")
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].site != "breaker" || got[0].wait < 5*time.Millisecond {
		t.Fatalf("observer calls = %+v, want one 'breaker' wait of >= 5ms", got)
	}
	reg.SetGateWaitObserver(nil)
}

// An accept on the first-byte path must not queue behind the registry write
// lock: with r.mu held for writing by someone else, the recorders still run.
func TestRecordersDoNotTakeTheRegistryWriteLock(t *testing.T) {
	reg := New(testLogger())
	p := attestSchedulerProvider(t, reg, "sess-nolock", "m", "SER-NOLOCK", 100)
	reg.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.RecordCapacityAccept(p.ID, "m")
		reg.RecordInferenceSuccess(p.ID, "m", "base")
		reg.RecordProviderOutcome(p.ID, true, 200, "")
		reg.RecordProviderServeOutcome("serial:SER-NOLOCK", true, 200, "")
		reg.ClearDispatchLoadCooldown(p.ID, "m")
		reg.RecordDispatchLoadFailure(p.ID, "m")
		reg.RecordInferenceError(p.ID, "m", 500, "base")
		reg.RecordCapacityReject(p.ID, "m")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		reg.mu.Unlock()
		t.Fatal("a recorder blocked behind the registry write lock")
	}
	reg.mu.Unlock()
}
