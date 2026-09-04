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
	ref := reg.gateForSession(p.ID)
	stale := ref.g
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
	hold := reg.lockGate(ref, "test")
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
			if reg.tryClaimCapacityProbe(p, model, now) {
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
	if !reg.tryClaimCapacityProbe(p1, "other-model", now) {
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
	if !reg.tryClaimCapacityProbe(bare, "m", now) || !reg.tryClaimCapacityProbe(nil, "m", now) {
		t.Fatal("a provider with no gate must admit the probe claim")
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

// A recorder that resolved a gate SHARED by two sessions just before one of
// them rebinds (sekey: → serial: enrichment) must land its outcome on the
// rebinding session's NEW gate — the shared gate stays in the index for the
// other session, carries no forward, and was emptied by the migration, so
// only the session's own repointed p.gate can tell the stale holder to move.
// The other session's recorder, resolved at the same moment, stays put.
func TestStaleRefFollowsSharedIdentityRebind(t *testing.T) {
	reg := New(testLogger())
	p1 := makeSchedulerProvider(t, reg, "sess-rebind-1", "m", 100)
	p2 := makeSchedulerProvider(t, reg, "sess-rebind-2", "m", 100)
	pk := &attestation.VerificationResult{Valid: true, PublicKey: "PK-REBIND"}
	p1.SetAttestationResult(pk)
	p2.SetAttestationResult(pk)
	shared := reg.lookupGateForKey("sekey:PK-REBIND")
	if shared == nil || p1.gate.Load() != shared || p2.gate.Load() != shared {
		t.Fatal("both sessions must share the identity's gate")
	}
	reg.RecordProviderOutcome(p1.ID, false, 500, "internal error")

	// Two recorders resolve the shared gate, one per session...
	ref1 := reg.gateForSession(p1.ID)
	ref2 := reg.gateForSession(p2.ID)
	if ref1.g != shared || ref1.p != p1 || ref2.g != shared || ref2.p != p2 {
		t.Fatalf("refs = %+v / %+v, want the shared gate via each session", ref1, ref2)
	}
	// ...and p1 enriches to a serial before either takes the lock.
	p1.SetAttestationResult(&attestation.VerificationResult{Valid: true, PublicKey: "PK-REBIND", SerialNumber: "SER-REBIND"})
	target := p1.gate.Load()
	if target == shared || target.key != "serial:SER-REBIND" {
		t.Fatalf("p1's gate after the rebind = %+v, want serial:SER-REBIND", target)
	}
	if shared.forwardTo.Load() != nil || rawGateForKey(reg, "sekey:PK-REBIND") != shared {
		t.Fatal("precondition: the shared gate stays in the index, unforwarded, for p2")
	}
	if !gateHasBreakerWindow(reg, "serial:SER-REBIND") || gateHasBreakerWindow(reg, "sekey:PK-REBIND") {
		t.Fatal("precondition: the fault history moved to the enriched identity")
	}

	hold := reg.lockGate(ref1, "test")
	if hold.g != target {
		hold.unlock()
		t.Fatalf("p1's recorder locked %q, want the session's new gate serial:SER-REBIND", hold.g.key)
	}
	hold.g.breakerTrips++
	hold.g.updatedLocked(time.Now())
	hold.unlock()
	if got := providerBreakerTripsOf(reg, p1.ID); got != 1 {
		t.Fatalf("p1's outcome did not land on its identity: trips=%d", got)
	}
	readGateForKey(reg, "sekey:PK-REBIND", func(g *gateState) {
		if g == nil || g.breakerTrips != 0 || g.outcomes != nil {
			t.Fatalf("p2's identity must be untouched by p1's stale recorder: %+v", g)
		}
	})

	hold = reg.lockGate(ref2, "test")
	if hold.g != shared {
		hold.unlock()
		t.Fatalf("p2's recorder locked %q, want its own (shared) gate", hold.g.key)
	}
	hold.g.healthWindowLocked().record(false, time.Now())
	hold.g.updatedLocked(time.Now())
	hold.unlock()
	if !gateHasBreakerWindow(reg, "sekey:PK-REBIND") || providerBreakerTripsOf(reg, p2.ID) != 0 {
		t.Fatal("p2's outcome must land on p2's identity, and only there")
	}
}

// A trailing-flush recorder that resolved a disconnected identity's gate just
// before the sweep dropped it (idle, no live session, past the grace) must not
// write into the retired gate — the fault would vanish before the identity's
// next reconnect. lockGate sees retired, re-resolves, and the fault lands on
// the gate a fresh lookup finds. Both resolution paths: by session id through
// the disconnect cache (RecordProviderOutcome) and by stable id
// (RecordProviderServeOutcome).
func TestStaleRefSurvivesSweepOfDisconnectedGate(t *testing.T) {
	const key = "serial:SER-SWEEP-RACE"
	backdate := func(g *gateState) { g.touched = time.Now().Add(-gateIdleGrace - time.Minute) }
	for _, tc := range []struct {
		name    string
		resolve func(reg *Registry, sessionID string) gateRef
	}{
		{"by session through the disconnect cache", func(reg *Registry, sessionID string) gateRef { return reg.gateForSession(sessionID) }},
		{"by stable id", func(reg *Registry, _ string) gateRef { return reg.gateForKey(key) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := New(testLogger())
			p := attestSchedulerProvider(t, reg, "sess-sweep-race", "m", "SER-SWEEP-RACE", 100)
			reg.Disconnect(p.ID)
			// Nothing was ever recorded on the identity, so its gate is idle;
			// backdate its creation past the grace so a PRESENT-time sweep
			// drops it (a future-time sweep would also expire the disconnect
			// cache the trailing flush resolves through).
			withGateForKey(reg, key, backdate)

			ref := tc.resolve(reg, p.ID)
			stale := ref.g
			if stale == nil || stale.key != key || ref.p != nil {
				t.Fatalf("pre-sweep ref = %+v, want the disconnected identity's gate with no live session", ref)
			}
			reg.sweepGates(time.Now())
			if rawGateForKey(reg, key) != nil {
				t.Fatal("precondition: the sweep must drop the idle disconnected gate")
			}

			hold := reg.lockGate(ref, "test")
			if hold.g == stale {
				hold.unlock()
				t.Fatal("lockGate handed out the gate the sweep retired")
			}
			if hold.g.key != key || rawGateForKey(reg, key) != hold.g {
				hold.unlock()
				t.Fatalf("re-resolved to %q (in index: %v), want a fresh %s gate", hold.g.key, rawGateForKey(reg, key) == hold.g, key)
			}
			now := time.Now()
			hold.g.healthWindowLocked().record(false, now)
			hold.g.updatedLocked(now)
			hold.unlock()

			stale.mu.Lock()
			retired := stale.retired
			stale.mu.Unlock()
			if !retired {
				t.Fatal("the swept gate must be marked retired under its lock")
			}
			if !gateHasBreakerWindow(reg, key) {
				t.Fatal("the trailing fault must be on the gate a fresh lookup finds")
			}
			// The rest of the flush lands on the same gate and trips the
			// identity's breaker — the reconnecting-zombie signal survives.
			for i := 1; i < providerBreakerConsecTrip; i++ {
				reg.RecordProviderOutcome(p.ID, false, 502, "provider disconnected")
			}
			if !reg.ProviderBreakerOpen(p.ID) {
				t.Fatal("the identity's breaker must be open through the disconnected session id")
			}
		})
	}
}

// A CLEAR recorder (no-insert resolution) whose gate was swept in the window
// has nothing left to clear: it keeps the retired gate — its write is a no-op
// — rather than file a gate under an identity nothing references.
func TestClearRefNeverFilesAGateForASweptIdentity(t *testing.T) {
	const key = "serial:SER-CLEAR-RACE"
	reg := New(testLogger())
	p := attestSchedulerProvider(t, reg, "sess-clear-race", "m", "SER-CLEAR-RACE", 100)
	reg.Disconnect(p.ID)
	withGateForKey(reg, key, func(g *gateState) { g.touched = time.Now().Add(-gateIdleGrace - time.Minute) })

	ref := reg.lookupSessionGateRef(p.ID)
	stale := ref.g
	if stale == nil || stale.key != key || ref.insert {
		t.Fatalf("pre-sweep lookup ref = %+v, want a no-insert ref to the identity's gate", ref)
	}
	reg.sweepGates(time.Now())
	if rawGateForKey(reg, key) != nil {
		t.Fatal("precondition: the sweep must drop the idle disconnected gate")
	}
	hold := reg.lockGate(ref, "test")
	if hold.g != stale {
		hold.unlock()
		t.Fatalf("a clear re-resolved to %q; it must keep the retired gate", hold.g.key)
	}
	hold.unlock()
	if rawGateForKey(reg, key) != nil || rawGateForKey(reg, p.ID) != nil {
		t.Fatal("a clear must not file a gate for a swept identity or a dead session")
	}
	// Through the real recorder, on the dead session: still nothing filed.
	reg.ClearDispatchLoadCooldown(p.ID, "m")
	if reg.gateCount() != 0 {
		t.Fatalf("gate index after a straggling clear = %d, want 0", reg.gateCount())
	}
}

// A gate created for an identity with no live session (the trailing flush's
// first fault, a serve outcome by stable id) counts its creation as activity:
// it is not idle-droppable before the grace, so the recorder that created it
// cannot lose the race against a sweep that runs before it takes the lock.
func TestFreshGateIsNotSweptBeforeTheGrace(t *testing.T) {
	reg := New(testLogger())
	ref := reg.gateForSession("sess-ghost")
	if ref.g == nil || ref.g.key != "sess-ghost" {
		t.Fatalf("ref = %+v, want a fresh session-keyed gate", ref)
	}
	reg.sweepGates(time.Now())
	if rawGateForKey(reg, "sess-ghost") != ref.g {
		t.Fatal("a just-created gate must survive the sweep until the grace")
	}
	reg.sweepGates(time.Now().Add(gateIdleGrace + time.Minute))
	if rawGateForKey(reg, "sess-ghost") != nil {
		t.Fatal("an idle unreferenced gate must be swept once past the grace")
	}
}

// Recorders racing identity rebinds (shared ↔ enriched) and sweeps that keep
// retiring idle gates: interleaving coverage under -race for lockGate's
// re-validation. The deterministic tests above carry the outcome assertions;
// here the invariants are "no retired gate is ever in the index", "the other
// session's binding is never disturbed", and a quiescent record lands on the
// session's current gate.
func TestGateRecordersRaceRebindsAndSweeps(t *testing.T) {
	reg := New(testLogger())
	const model = "m"
	p1 := makeSchedulerProvider(t, reg, "sess-stress-1", model, 100)
	p2 := makeSchedulerProvider(t, reg, "sess-stress-2", model, 100)
	pk := &attestation.VerificationResult{Valid: true, PublicKey: "PK-STRESS"}
	enriched := &attestation.VerificationResult{Valid: true, PublicKey: "PK-STRESS", SerialNumber: "SER-STRESS"}
	p1.SetAttestationResult(pk)
	p2.SetAttestationResult(pk)
	// A disconnected identity that keeps faulting (never idle) and one that
	// only ever sees successes (always idle: retired and re-created on every
	// sweep once backdated).
	gone := attestSchedulerProvider(t, reg, "sess-stress-gone", model, "SER-STRESS-GONE", 100)
	quiet := attestSchedulerProvider(t, reg, "sess-stress-quiet", model, "SER-STRESS-QUIET", 100)
	reg.Disconnect(gone.ID)
	reg.Disconnect(quiet.ID)

	backdateAll := func() {
		reg.gatesMu.RLock()
		gates := make([]*gateState, 0, len(reg.gates))
		for _, g := range reg.gates {
			gates = append(gates, g)
		}
		reg.gatesMu.RUnlock()
		past := time.Now().Add(-gateIdleGrace - time.Minute)
		for _, g := range gates {
			g.mu.Lock()
			g.touched = past
			g.mu.Unlock()
		}
	}

	const iters = 1500
	var wg sync.WaitGroup
	recorders := []func(i int){
		func(i int) { reg.RecordProviderOutcome(p1.ID, i%3 != 0, 500, "internal error") },
		func(i int) { reg.RecordCapacityReject(p1.ID, model) },
		func(i int) { reg.RecordCapacityAccept(p1.ID, model) },
		func(i int) { reg.RecordInferenceError(p1.ID, model, 500, "base") },
		func(i int) { reg.RecordInferenceSuccess(p1.ID, model, "base") },
		func(i int) { reg.ClearDispatchLoadCooldown(p1.ID, model) },
		func(i int) { reg.tryClaimCapacityProbe(p1, model, time.Now()) },
		func(i int) { reg.RecordProviderServeOutcome("sekey:PK-STRESS", i%2 == 0, 500, "internal error") },
		func(i int) { reg.RecordProviderOutcome(p2.ID, true, 200, "") },
		func(i int) { reg.RecordProviderOutcome(gone.ID, false, 502, "provider disconnected") },
		func(i int) { reg.RecordInferenceSuccess(quiet.ID, model, "base") },
		func(i int) { reg.RecordProviderServeOutcome("serial:SER-STRESS-QUIET", true, 200, "") },
	}
	for _, rec := range recorders {
		wg.Add(1)
		go func(rec func(int)) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				rec(i)
			}
		}(rec)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if i%2 == 0 {
				p1.SetAttestationResult(enriched)
			} else {
				p1.SetAttestationResult(pk)
			}
		}
	}()
	stop := make(chan struct{})
	sweeperDone := make(chan struct{})
	go func() {
		defer close(sweeperDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			backdateAll()
			reg.sweepGates(time.Now())
		}
	}()
	wg.Wait()
	close(stop)
	<-sweeperDone

	reg.gatesMu.RLock()
	for key, g := range reg.gates {
		g.mu.Lock()
		retired := g.retired
		g.mu.Unlock()
		if retired {
			t.Errorf("retired gate %q is still in the index", key)
		}
	}
	reg.gatesMu.RUnlock()
	if g := p2.gate.Load(); g == nil || g.key != "sekey:PK-STRESS" || rawGateForKey(reg, "sekey:PK-STRESS") != g {
		t.Fatalf("p2's binding was disturbed: %+v", g)
	}
	p1.SetAttestationResult(enriched)
	reg.RecordProviderOutcome(p1.ID, false, 500, "internal error")
	readGateForSession(reg, p1.ID, func(g *gateState) {
		if g == nil || g.key != "serial:SER-STRESS" || g.outcomes == nil {
			t.Fatalf("a quiescent fault must land on p1's current gate: %+v", g)
		}
	})
}

// A reservation commit that resolved the probe's gate just before its session
// rebound away from a SHARED identity must claim the probe on the session's
// NEW gate — where the cooldown entry migrated — and leave the other
// identity's (reset) state untouched. A claim on the old gate would find no
// entry and admit: a leaked probe through a cooled pair. The claim is the
// same in both commit modes (the mode only chooses the r.mu lock kind).
func TestProbeClaimFollowsSharedIdentityRebind(t *testing.T) {
	forEachCommitMode(t, func(t *testing.T, mode reserveCommitMode) {
		reg := New(testLogger())
		setReserveCommitModeForTest(reg, mode)
		const model = "m"
		p1 := makeSchedulerProvider(t, reg, "sess-probe-rebind-1", model, 100)
		p2 := makeSchedulerProvider(t, reg, "sess-probe-rebind-2", model, 100)
		pk := &attestation.VerificationResult{Valid: true, PublicKey: "PK-PROBE-REBIND"}
		p1.SetAttestationResult(pk)
		p2.SetAttestationResult(pk)
		shared := reg.lookupGateForKey("sekey:PK-PROBE-REBIND")
		if shared == nil || p1.gate.Load() != shared || p2.gate.Load() != shared {
			t.Fatal("both sessions must share the identity's gate")
		}
		for i := 0; i < reg.capacityCooldownCfg.Threshold; i++ {
			reg.RecordCapacityReject(p1.ID, model)
		}
		expireCapacityCooldown(reg, p1.ID, model)
		if reg.CapacityCooldownActive(p1.ID, model) {
			t.Fatal("precondition: an expired, unclaimed cooldown must read open")
		}

		// The commit resolves the probe's gate (under p.mu)...
		ref := reg.probeGateRef(p1)
		if ref.g != shared || ref.p != p1 {
			t.Fatalf("probe ref = %+v, want the shared gate via p1", ref)
		}
		// ...and p1 enriches to a serial before the claim takes the lock.
		p1.SetAttestationResult(&attestation.VerificationResult{Valid: true, PublicKey: "PK-PROBE-REBIND", SerialNumber: "SER-PROBE-REBIND"})
		target := p1.gate.Load()
		if target == shared || target.key != "serial:SER-PROBE-REBIND" {
			t.Fatalf("p1's gate after the rebind = %+v, want serial:SER-PROBE-REBIND", target)
		}
		if !target.hasPairState(gateFlagCapacityCooldown) || shared.hasPairState(gateFlagCapacityCooldown) {
			t.Fatal("precondition: the cooldown entry moved with the session")
		}

		now := time.Now()
		if !reg.claimCapacityProbeRef(ref, model, now) {
			t.Fatal("the expired, unclaimed probe must be claimable")
		}
		readGateForKey(reg, "serial:SER-PROBE-REBIND", func(g *gateState) {
			if g == nil {
				t.Fatal("the enriched identity's gate must exist")
			}
			if e := g.capacityCooldowns[model]; e == nil || !e.probeAt.Equal(now) {
				t.Fatalf("the claim must land on the session's new gate: entry=%+v", e)
			}
		})
		readGateForKey(reg, "sekey:PK-PROBE-REBIND", func(g *gateState) {
			if g == nil {
				t.Fatal("the shared gate must stay in the index for p2")
			}
			if len(g.capacityCooldowns) != 0 {
				t.Fatalf("the other identity's state must be untouched by the stale claim: %+v", g.capacityCooldowns)
			}
		})
		if !reg.CapacityCooldownActive(p1.ID, model) {
			t.Fatal("the claimed probe must close the gate for p1's identity")
		}
		if reg.CapacityCooldownActive(p2.ID, model) {
			t.Fatal("p2's identity carries no cooldown after the migration")
		}
		// A second commit through the live path sees the fresh claim: exactly
		// one probe gets through.
		if reg.tryClaimCapacityProbe(p1, model, now) {
			t.Fatal("a second claim must see the fresh claim and reject")
		}
	})
}

// The lock-free "no per-model state" fast path the clear recorders and the
// probe claim take before locking must not trust the flag of a gate the
// session has moved away from: after a shared-identity rebind the emptied
// source says "nothing" precisely because the state migrated. refHasPairState
// re-resolves to the session's new gate; a genuinely empty current gate is
// reported as such without a re-resolve.
func TestRefHasPairStateFollowsSharedIdentityRebind(t *testing.T) {
	reg := New(testLogger())
	const model = "m"
	p1 := makeSchedulerProvider(t, reg, "sess-flag-rebind-1", model, 100)
	p2 := makeSchedulerProvider(t, reg, "sess-flag-rebind-2", model, 100)
	pk := &attestation.VerificationResult{Valid: true, PublicKey: "PK-FLAG-REBIND"}
	p1.SetAttestationResult(pk)
	p2.SetAttestationResult(pk)
	shared := reg.lookupGateForKey("sekey:PK-FLAG-REBIND")
	reg.RecordDispatchLoadFailure(p1.ID, model)

	ref := reg.lookupSessionGateRef(p1.ID) // ClearDispatchLoadCooldown's resolution
	if ref.g != shared || ref.p != p1 || !shared.hasPairState(gateFlagDispatchLoad) {
		t.Fatalf("pre-rebind ref = %+v, want the shared gate (with dispatch-load state) via p1", ref)
	}
	p1.SetAttestationResult(&attestation.VerificationResult{Valid: true, PublicKey: "PK-FLAG-REBIND", SerialNumber: "SER-FLAG-REBIND"})
	target := p1.gate.Load()
	if target == shared || shared.hasPairState(gateFlagDispatchLoad) || !target.hasPairState(gateFlagDispatchLoad) {
		t.Fatal("precondition: the dispatch-load cooldown moved with the session and the shared gate reads empty")
	}

	got, has := reg.refHasPairState(ref, gateFlagDispatchLoad)
	if !has || got.g != target || got.p != p1 {
		t.Fatalf("refHasPairState = (%+v, %v), want the session's new gate with state", got, has)
	}
	// Through the real recorder: the completion-time clear lands on the new
	// identity, so the pair is routable again.
	reg.ClearDispatchLoadCooldown(p1.ID, model)
	if reg.dispatchLoadCooled(p1.ID, model, time.Now()) {
		t.Fatal("the clear must land on the session's new gate")
	}
	// A genuinely empty current gate: reported empty, ref unchanged.
	got, has = reg.refHasPairState(reg.lookupSessionGateRef(p2.ID), gateFlagDispatchLoad)
	if has || got.g != shared {
		t.Fatalf("refHasPairState on p2's empty gate = (%+v, %v), want (shared, false)", got, has)
	}
}
