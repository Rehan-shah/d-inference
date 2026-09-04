package registry

import (
	"testing"
	"time"
)

// Tests for the gate index (gate_index.go): session → gate resolution through
// the disconnect cache, and nil-safety of the scan's gate reads.

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
