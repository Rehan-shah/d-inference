package registry

import (
	"fmt"
	"testing"
	"time"

	"github.com/eigeninference/d-inference/coordinator/attestation"
	"github.com/eigeninference/d-inference/coordinator/protocol"
)

// Version-changed reconnect reset (version_reset.go), live-isolated on a real
// Registry: sessions register, bind a serial identity, die abruptly with work
// in flight (the flush the 2026-08-31 upgrade wave produced), and reconnect.

const versionResetSerial = "SER-UPGRADE"
const versionResetStable = "serial:" + versionResetSerial

// bindVersionedSession registers a session, stores its binary version, and
// binds it to the serial identity. versionFirst=true is the re-attestation
// order (version already stored when bindStableFaultKey runs); false is the
// registration order (attestation binds BEFORE the api stores the version,
// so Provider.SetVersion must run the check).
func bindVersionedSession(t *testing.T, r *Registry, id, version string, versionFirst bool) *Provider {
	t.Helper()
	msg := testRegisterMessage()
	msg.Models = []protocol.ModelInfo{{ID: "m", ModelType: "chat"}}
	p := r.Register(id, nil, msg)
	bind := func() {
		p.SetAttestationResult(&attestation.VerificationResult{Valid: true, SerialNumber: versionResetSerial})
	}
	if versionFirst {
		p.SetVersion(version)
		bind()
	} else {
		bind()
		p.SetVersion(version)
	}
	return p
}

// dieAbruptlyWithFlush parks requests on the session, drops it without a close
// frame, and records the flush terminals the consumers feed: enough 502s to
// trip the inference-error cooldown (2), the node breaker (5), and the
// identity ejection (8) — roughly one upgraded box's worth in the incident.
func dieAbruptlyWithFlush(t *testing.T, r *Registry, id string) {
	t.Helper()
	p := r.GetProvider(id)
	if p == nil {
		t.Fatalf("provider %s not registered", id)
	}
	for i := 0; i < 3; i++ {
		p.AddPending(&PendingRequest{
			RequestID: fmt.Sprintf("%s-req-%d", id, i),
			Model:     "m",
			ErrorCh:   make(chan protocol.InferenceErrorMessage, 1),
		})
	}
	r.DisconnectWithReason(id, DisconnectReasonReadError)
	sid := r.GetProviderStableIdentity(id)
	if sid != versionResetStable {
		t.Fatalf("stable identity after disconnect = %q, want %q", sid, versionResetStable)
	}
	for i := 0; i < 2; i++ {
		r.RecordInferenceError(id, "m", 502, "base")
	}
	for i := 0; i < 5; i++ {
		r.RecordProviderOutcome(id, false, 502, "provider disconnected")
	}
	for i := 0; i < 8; i++ {
		r.RecordProviderServeOutcome(sid, false, 502, "provider disconnected")
	}
}

// assertIdentityQuarantine checks all three fault trackers for the identity,
// querying the inference-error cooldown and node breaker through queryID
// (a live session id resolves via faultKeyBySession, a dead one via the
// disconnect cache) and the ejection through the stable id.
func assertIdentityQuarantine(t *testing.T, r *Registry, queryID string, want bool) {
	t.Helper()
	if got := r.InferenceErrorCooldownActive(queryID, "m", "base"); got != want {
		t.Errorf("InferenceErrorCooldownActive(%s) = %v, want %v", queryID, got, want)
	}
	if got := r.ProviderBreakerOpen(queryID); got != want {
		t.Errorf("ProviderBreakerOpen(%s) = %v, want %v", queryID, got, want)
	}
	if got := r.HealthEjectionOpen(versionResetStable); got != want {
		t.Errorf("HealthEjectionOpen(%s) = %v, want %v", versionResetStable, got, want)
	}
}

func TestVersionChangedReconnect_ClearsDisconnectFlushStrikes(t *testing.T) {
	for _, versionFirst := range []bool{true, false} {
		name := "registration order (bind then SetVersion)"
		if versionFirst {
			name = "re-attestation order (SetVersion then bind)"
		}
		t.Run(name, func(t *testing.T) {
			r := New(testLogger())
			bindVersionedSession(t, r, "s1", "0.9.0", true)
			dieAbruptlyWithFlush(t, r, "s1")
			assertIdentityQuarantine(t, r, "s1", true)

			bindVersionedSession(t, r, "s2", "0.9.1", versionFirst)
			assertIdentityQuarantine(t, r, "s2", false)
			if r.InferenceErrorCooldownActive(versionResetStable, "m", "base") {
				t.Errorf("cooldown still active under the stable id after the version bump")
			}
		})
	}
}

func TestSameVersionReconnect_RetainsDisconnectFlushStrikes(t *testing.T) {
	r := New(testLogger())
	bindVersionedSession(t, r, "s1", "0.9.0", true)
	dieAbruptlyWithFlush(t, r, "s1")
	assertIdentityQuarantine(t, r, "s1", true)

	// The zombie signature: same binary, churning reconnects. Nothing resets.
	bindVersionedSession(t, r, "s2", "0.9.0", false)
	assertIdentityQuarantine(t, r, "s2", true)
}

// A version bump removes ONLY the flush 502s: genuine 500 faults recorded on
// the old binary still satisfy every trip condition, so the quarantines stay.
func TestVersionChangedReconnect_KeepsGenuineFaults(t *testing.T) {
	r := New(testLogger())
	bindVersionedSession(t, r, "s1", "0.9.0", true)
	for i := 0; i < 2; i++ {
		r.RecordInferenceError("s1", "m", 500, "base")
	}
	for i := 0; i < 5; i++ {
		r.RecordProviderOutcome("s1", false, 500, "internal error")
	}
	for i := 0; i < 8; i++ {
		r.RecordProviderServeOutcome(versionResetStable, false, 500, "internal error")
	}
	assertIdentityQuarantine(t, r, "s1", true)
	dieAbruptlyWithFlush(t, r, "s1")

	bindVersionedSession(t, r, "s2", "0.9.1", false)
	assertIdentityQuarantine(t, r, "s2", true)
}

// The first version ever observed for an identity only records it: strikes
// accumulated before any version was known (coordinator restart, un-versioned
// legacy session) are not wiped by the first versioned reconnect.
func TestVersionReconnect_FirstObservationOnlyRecords(t *testing.T) {
	r := New(testLogger())
	msg := testRegisterMessage()
	msg.Models = []protocol.ModelInfo{{ID: "m", ModelType: "chat"}}
	p := r.Register("s0", nil, msg)
	p.SetAttestationResult(&attestation.VerificationResult{Valid: true, SerialNumber: versionResetSerial})
	dieAbruptlyWithFlush(t, r, "s0")
	assertIdentityQuarantine(t, r, "s0", true)

	bindVersionedSession(t, r, "s1", "0.9.5", false)
	assertIdentityQuarantine(t, r, "s1", true)
}

// A graceful (peer-close) flush never strikes in the first place, so nothing
// is left to reset: the reconnect on any version finds a clean identity.
func TestGracefulDisconnect_NoStrikesToReset(t *testing.T) {
	r := New(testLogger())
	p := bindVersionedSession(t, r, "s1", "0.9.0", true)
	for i := 0; i < 3; i++ {
		p.AddPending(&PendingRequest{
			RequestID: fmt.Sprintf("s1-req-%d", i),
			Model:     "m",
			ErrorCh:   make(chan protocol.InferenceErrorMessage, 1),
		})
	}
	r.DisconnectWithReason("s1", DisconnectReasonPeerClose)
	// The api layer's noteInferenceError gates on the provider_restart reason
	// before any Record* call, so the registry sees no strikes at all.
	assertIdentityQuarantine(t, r, "s1", false)
	bindVersionedSession(t, r, "s2", "0.9.0", false)
	assertIdentityQuarantine(t, r, "s2", false)
}

// RegisterMessage.Version is provider-asserted, so the reset is rate-limited
// per identity: a second version change inside identityVersionResetMinInterval
// retains the strikes (a modified binary alternating two version strings
// cannot launder every reconnect), and the reset is available again once the
// interval has elapsed (a genuine later rollout).
func TestVersionChangedReconnect_ResetIsRateLimitedPerIdentity(t *testing.T) {
	r := New(testLogger())
	bindVersionedSession(t, r, "s1", "0.9.0", true)
	dieAbruptlyWithFlush(t, r, "s1")
	assertIdentityQuarantine(t, r, "s1", true)

	// First version change: reset consumed.
	bindVersionedSession(t, r, "s2", "0.9.1", false)
	assertIdentityQuarantine(t, r, "s2", false)
	dieAbruptlyWithFlush(t, r, "s2")
	assertIdentityQuarantine(t, r, "s2", true)

	// Second change inside the interval: strikes retained.
	bindVersionedSession(t, r, "s3", "0.9.2", false)
	assertIdentityQuarantine(t, r, "s3", true)

	// Once the interval has elapsed the reset is available again.
	r.mu.Lock()
	r.identityVersionResetAt[versionResetStable] = time.Now().Add(-identityVersionResetMinInterval - time.Second)
	r.mu.Unlock()
	bindVersionedSession(t, r, "s4", "0.9.3", false)
	assertIdentityQuarantine(t, r, "s4", false)
}

// TestInferenceFlushStrikes_BoundedForSameVersionIdentity: an identity that
// churns on the SAME binary version never triggers the version reset, so its
// disconnect-flush tags were append-only — the main strikes slid out of the
// breaker window and a success deleted them, but every flushed request kept a
// time.Time under the identity forever. Seed 10,000 flushed requests spread
// over hours (each also in the main strike list, exactly as RecordInferenceError
// writes them), record one more flush, and the tag slice must be bounded by
// the breaker window and remain a subset of the strikes; a success clears it.
func TestInferenceFlushStrikes_BoundedForSameVersionIdentity(t *testing.T) {
	r := New(testLogger())
	bindVersionedSession(t, r, "s1", "0.9.0", true)
	key := inferenceErrorKey{ProviderID: versionResetStable, ModelID: "m", Shape: "base"}

	const flushed = 10_000
	now := time.Now()
	seed := make([]time.Time, 0, flushed)
	for i := 0; i < flushed; i++ {
		// One flush every 2 s, the newest 2 s ago: ~30 fall inside the 60 s
		// breaker window, the rest are hours old.
		seed = append(seed, now.Add(-time.Duration(flushed-i)*2*time.Second))
	}
	r.mu.Lock()
	r.inferenceErrorStrikes[key] = append([]time.Time(nil), seed...)
	if r.inferenceErrorFlushStrikes == nil {
		r.inferenceErrorFlushStrikes = make(map[inferenceErrorKey][]time.Time)
	}
	r.inferenceErrorFlushStrikes[key] = append([]time.Time(nil), seed...)
	r.mu.Unlock()

	r.RecordInferenceError("s1", "m", 502, "base")

	r.mu.Lock()
	strikes := append([]time.Time(nil), r.inferenceErrorStrikes[key]...)
	flush := append([]time.Time(nil), r.inferenceErrorFlushStrikes[key]...)
	r.mu.Unlock()
	if len(strikes) == 0 {
		t.Fatal("main strike list is empty after a recorded flush")
	}
	// The bound is the breaker window: everything older than 60 s is gone.
	// 30 seeded entries at most survive (2 s spacing) plus the strike just
	// recorded; allow the wall clock a little drift.
	if len(flush) > int(inferenceErrorWindow/(2*time.Second))+2 {
		t.Fatalf("flush tags = %d after %d historical flushes, want the slice bounded by the %s window", len(flush), flushed, inferenceErrorWindow)
	}
	if len(flush) > len(strikes) {
		t.Fatalf("flush tags (%d) outnumber live strikes (%d)", len(flush), len(strikes))
	}
	for _, ts := range flush {
		if !containsTimestamp(strikes, ts) {
			t.Fatalf("flush tag %v marks a strike that is no longer in the window", ts)
		}
		if now.Sub(ts) >= inferenceErrorWindow+time.Second {
			t.Fatalf("flush tag %v is older than the breaker window", ts)
		}
	}
	if !containsTimestamp(flush, strikes[len(strikes)-1]) {
		t.Fatal("the flush just recorded is not tagged")
	}

	// A served request clears the shape's history — tags included.
	r.RecordInferenceSuccess("s1", "m", "base")
	r.mu.Lock()
	_, strikesLeft := r.inferenceErrorStrikes[key]
	_, flushLeft := r.inferenceErrorFlushStrikes[key]
	r.mu.Unlock()
	if strikesLeft || flushLeft {
		t.Fatalf("after success: strikes present=%v flush tags present=%v, want both cleared", strikesLeft, flushLeft)
	}
}

// TestInferenceFlushStrikes_NonFlushStrikePrunesTags: the tags slide out of
// the window on EVERY counted strike, not only on a 502, so they can never
// reference a strike the main list has already dropped.
func TestInferenceFlushStrikes_NonFlushStrikePrunesTags(t *testing.T) {
	r := New(testLogger())
	bindVersionedSession(t, r, "s1", "0.9.0", true)
	key := inferenceErrorKey{ProviderID: versionResetStable, ModelID: "m", Shape: "base"}

	stale := time.Now().Add(-2 * inferenceErrorWindow)
	r.mu.Lock()
	r.inferenceErrorStrikes[key] = []time.Time{stale}
	r.inferenceErrorFlushStrikes = map[inferenceErrorKey][]time.Time{key: {stale}}
	r.mu.Unlock()

	r.RecordInferenceError("s1", "m", 500, "base")

	r.mu.Lock()
	flush, present := r.inferenceErrorFlushStrikes[key]
	r.mu.Unlock()
	if present {
		t.Fatalf("stale flush tag survived a non-flush strike: %v", flush)
	}
}
