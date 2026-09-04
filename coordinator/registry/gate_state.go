package registry

import (
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// gate_state.go — per-identity routing-gate state.
//
// Every fault tracker that gates routing — the node-health breaker
// (provider_breaker.go), stable-identity health ejection (health_ejection.go),
// the shape-keyed inference-error cooldown (error_cooldown.go), the capacity
// cooldown / rate window / budget clamp (capacity_cooldown.go, capacity_rate.go,
// budget_clamp.go) and the dispatch-load cooldown (registry.go) — used to live
// in global maps guarded by Registry.mu. Recording an outcome therefore took
// the registry WRITE lock: six times per served request, each acquisition
// draining the whole batch of fleet-scan readers first (~190 ms in prod). The
// holders were microseconds; the wait was structural.
//
// The state now lives in one gateState per fault key with its own mutex.
// Recorders resolve the gate, take gate.mu, mutate, release — they never touch
// r.mu. The scan reads the two hot booleans (breaker open, ejected) from
// atomics and takes gate.mu only for providers that actually carry per-model
// state (a flag word says which), so the common per-provider cost is a few
// atomic loads.
//
// LOCK ORDER: r.mu → p.mu → r.gatesMu → gate.mu. Never acquire r.mu or p.mu
// while holding gatesMu or a gate.mu. gatesMu is insert-only from the request
// path's point of view: recorders and readers take it for READING (session →
// gate resolution); it is written only by Register, Disconnect, the
// attestation-time identity bind and the periodic sweep. Only the identity
// migration (migrateGateLocked, under gatesMu.Lock) ever holds two gate.mu at
// once, so there is no ordering problem between gates. There is deliberately
// NO walk-wide gates lock on the scan: a lock taken for the whole fleet walk
// with per-completion writers would rebuild the exact convoy this removes.
//
// A recorder resolves its gate under gatesMu.RLock and locks it AFTER letting
// go of gatesMu, so the index can change in between: the identity bind can
// move the session to another gate, the sweep can drop the gate. The
// map-keyed implementation had no such window (key resolution and the write
// shared one r.mu.Lock section), so lockGate re-establishes it: after
// acquiring gate.mu it checks that the gate is still the one the outcome
// belongs to (not retired, still the session's cached gate) and otherwise
// releases, re-resolves through the index and retries. Everything that can
// invalidate a resolved gate — the forward, the retire flag, the session's
// repoint — is therefore written while holding that gate's mu.
//
// Sections under gate.mu must stay per identity and microseconds long.

// Bits of gateState.pairFlags: which per-model trackers hold ANY entry for this
// identity. Published under gate.mu after every mutation; read lock-free by the
// scan so a provider with no per-model state costs no lock section at all.
const (
	gateFlagDispatchLoad uint32 = 1 << iota
	gateFlagErrorCooldown
	gateFlagCapacityCooldown
	gateFlagBudgetClamp
)

// gateWaitReportThreshold is the gate.mu acquisition wait above which the
// wait is reported to the gate-wait observer (registry.gate.wait_ms). Below it
// the lock is doing what it should; above it something is holding a gate too
// long or the convoy is re-forming on a new lock.
const gateWaitReportThreshold = time.Millisecond

// gateIdleGrace is how long a gate with no live session must stay idle (every
// tracker expired or empty and nothing recorded) before the sweep drops it. A
// disconnected identity keeps its half-open trip memory for at least this long.
const gateIdleGrace = 10 * time.Minute

// gateSweepHighWater is the gate count above which an insert triggers an
// inline sweep (rate-limited by gateSweepMinInterval) so the index stays
// bounded even when the eviction loop is not running. The loop sweeps every
// timeout/3 (30 s at the default), so with a 60 s minimum interval the inline
// path — a walk of every gate under gatesMu.Lock, on a recorder's insert —
// never fires in a process that runs the loop.
const (
	gateSweepHighWater   = 4096
	gateSweepMinInterval = 60 * time.Second
)

// modelShapeKey identifies an inference-error bucket inside one gate:
// (model, request shape). Shape is RequestTraits.CooldownShape ("tools" /
// "base"); see error_cooldown.go.
type modelShapeKey struct {
	Model string
	Shape string
}

// gateState is the fault state of ONE identity (fault key: serial → SE key →
// account → session id). Fields below the atomics are guarded by mu.
type gateState struct {
	// key is the fault key this state is filed under (immutable).
	key string
	mu  sync.Mutex

	// forwardTo is set when this gate has been migrated into another (identity
	// rebind) and is no longer in r.gates. Holders of a stale pointer follow it
	// (resolve / lockResolved) so a recorder that resolved the gate just before
	// the bind lands its outcome on the live state, not on the orphan.
	forwardTo atomic.Pointer[gateState]

	// live counts connected sessions bound to this gate. Guarded by r.gatesMu
	// (Register / Disconnect / bind / sweep). A gate with live > 0 is never
	// swept — deleting it would let the next lookup create a twin.
	live int

	// Lock-free routing view, published under mu after every mutation.
	breakerOpenUntilNS atomic.Int64  // 0 when the breaker has never opened
	ejectionUntilNS    atomic.Int64  // 0 when the identity has never been ejected
	pairFlags          atomic.Uint32 // gateFlag* bits: which per-model maps are non-empty
	newestRateRejectNS atomic.Int64  // newest capacity-503 rate reject across models; 0 = none

	// retired is set (under mu, before the index delete) when the sweep drops
	// this idle gate from r.gates. A recorder that resolved the gate before
	// the sweep and locks it afterwards must not write here — no lookup will
	// ever find this gate again — so lockGate re-resolves instead.
	retired bool

	// touched is when a recorder last mutated this gate, or when it was
	// created (sweep grace anchor: a fresh gate is not idle-droppable before
	// the first recorder that resolved it has had a chance to lock it).
	touched time.Time

	// Node-health breaker (provider_breaker.go).
	outcomes     *providerHealthWindow // nil until the first recorded fault/success
	breakerUntil time.Time
	breakerTrips int

	// Stable-identity health ejection (health_ejection.go).
	ejection                 *providerHealthWindow
	ejectionUntil            time.Time
	ejectionTrips            int
	ejectionCapacityStreak   capacityStreak
	ejectionLastTripCapacity bool

	// Inference-error breaker, per (model, shape) (error_cooldown.go).
	inferenceErrorStrikes   map[modelShapeKey][]time.Time
	inferenceErrorCooldowns map[modelShapeKey]time.Time

	// Per-model trackers, keyed by model id.
	dispatchLoadCooldowns map[string]time.Time              // registry.go
	capacityRejectStrikes map[string][]time.Time            // capacity_cooldown.go
	capacityCooldowns     map[string]*capacityCooldownEntry // capacity_cooldown.go
	capacityCooldownTrips map[string]int                    // capacity_cooldown.go
	budgetClamps          map[string]*budgetClampEntry      // budget_clamp.go
	capacityRateRejects   map[string][]time.Time            // capacity_rate.go
	capacityRateAccepts   map[string][]time.Time            // capacity_rate.go
}

func newGateState(key string) *gateState {
	return &gateState{
		key:                     key,
		inferenceErrorStrikes:   make(map[modelShapeKey][]time.Time),
		inferenceErrorCooldowns: make(map[modelShapeKey]time.Time),
		dispatchLoadCooldowns:   make(map[string]time.Time),
		capacityRejectStrikes:   make(map[string][]time.Time),
		capacityCooldowns:       make(map[string]*capacityCooldownEntry),
		capacityCooldownTrips:   make(map[string]int),
		budgetClamps:            make(map[string]*budgetClampEntry),
		capacityRateRejects:     make(map[string][]time.Time),
		capacityRateAccepts:     make(map[string][]time.Time),
	}
}

// resolve follows forwardTo to the gate that currently holds this identity's
// state. Lock-free; one atomic load in the common (not migrated) case.
// nil-safe.
func (g *gateState) resolve() *gateState {
	for g != nil {
		next := g.forwardTo.Load()
		if next == nil {
			return g
		}
		g = next
	}
	return nil
}

// lockResolved locks the gate, following a migration that landed between the
// caller's resolve and the lock. The caller unlocks the RETURNED gate.
func (g *gateState) lockResolved() *gateState {
	for {
		g.mu.Lock()
		next := g.forwardTo.Load()
		if next == nil {
			return g
		}
		g.mu.Unlock()
		g = next
	}
}

// publishLocked refreshes the lock-free routing view from the guarded state.
// Call after every mutation, before releasing mu.
func (g *gateState) publishLocked() {
	g.breakerOpenUntilNS.Store(unixNanoOrZero(g.breakerUntil))
	g.ejectionUntilNS.Store(unixNanoOrZero(g.ejectionUntil))
	var flags uint32
	if len(g.dispatchLoadCooldowns) > 0 {
		flags |= gateFlagDispatchLoad
	}
	if len(g.inferenceErrorCooldowns) > 0 {
		flags |= gateFlagErrorCooldown
	}
	if len(g.capacityCooldowns) > 0 {
		flags |= gateFlagCapacityCooldown
	}
	if len(g.budgetClamps) > 0 {
		flags |= gateFlagBudgetClamp
	}
	g.pairFlags.Store(flags)
	var newest int64
	for _, rejects := range g.capacityRateRejects {
		if n := len(rejects); n > 0 {
			if ns := rejects[n-1].UnixNano(); ns > newest {
				newest = ns
			}
		}
	}
	g.newestRateRejectNS.Store(newest)
}

// updatedLocked stamps the mutation time and publishes. Recorders call this at
// the end of every mutating section.
func (g *gateState) updatedLocked(now time.Time) {
	g.touched = now
	g.publishLocked()
}

func unixNanoOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// breakerOpenAt reports whether the node-health breaker is open at nowNS.
// Lock-free; nil-safe (no gate = no state).
func (g *gateState) breakerOpenAt(nowNS int64) bool {
	return g != nil && nowNS < g.breakerOpenUntilNS.Load()
}

// ejectedAt reports whether the identity is health-ejected at nowNS.
// Lock-free; nil-safe.
func (g *gateState) ejectedAt(nowNS int64) bool {
	return g != nil && nowNS < g.ejectionUntilNS.Load()
}

// hasPairState reports (lock-free) whether any of the flagged per-model
// trackers holds an entry. Readers use it to skip the lock section entirely.
func (g *gateState) hasPairState(flag uint32) bool {
	return g != nil && g.pairFlags.Load()&flag != 0
}

// mergeLocked folds src's state into g for an identity rebind whose new key
// ALREADY has history (e.g. this session's sekey:-keyed faults migrating onto
// a serial: gate populated by a previous connection). Merge policy: expiries
// and streak recency take the max, trip counts take the max, timestamp
// histories merge chronologically, health rings merge in timestamp order
// bounded by the ring size (providerHealthWindow.merge) so an in-progress
// consecutive-fault streak survives the rebind, and a clamp entry with the
// later clamp time wins whole. Caller holds BOTH g.mu and src.mu (only the
// identity bind, under gatesMu.Lock, ever does).
func (g *gateState) mergeLocked(src *gateState) {
	for model, expiry := range src.dispatchLoadCooldowns {
		if cur, ok := g.dispatchLoadCooldowns[model]; !ok || expiry.After(cur) {
			g.dispatchLoadCooldowns[model] = expiry
		}
	}
	for k, strikes := range src.inferenceErrorStrikes {
		g.inferenceErrorStrikes[k] = mergeChronologicalTimestamps(g.inferenceErrorStrikes[k], strikes)
	}
	for k, expiry := range src.inferenceErrorCooldowns {
		if cur, ok := g.inferenceErrorCooldowns[k]; !ok || expiry.After(cur) {
			g.inferenceErrorCooldowns[k] = expiry
		}
	}

	if src.outcomes != nil {
		if g.outcomes != nil {
			g.outcomes.merge(src.outcomes)
		} else {
			g.outcomes = src.outcomes
		}
	}
	if src.breakerUntil.After(g.breakerUntil) {
		g.breakerUntil = src.breakerUntil
	}
	if src.breakerTrips > g.breakerTrips {
		g.breakerTrips = src.breakerTrips
	}

	for model, strikes := range src.capacityRejectStrikes {
		g.capacityRejectStrikes[model] = mergeChronologicalTimestamps(g.capacityRejectStrikes[model], strikes)
	}
	for model, entry := range src.capacityCooldowns {
		if cur, ok := g.capacityCooldowns[model]; !ok || entry.expiry.After(cur.expiry) {
			g.capacityCooldowns[model] = entry
		}
	}
	for model, trips := range src.capacityCooldownTrips {
		if cur, ok := g.capacityCooldownTrips[model]; !ok || trips > cur {
			g.capacityCooldownTrips[model] = trips
		}
	}
	for model, entry := range src.budgetClamps {
		if cur, ok := g.budgetClamps[model]; !ok || entry.clampedAt.After(cur.clampedAt) {
			g.budgetClamps[model] = entry
		}
	}
	for model, outcomes := range src.capacityRateRejects {
		g.capacityRateRejects[model] = mergeChronologicalTimestamps(g.capacityRateRejects[model], outcomes)
	}
	for model, outcomes := range src.capacityRateAccepts {
		g.capacityRateAccepts[model] = mergeChronologicalTimestamps(g.capacityRateAccepts[model], outcomes)
	}

	if src.ejection != nil {
		if g.ejection != nil {
			g.ejection.merge(src.ejection)
		} else {
			g.ejection = src.ejection
		}
	}
	if src.ejectionUntil.After(g.ejectionUntil) {
		g.ejectionUntil = src.ejectionUntil
	}
	// The last-trip marker travels with the trip count it describes: the
	// destination's own marker wins when it has trips of its own.
	if g.ejectionTrips == 0 {
		g.ejectionLastTripCapacity = src.ejectionLastTripCapacity
	}
	if src.ejectionTrips > g.ejectionTrips {
		g.ejectionTrips = src.ejectionTrips
	}
	if src.ejectionCapacityStreak.n > g.ejectionCapacityStreak.n {
		g.ejectionCapacityStreak = src.ejectionCapacityStreak
	}
	if src.touched.After(g.touched) {
		g.touched = src.touched
	}
}

// resetLocked drops every tracker (the key, live count and forwardTo stay).
// After a migration the source key starts from nothing, as the old map-keyed
// implementation left it. Caller holds g.mu.
func (g *gateState) resetLocked() {
	g.outcomes, g.breakerUntil, g.breakerTrips = nil, time.Time{}, 0
	g.ejection, g.ejectionUntil, g.ejectionTrips = nil, time.Time{}, 0
	g.ejectionCapacityStreak, g.ejectionLastTripCapacity = capacityStreak{}, false
	g.inferenceErrorStrikes = make(map[modelShapeKey][]time.Time)
	g.inferenceErrorCooldowns = make(map[modelShapeKey]time.Time)
	g.dispatchLoadCooldowns = make(map[string]time.Time)
	g.capacityRejectStrikes = make(map[string][]time.Time)
	g.capacityCooldowns = make(map[string]*capacityCooldownEntry)
	g.capacityCooldownTrips = make(map[string]int)
	g.budgetClamps = make(map[string]*budgetClampEntry)
	g.capacityRateRejects = make(map[string][]time.Time)
	g.capacityRateAccepts = make(map[string][]time.Time)
}

// pruneLocked drops per-model entries that can no longer influence routing
// and reports whether the whole gate is idle: nothing open, no windowed
// history, no live per-model state. It mirrors what the old size-triggered
// map sweeps removed — with one deliberate difference: half-open memory
// (breaker/cooldown trip counts, an expired cooldown entry awaiting its
// probe, a fault ring's consecutive-fault counter) is NEVER pruned from a gate
// that still has a live session, because the old sweeps only ran past 1024
// entries and the half-open re-arm semantics depend on that memory. Such
// memory goes only when the whole gate is dropped. Caller holds g.mu.
func (g *gateState) pruneLocked(r *Registry, now time.Time) (idle bool) {
	idle = true
	for model, expiry := range g.dispatchLoadCooldowns {
		if !now.Before(expiry) {
			delete(g.dispatchLoadCooldowns, model)
		}
	}
	for k, expiry := range g.inferenceErrorCooldowns {
		if !now.Before(expiry) {
			delete(g.inferenceErrorCooldowns, k)
		}
	}
	for k, strikes := range g.inferenceErrorStrikes {
		if len(strikes) == 0 || !strikes[len(strikes)-1].Add(inferenceErrorWindow).After(now) {
			delete(g.inferenceErrorStrikes, k)
		}
	}
	window := r.capacityCooldownCfg.Window
	for model, strikes := range g.capacityRejectStrikes {
		if len(strikes) == 0 || !strikes[len(strikes)-1].Add(window).After(now) {
			delete(g.capacityRejectStrikes, model)
		}
	}
	for model, e := range g.budgetClamps {
		if !now.Before(e.clampedAt.Add(r.budgetClampCfg.TTL)) {
			delete(g.budgetClamps, model)
		}
	}
	for model, outcomes := range g.capacityRateRejects {
		if len(outcomes) == 0 || now.Sub(outcomes[len(outcomes)-1]) >= capacityRateWindow {
			delete(g.capacityRateRejects, model)
		}
	}
	for model, outcomes := range g.capacityRateAccepts {
		if len(outcomes) == 0 || now.Sub(outcomes[len(outcomes)-1]) >= capacityRateWindow {
			delete(g.capacityRateAccepts, model)
		}
	}

	if len(g.dispatchLoadCooldowns)+len(g.inferenceErrorCooldowns)+len(g.inferenceErrorStrikes)+
		len(g.capacityRejectStrikes)+len(g.budgetClamps)+len(g.capacityRateRejects)+len(g.capacityRateAccepts) > 0 {
		idle = false
	}
	for _, e := range g.capacityCooldowns {
		// An expired entry with a fresh probe claim is still gating (see
		// capacityCooldownActiveLocked); one whose claim is stale or absent is
		// half-open memory only.
		if now.Before(e.expiry) || (!e.probeAt.IsZero() && now.Before(e.probeAt.Add(capacityProbeOutcomeWindow))) {
			idle = false
		}
	}
	if now.Before(g.breakerUntil) || now.Before(g.ejectionUntil) {
		idle = false
	}
	if g.outcomes != nil {
		if total, _ := g.outcomes.windowStats(now, providerBreakerWindow); total > 0 {
			idle = false
		}
	}
	if g.ejection != nil {
		if total, _ := g.ejection.windowStats(now, healthEjectionWindow); total > 0 {
			idle = false
		}
	}
	if g.ejectionCapacityStreak.n > 0 && now.Sub(g.ejectionCapacityStreak.last) <= healthEjectionWindow {
		idle = false
	}
	return idle
}

// gateRef is a recorder's resolution of a gate: the gate plus how it was
// reached, so that lockGate can prove — under gate.mu — that the gate is
// still the one the outcome belongs to, and re-resolve when it is not. A
// value type: the recorder path allocates nothing.
type gateRef struct {
	g *gateState
	// p is the live session whose cached p.gate produced g; nil when g was
	// reached through the disconnect cache, the bare session id or an
	// explicit fault key (nothing can rebind those). After the lock,
	// p.gate.Load() != g means the session rebound to another gate in
	// between and the outcome belongs there.
	p *Provider
	// Exactly one of session / key names what to re-resolve.
	session string
	key     string
	// insert says whether re-resolution may create the gate (gateForSession /
	// gateForKey) or must find an existing one (lookupSessionGateRef: the
	// "clear" recorders, which have nothing to clear on an identity without a
	// gate and must not file one under a dead session id).
	insert bool
}

// currentLocked reports whether g — locked by the caller — is still the gate
// ref's outcome belongs to: not retired by the sweep, and, when the ref was
// reached through a live session's cached pointer, still that session's gate.
func (ref gateRef) currentLocked(g *gateState) bool {
	if g.retired {
		return false
	}
	return ref.p == nil || ref.p.gate.Load() == g
}

// gateRelockMaxRetries bounds how many times lockGate re-resolves a gate that
// went stale between the index lookup and the lock. One retry is the
// expected maximum (a rebind or a sweep landed in the window); the bound only
// guarantees termination under an adversarial schedule, where the recorder
// falls back to the gate it holds — today's behaviour, no worse.
const gateRelockMaxRetries = 4

// lockGate acquires gate.mu for a recorder at the named site. It follows any
// migration forward (see lockResolved) and then validates, under the lock,
// that the gate is still current for the ref (currentLocked); a stale gate is
// released and the ref re-resolved through the index — gatesMu is never
// taken while a gate.mu is held, so the lock order stands — and locked again.
// The uncontended path is one TryLock — no clock reads; only a contended
// acquisition is timed, and only a wait above gateWaitReportThreshold is
// reported. The caller uses hold.g (the gate actually locked) and calls
// hold.unlock.
func (r *Registry) lockGate(ref gateRef, site string) gateHold {
	var wait time.Duration
	g := ref.g
	retries := 0
	for {
		if !g.mu.TryLock() {
			start := time.Now()
			g.mu.Lock()
			wait += time.Since(start)
		}
		if next := g.forwardTo.Load(); next != nil {
			g.mu.Unlock()
			g = next
			continue
		}
		if retries >= gateRelockMaxRetries || ref.currentLocked(g) {
			return gateHold{g: g, r: r, site: site, wait: wait}
		}
		// Stale: the sweep retired g, or the session rebound to another gate
		// while we were between the index and the lock. Release, re-resolve
		// through the index and try again.
		g.mu.Unlock()
		retries++
		fresh := r.reresolveGate(ref)
		if fresh.g == nil {
			// A no-insert ref (a clear) whose identity has no gate any more:
			// nothing left to clear. Keep the retired gate — its state is out
			// of the index, so the write is a no-op — rather than file a gate
			// under an identity nothing references.
			fresh.g = g
			retries = gateRelockMaxRetries
		}
		ref = fresh
		g = fresh.g
	}
}

// reresolveGate resolves ref again through the index (takes gatesMu; the
// caller holds no gate.mu).
func (r *Registry) reresolveGate(ref gateRef) gateRef {
	if ref.key != "" {
		return r.gateForKey(ref.key)
	}
	return r.sessionGateRef(ref.session, ref.insert)
}

// gateHold is an acquired gate.mu. unlock releases the gate and only then
// reports a long acquisition wait to the observer, so the DogStatsD emit never
// runs inside the critical section.
type gateHold struct {
	g    *gateState
	r    *Registry
	site string
	wait time.Duration
}

func (h gateHold) unlock() {
	h.g.mu.Unlock()
	if h.wait > gateWaitReportThreshold {
		if obs := h.r.gateWaitObserver.Load(); obs != nil {
			(*obs)(h.site, h.wait)
		}
	}
}

// SetGateWaitObserver registers an optional observer for gate.mu acquisition
// waits above gateWaitReportThreshold on the recorder sites. The api layer
// turns it into the registry.gate.wait_ms histogram tagged by site, so the
// per-identity locks that replaced the global write lock are observable. Set
// once at startup; nil clears it. Thread-safe.
func (r *Registry) SetGateWaitObserver(fn func(site string, wait time.Duration)) {
	if fn == nil {
		r.gateWaitObserver.Store(nil)
		return
	}
	r.gateWaitObserver.Store(&fn)
}

// HoldGateForTest acquires the gate.mu of the provider's current gate and
// returns the release function. Test-only (mirrors reservationAfterScan): it
// lets a test prove a recorder blocks on the identity's gate, not on r.mu, and
// that a long wait reaches the observer. Production code never calls it.
func (r *Registry) HoldGateForTest(providerID string) (release func()) {
	hold := r.lockGate(r.gateForSession(providerID), "test")
	return hold.g.mu.Unlock
}

// --- Registry side: the gate index ---

// gatesInit lazily creates the gate index so bare &Registry{} test
// constructions work without New(). Caller holds gatesMu for writing.
func (r *Registry) gatesInitLocked() {
	if r.gates == nil {
		r.gates = make(map[string]*gateState)
	}
	if r.sessions == nil {
		r.sessions = make(map[string]*Provider)
	}
	if r.disconnectedStableIDs == nil {
		r.disconnectedStableIDs = make(map[string]disconnectedStableID)
	}
}

// ensureGateLocked returns the gate for key, creating it on first use. Caller
// holds gatesMu for writing. A creation past the high-water mark runs the
// rate-limited inline sweep so the index stays bounded without the eviction
// loop.
func (r *Registry) ensureGateLocked(key string, now time.Time) *gateState {
	r.gatesInitLocked()
	if g, ok := r.gates[key]; ok {
		return g
	}
	if len(r.gates) > gateSweepHighWater && now.Sub(r.gateSweepAt) > gateSweepMinInterval {
		r.sweepGatesLocked(now)
	}
	g := newGateState(key)
	g.touched = now // creation counts as activity: see gateState.touched
	r.gates[key] = g
	return g
}

// gateForKey returns the gate filed under an explicit fault key / stable
// identity (RecordProviderServeOutcome is keyed by the caller's stable id),
// creating it on first use. One gatesMu.RLock in the common case.
func (r *Registry) gateForKey(key string) gateRef {
	r.gatesMu.RLock()
	g := r.gates[key]
	r.gatesMu.RUnlock()
	if g == nil {
		r.gatesMu.Lock()
		g = r.ensureGateLocked(key, time.Now())
		r.gatesMu.Unlock()
	}
	return gateRef{g: g.resolve(), key: key, insert: true}
}

// lookupGateForKey is gateForKey without the insert: nil when the identity has
// no state.
func (r *Registry) lookupGateForKey(key string) *gateState {
	r.gatesMu.RLock()
	g := r.gates[key]
	r.gatesMu.RUnlock()
	return g.resolve()
}

// gateForSession resolves a live session id to its identity's gate, creating
// the (session-keyed) gate when the identity has none. Precedence mirrors the
// old faultKeyLocked: the bound identity of a live session → the identity
// cached at Disconnect for the trailing ErrorCh flush → the session id itself.
// Recorders call this; it never touches r.mu.
func (r *Registry) gateForSession(sessionID string) gateRef {
	return r.sessionGateRef(sessionID, true)
}

// lookupSessionGateRef is gateForSession without the insert: ref.g is nil when
// the session's identity has no state. The recorders that only CLEAR state
// (and so have nothing to do for an identity without a gate) resolve through
// this, so a straggling clear for a dead session never files a gate under
// its id — including when lockGate has to re-resolve.
func (r *Registry) lookupSessionGateRef(sessionID string) gateRef {
	return r.sessionGateRef(sessionID, false)
}

// lookupGateForSession is the plain-pointer form of lookupSessionGateRef for
// readers (the scan's fallback for a bare provider, the *Active probes,
// tests): nil when the session's identity has no state.
func (r *Registry) lookupGateForSession(sessionID string) *gateState {
	return r.sessionGateRef(sessionID, false).g
}

func (r *Registry) sessionGateRef(sessionID string, insert bool) gateRef {
	r.gatesMu.RLock()
	g, key, via := r.resolveSessionGateLocked(sessionID)
	r.gatesMu.RUnlock()
	if g == nil && insert {
		r.gatesMu.Lock()
		g = r.ensureGateLocked(key, time.Now())
		r.gatesMu.Unlock()
	}
	return gateRef{g: g.resolve(), p: via, session: sessionID, insert: insert}
}

// resolveSessionGateLocked returns the gate a session resolves to (nil when
// its key has no gate yet), that key, and — when the gate came from a live
// session's cached pointer — that session's Provider, so the caller can later
// detect a rebind (gateRef.p). Caller holds gatesMu (either mode).
func (r *Registry) resolveSessionGateLocked(sessionID string) (g *gateState, key string, via *Provider) {
	if p := r.sessions[sessionID]; p != nil {
		if g := p.gate.Load(); g != nil {
			return g, g.key, p
		}
		return r.gates[sessionID], sessionID, nil
	}
	if c, ok := r.disconnectedStableIDs[sessionID]; ok && c.id != "" && time.Since(c.at) < disconnectedStableIDTTL {
		return r.gates[c.id], c.id, nil
	}
	return r.gates[sessionID], sessionID, nil
}

// faultKeyForSession resolves a session provider id to the key its fault state
// lives under: the bound stable identity (serial/SE-key/account), the identity
// cached at Disconnect for the trailing ErrorCh flush, or — when no identity
// was ever available — the session id itself. Takes gatesMu for reading.
func (r *Registry) faultKeyForSession(sessionID string) string {
	r.gatesMu.RLock()
	defer r.gatesMu.RUnlock()
	_, key, _ := r.resolveSessionGateLocked(sessionID)
	return key
}

// sessionProvider returns the live Provider for a session id without touching
// r.mu (the recorders that need a budget snapshot read p.BackendCapacity
// under p.mu). nil when the session is gone.
func (r *Registry) sessionProvider(sessionID string) *Provider {
	r.gatesMu.RLock()
	defer r.gatesMu.RUnlock()
	return r.sessions[sessionID]
}

// gateOf returns the gate the scan should consult for p: the pointer cached on
// the connected Provider (no lock), falling back to a session lookup for a
// Provider that was never registered (bare test objects). nil-safe result:
// every gate read treats a nil gate as "no state".
func (r *Registry) gateOf(p *Provider) *gateState {
	if p == nil {
		return nil
	}
	if g := p.gate.Load(); g != nil {
		return g.resolve()
	}
	return r.lookupGateForSession(p.ID)
}

// attachSessionGate files a freshly registered session under its own id and
// caches the gate on the Provider. Called by Register (under r.mu; the order
// r.mu → gatesMu holds).
func (r *Registry) attachSessionGate(p *Provider) {
	now := time.Now()
	r.gatesMu.Lock()
	defer r.gatesMu.Unlock()
	g := r.ensureGateLocked(p.ID, now)
	g.live++
	p.gate.Store(g)
	r.sessions[p.ID] = p
}

// detachSessionGate removes a disconnecting session from the index. stableID
// is the identity derived at disconnect: when present it is cached so the
// trailing pending-request flush still resolves the session (its state lives
// on under the identity's gate — FAULT STATE IS NOT CLEARED ON DISCONNECT);
// when absent the session-keyed gate is the only thing that ever referenced
// this identity, so its residue is dropped for hygiene, exactly as the old
// implementation dropped the session-keyed map entries. Called by Disconnect
// (under r.mu and p.mu; the order r.mu → p.mu → gatesMu holds).
func (r *Registry) detachSessionGate(p *Provider, stableID string) {
	r.gatesMu.Lock()
	defer r.gatesMu.Unlock()
	r.gatesInitLocked()
	delete(r.sessions, p.ID)
	g := p.gate.Load()
	if g != nil {
		g.live--
	}
	if stableID != "" {
		r.rememberDisconnectedStableIDLocked(p.ID, stableID)
		return
	}
	if g != nil && g.key == p.ID && g.live <= 0 && r.gates[p.ID] == g {
		delete(r.gates, p.ID)
	}
}

// bindStableFaultKey binds a live session to its stable identity so every
// fault tracker keys by identity and survives reconnects. Called by
// SetAttestationResult on every (re-)attestation — i.e. BEFORE the session is
// routable for public traffic — which is what re-attaches a reconnecting
// machine's accumulated fault state to its fresh session id. An empty stableID
// (attestation cleared / never valid) unbinds, falling back to session keying;
// the identity's state stays on its own gate (an unbind never migrates).
//
// Only LIVE sessions bind: a re-attestation racing Disconnect must not
// re-insert an entry Disconnect already removed. Liveness is the sessions
// index under gatesMu, so this never takes r.mu.
//
// Accumulated fault state migrates when the key changes: from the session id
// on the FIRST bind (strikes recorded pre-attestation live on the session
// gate), or from the previous identity on a rebind (e.g. sekey: → serial:
// after MDA enrichment). Without this, a machine near quarantine sheds its
// history at the exact moment its identity improves. No-op on the common
// re-attestation with an unchanged identity.
func (r *Registry) bindStableFaultKey(p *Provider, stableID string) {
	if p == nil || p.ID == "" {
		return
	}
	now := time.Now()
	r.gatesMu.Lock()
	defer r.gatesMu.Unlock()
	r.gatesInitLocked()
	if r.sessions[p.ID] != p {
		return
	}
	targetKey := stableID
	if targetKey == "" {
		targetKey = p.ID
	}
	cur := p.gate.Load()
	if cur != nil && cur.key == targetKey {
		return
	}
	target := r.ensureGateLocked(targetKey, now)
	if cur != nil && stableID != "" {
		// Migrate, and repoint the session INSIDE the migration's locked
		// section: a lock-free reader that loads p.gate must find either cur's
		// intact pre-migration view or a target that already carries the
		// merged state, never an empty target; and a recorder that resolved
		// cur before this rebind must, once it holds cur.mu, either have
		// written before the merge (its outcome travels to target) or find
		// p.gate moved and re-resolve (lockGate) — never write into the
		// emptied cur. That matters most when cur is SHARED with another
		// session (cur.live > 1): it stays in the index for that session and
		// carries no forward, so the session's own pointer is the only thing
		// that can tell a stale recorder its outcome now belongs to target.
		// (An unbind never migrates: session keying resumes and the identity
		// keeps its state.) cur is orphaned when this was its last live
		// session.
		r.migrateGateLocked(p, cur, target, cur.live <= 1)
	} else {
		p.gate.Store(target)
	}
	target.live++
	if cur != nil {
		cur.live--
	}
}

// migrateGateLocked re-keys accumulated fault state from src to dst (merge
// policy in mergeLocked) and repoints p (the session being bound) at dst
// while BOTH gates are locked, so no recorder can hold src.mu with p still
// pointing at src after the merge. src is emptied afterwards; when no live
// session still points at it (orphan) it is forwarded to dst and dropped from
// the index so stale pointers land on the live state. The only place two gate
// locks nest; caller holds gatesMu for writing, which serializes migrations.
func (r *Registry) migrateGateLocked(p *Provider, src, dst *gateState, orphan bool) {
	if src == dst {
		p.gate.Store(dst)
		return
	}
	src.mu.Lock()
	dst.mu.Lock()
	dst.mergeLocked(src)
	dst.publishLocked()
	p.gate.Store(dst)
	if orphan {
		// Forward BEFORE resetting, and never republish the orphan: a
		// lock-free reader that loaded src sees either its intact pre-merge
		// atomics (stale but conservative — expiries merged by max into dst)
		// or the forward, never zeros. Locked reads follow the forward.
		src.forwardTo.Store(dst)
		if r.gates[src.key] == src {
			delete(r.gates, src.key)
		}
		src.resetLocked()
	} else {
		// Still bound to other sessions: it starts from nothing, visibly.
		src.resetLocked()
		src.publishLocked()
	}
	dst.mu.Unlock()
	src.mu.Unlock()
}

// sweepGates bounds the gate index: it prunes dead per-model entries from
// every gate and drops gates that no live session references once they have
// been idle for gateIdleGrace. Called from the eviction loop (every
// timeout/3) and inline from an insert past the high-water mark. Each gate is
// locked for microseconds; gatesMu is held for the walk, which is why this
// runs at most every few seconds and never on the request path.
func (r *Registry) sweepGates(now time.Time) {
	r.gatesMu.Lock()
	defer r.gatesMu.Unlock()
	r.gatesInitLocked()
	r.sweepGatesLocked(now)
}

func (r *Registry) sweepGatesLocked(now time.Time) {
	r.gateSweepAt = now
	for key, g := range r.gates {
		g.mu.Lock()
		idle := g.pruneLocked(r, now)
		g.publishLocked()
		drop := idle && g.live <= 0 && now.Sub(g.touched) > gateIdleGrace
		if drop {
			// Retire under g.mu BEFORE the index delete: a recorder that
			// resolved this gate before the walk and locks it afterwards
			// sees retired and re-resolves (lockGate) instead of writing a
			// trailing fault into a gate no lookup will ever find again.
			g.retired = true
		}
		g.mu.Unlock()
		if drop {
			delete(r.gates, key)
		}
	}
	cutoff := now.Add(-disconnectedStableIDTTL)
	for k, v := range r.disconnectedStableIDs {
		if v.at.Before(cutoff) {
			delete(r.disconnectedStableIDs, k)
		}
	}
}

// gateCount reports the size of the gate index (tests / observability).
func (r *Registry) gateCount() int {
	r.gatesMu.RLock()
	defer r.gatesMu.RUnlock()
	return len(r.gates)
}

// --- reservation commit lock mode ---

// reserveCommitMode selects how commitProviderReservation and
// ReserveNextFromPlan hold the registry lock while they debit a provider.
type reserveCommitMode uint8

const (
	// reserveCommitShared (default) commits under r.mu.RLock + p.mu: the
	// double-booking guard is the admit re-check under p.mu, the herd guard
	// is the "winner unchanged since scan" compare under the same p.mu, and
	// the probe claim is atomic under gate.mu. Commits no longer drain the
	// fleet-scan reader batch.
	reserveCommitShared reserveCommitMode = iota
	// reserveCommitGlobal is the kill switch: commits take r.mu.Lock() and
	// serialize fleet-wide exactly as before. The recorders stay on their
	// per-identity gates in both modes — that half is safe on its own.
	reserveCommitGlobal
)

// envReserveCommitMode selects the mode: "global" restores the fleet-wide
// commit serialization; anything else (including unset) is "shared". Read
// once at Registry construction.
const envReserveCommitMode = "EIGENINFERENCE_RESERVE_COMMIT_MODE"

func loadReserveCommitMode(logger *slog.Logger) reserveCommitMode {
	raw := os.Getenv(envReserveCommitMode)
	mode, known := parseReserveCommitMode(raw)
	if !known && logger != nil {
		// A kill switch that silently ignores a typo is not a kill switch.
		logger.Warn("unknown reserve commit mode; using shared",
			"env", envReserveCommitMode, "value", raw)
	}
	return mode
}

// parseReserveCommitMode maps the raw value to a mode; known reports whether
// the value named one ("" and "shared" are shared, "global" is global).
func parseReserveCommitMode(raw string) (mode reserveCommitMode, known bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "shared":
		return reserveCommitShared, true
	case "global":
		return reserveCommitGlobal, true
	default:
		return reserveCommitShared, false
	}
}

func (m reserveCommitMode) String() string {
	if m == reserveCommitGlobal {
		return "global"
	}
	return "shared"
}

// commitLock is the registry lock held across a reservation commit in the
// configured mode. A value type so the commit path allocates nothing.
type commitLock struct {
	r      *Registry
	global bool
}

func (r *Registry) commitLock() commitLock {
	return commitLock{r: r, global: r.reserveCommitMode == reserveCommitGlobal}
}

func (l commitLock) lock() {
	if l.global {
		l.r.mu.Lock()
	} else {
		l.r.mu.RLock()
	}
}

func (l commitLock) unlock() {
	if l.global {
		l.r.mu.Unlock()
	} else {
		l.r.mu.RUnlock()
	}
}
