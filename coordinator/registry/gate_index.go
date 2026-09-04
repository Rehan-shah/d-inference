package registry

import "time"

// gate_index.go — the registry side of the per-identity gates: the r.gates /
// r.sessions index under gatesMu, session → gate resolution (gateRef), the
// scan's gateOf, and attach/detach at Register / Disconnect. Design, lock
// order and file map: gate_state.go.

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

// gateCount reports the size of the gate index (tests / observability).
func (r *Registry) gateCount() int {
	r.gatesMu.RLock()
	defer r.gatesMu.RUnlock()
	return len(r.gates)
}
