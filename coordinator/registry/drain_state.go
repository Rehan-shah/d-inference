package registry

import (
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
)

// Drain awareness (R2, coordinator half).
//
// A provider that is draining ahead of a restart/update refuses every new
// dispatch, but until now nothing told routing: its heartbeat kept reporting
// idle/serving with warm slots, so the cost scheduler kept ranking it as an
// ideal instant-TTFT target and each bounce cost the request one of its three
// transient-capacity retries, derated the pair's gray-box capacity-503 rate,
// and armed its budget clamp — for a healthy box that was about to restart.
//
// Wire strings expected from the provider (the Swift half is implemented
// separately; mirror in provider-swift/Sources/ProviderCore/Protocol/):
//
//   - heartbeat `"status": "draining"` (protocol.HeartbeatStatusDraining)
//     while the provider is refusing new work. Any later heartbeat carrying
//     "idle" or "serving" clears the state immediately; a heartbeat that
//     keeps saying "draining" refreshes the TTL.
//   - inference_error `{"failure_code": "capacity", "status_code": 503,
//     "error_reason": "draining"}` (protocol.InferenceErrorReasonDraining) on
//     the drain admission rejection. The api layer marks the provider
//     draining from that terminal too, so a provider that sends the typed
//     reason but not the heartbeat status is skipped from the next scan on.
//
// Legacy providers send neither and keep today's path (an untyped 503
// sanitized to capacity_busy). Both markers are additive and ignored by older
// coordinators.
//
// Routing: providerPassesRoutingGatesLockedEx skips a draining provider on
// the same branch as the capacity-reject cooldown, so the candidate scan and
// the admission preflight (quickCapacityCheck) count it as a TRANSIENT
// capacityRejection — an all-draining model surfaces as 429 + Retry-After /
// queue material, never as a "no providers" 503.

// drainStateTTL bounds how long a draining mark is honored without a
// refreshing heartbeat: long enough to outlast the 5 s heartbeat cadence and
// the provider's shutdown path (which restarts the socket anyway), short
// enough that a provider whose drain was aborted (restart failed, resumed
// serving) but whose next heartbeat was lost is back in routing within
// minutes even without a clearing heartbeat.
const drainStateTTL = 150 * time.Second

// providerDrainingLocked reports whether p has an unexpired draining mark.
// Caller holds p.mu.
func providerDrainingLocked(p *Provider, now time.Time) bool {
	return !p.drainingUntil.IsZero() && now.Before(p.drainingUntil)
}

// applyHeartbeatDrainStateLocked updates the draining mark from a heartbeat's
// status string: "draining" (re)arms the TTL and takes ownership of the mark,
// "idle"/"serving" clear a HEARTBEAT-owned mark, and any other value
// (legacy/unknown) leaves it untouched. A mark set by a typed draining
// rejection is NOT cleared by idle/serving: a legacy provider that types the
// reason but keeps reporting "idle" would otherwise be re-selected on its
// next heartbeat, bounce, and be re-marked every ~30s. Caller holds p.mu.
func applyHeartbeatDrainStateLocked(p *Provider, status string, now time.Time) {
	switch status {
	case protocol.HeartbeatStatusDraining:
		p.drainingUntil = now.Add(drainStateTTL)
		p.drainingByRejection = false
	case "idle", "serving":
		if p.drainingByRejection && providerDrainingLocked(p, now) {
			return
		}
		p.drainingUntil = time.Time{}
		p.drainingByRejection = false
	}
}

// MarkDraining marks a live provider draining for drainStateTTL (the typed
// draining rejection path). Returns true only on the transition into the
// draining state so callers can log/meter once. Unknown ids are a no-op.
func (r *Registry) MarkDraining(id string) (transitioned bool) {
	r.mu.RLock()
	p := r.providers[id]
	r.mu.RUnlock()
	if p == nil {
		return false
	}
	now := time.Now()
	p.mu.Lock()
	was := providerDrainingLocked(p, now)
	p.drainingUntil = now.Add(drainStateTTL)
	// Ownership: a mark the provider's own heartbeat already owns stays
	// heartbeat-owned, so its next idle/serving heartbeat (an aborted update
	// that resumed serving) still clears it. Only a mark that did not exist
	// becomes rejection-owned — the legacy provider that types the reason but
	// never reports the status.
	if !was {
		p.drainingByRejection = true
	}
	p.mu.Unlock()
	if !was {
		r.logger.Info("provider draining: routing skips it until it reports idle/serving or the drain TTL lapses",
			"provider_id", id, "ttl", drainStateTTL)
	}
	return !was
}

// ProviderDraining reports whether the provider currently carries an
// unexpired draining mark (false for unknown ids).
func (r *Registry) ProviderDraining(id string) bool {
	r.mu.RLock()
	p := r.providers[id]
	r.mu.RUnlock()
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return providerDrainingLocked(p, time.Now())
}
