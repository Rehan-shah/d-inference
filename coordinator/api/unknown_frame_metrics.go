package api

import (
	"strings"

	"github.com/eigeninference/d-inference/coordinator/registry"
)

// Unknown-frame instrumentation (zombie-stream amplifier visibility).
//
// A provider that keeps generating into a request the coordinator already
// abandoned (consumer gone, first-chunk timeout, settled) sends chunk /
// complete / error frames whose request_id matches no pending state. The
// coordinator logs each one and (throttled) re-sends a cancel, but during the
// 2026-08-31 cascade ~65K such frames in 10 minutes were invisible on any
// panel: inference.zombie_stream_cancel is throttled and untagged. This
// counter is the raw, unthrottled count, tagged by the FRAME KIND and the
// provider's binary version — bounded vocabularies — so a zombie wave can be
// pinned to a release. It never carries the provider id or the request id.
const (
	metricUnknownFrames        = "inference.unknown_frames"
	metricUnknownFramesCounter = "inference_unknown_frames_total"

	unknownFrameKindChunk    = "chunk"
	unknownFrameKindComplete = "complete"
	unknownFrameKindError    = "error"
)

// maxVersionTagLen bounds the provider_version tag: a real semver string is
// short, and anything longer is treated as untrusted input.
const maxVersionTagLen = 32

// emitUnknownFrame counts one provider frame for an unknown request id.
func (s *Server) emitUnknownFrame(kind string, provider *registry.Provider) {
	if s == nil {
		return
	}
	version := providerVersionTag(provider)
	if s.metrics != nil {
		s.metrics.IncCounter(metricUnknownFramesCounter,
			MetricLabel{"kind", kind}, MetricLabel{"provider_version", version})
	}
	if s.dd == nil {
		return
	}
	s.ddIncr(metricUnknownFrames, []string{"kind:" + kind, "provider_version:" + version})
}

// providerVersionTag reads the provider's reported binary version under its
// lock and fences it to a bounded, tag-safe value.
func providerVersionTag(p *registry.Provider) string {
	if p == nil {
		return "unknown"
	}
	p.Mu().Lock()
	version := p.Version
	p.Mu().Unlock()
	return sanitizeVersionTag(version)
}

// sanitizeVersionTag keeps a version string only when it is short and made of
// the characters a release version can contain (digits, letters, '.', '-').
// Everything else — including the empty string — is normalized so a
// provider-controlled value can never mint tag cardinality.
func sanitizeVersionTag(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return "unknown"
	}
	if len(version) > maxVersionTagLen {
		return "invalid"
	}
	for _, c := range version {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '.', c == '-':
		default:
			return "invalid"
		}
	}
	return version
}
