package earthquake

import (
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
)

type SolutionClass string

const (
	PreliminarySolution SolutionClass = "preliminary"
	ConfirmedSolution   SolutionClass = "confirmed"
	ReviewedSolution    SolutionClass = "reviewed"
	RetractedSolution   SolutionClass = "retracted"
)

func (s SolutionClass) Valid() bool {
	switch s {
	case PreliminarySolution, ConfirmedSolution, ReviewedSolution, RetractedSolution:
		return true
	default:
		return false
	}
}

// EffectiveSolutionClass keeps existing providers backward compatible. New
// low-latency adapters must explicitly mark preliminary observations.
func (e Event) EffectiveSolutionClass() SolutionClass {
	if e.SolutionClass.Valid() {
		return e.SolutionClass
	}
	return ConfirmedSolution
}

func (e Event) EffectiveObservationChannel() string {
	if e.ObservationChannel != "" {
		return e.ObservationChannel
	}
	return "legacy"
}

type Lifecycle string

const (
	Preliminary Lifecycle = "preliminary"
	Confirmed   Lifecycle = "confirmed"
	Reviewed    Lifecycle = "reviewed"
	Retracted   Lifecycle = "retracted"
)

// ResolveLifecycle uses all active provider evidence. A retraction from one
// provider does not override another active solution.
func ResolveLifecycle(solutions []SolutionClass) Lifecycle {
	best := 0
	for _, solution := range solutions {
		rank := 0
		switch solution {
		case PreliminarySolution:
			rank = 1
		case ConfirmedSolution:
			rank = 2
		case ReviewedSolution:
			rank = 3
		}
		if rank > best {
			best = rank
		}
	}
	switch best {
	case 3:
		return Reviewed
	case 2:
		return Confirmed
	case 1:
		return Preliminary
	default:
		return Retracted
	}
}

// StrongerSolution keeps the highest-quality active solution. Retraction is a
// state transition and is therefore handled only from a non-stale observation.
func StrongerSolution(current, incoming SolutionClass) SolutionClass {
	if current == RetractedSolution {
		return current
	}
	if solutionRank(incoming) > solutionRank(current) {
		return incoming
	}
	return current
}

func solutionRank(solution SolutionClass) int {
	switch solution {
	case PreliminarySolution:
		return 1
	case ConfirmedSolution:
		return 2
	case ReviewedSolution:
		return 3
	default:
		return 0
	}
}

type CorrelationPolicy struct {
	Version              string
	MaximumTimeDelta     time.Duration
	MaximumDistanceKM    float64
	MaximumMagnitudeDiff float64
	MaximumDepthDiffKM   float64
	TimeWeight           float64
	DistanceWeight       float64
	MagnitudeWeight      float64
	DepthWeight          float64
	AcceptanceThreshold  float64
	AmbiguityMargin      float64
}

func (p CorrelationPolicy) Valid() bool {
	return p.Version != "" && p.MaximumTimeDelta > 0 && p.MaximumDistanceKM > 0 &&
		p.MaximumMagnitudeDiff > 0 && p.MaximumDepthDiffKM > 0 &&
		p.TimeWeight >= 0 && p.DistanceWeight >= 0 && p.MagnitudeWeight >= 0 && p.DepthWeight >= 0 &&
		p.TimeWeight+p.DistanceWeight+p.MagnitudeWeight+p.DepthWeight > 0 &&
		p.AcceptanceThreshold >= 0 && p.AcceptanceThreshold <= 1 &&
		p.AmbiguityMargin >= 0 && p.AmbiguityMargin <= 1
}

type CorrelationCandidate struct {
	IncidentID uuid.UUID
	Event      Event
}

type CorrelationMatch struct {
	IncidentID       uuid.UUID `json:"incident_id"`
	Score            float64   `json:"score"`
	TimeDeltaSeconds float64   `json:"time_delta_seconds"`
	DistanceKM       float64   `json:"distance_km"`
	MagnitudeDiff    *float64  `json:"magnitude_diff,omitempty"`
	DepthDiffKM      *float64  `json:"depth_diff_km,omitempty"`
}

type CorrelationDecision struct {
	Match     *CorrelationMatch
	Ambiguous bool
	Ranked    []CorrelationMatch
}

// Correlate ranks candidates but deliberately leaves policy calibration and
// activation to the application layer.
func (p CorrelationPolicy) Correlate(incoming Event, candidates []CorrelationCandidate) CorrelationDecision {
	if !p.Valid() {
		return CorrelationDecision{}
	}
	ranked := make([]CorrelationMatch, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Event.Provider == incoming.Provider {
			continue
		}
		if match, ok := p.score(incoming, candidate.Event); ok {
			match.IncidentID = candidate.IncidentID
			ranked = append(ranked, match)
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Score == ranked[j].Score {
			return ranked[i].IncidentID.String() < ranked[j].IncidentID.String()
		}
		return ranked[i].Score > ranked[j].Score
	})
	decision := CorrelationDecision{Ranked: ranked}
	if len(ranked) == 0 || ranked[0].Score < p.AcceptanceThreshold {
		return decision
	}
	if len(ranked) > 1 && ranked[0].Score-ranked[1].Score < p.AmbiguityMargin {
		decision.Ambiguous = true
		return decision
	}
	match := ranked[0]
	decision.Match = &match
	return decision
}

func (p CorrelationPolicy) score(a, b Event) (CorrelationMatch, bool) {
	timeDelta := absDuration(a.OccurredAt.Sub(b.OccurredAt))
	if timeDelta > p.MaximumTimeDelta {
		return CorrelationMatch{}, false
	}
	distance := surfaceDistanceKM(a.Latitude, a.Longitude, b.Latitude, b.Longitude)
	if distance > p.MaximumDistanceKM {
		return CorrelationMatch{}, false
	}
	match := CorrelationMatch{TimeDeltaSeconds: timeDelta.Seconds(), DistanceKM: distance}

	weighted := p.TimeWeight*(1-float64(timeDelta)/float64(p.MaximumTimeDelta)) +
		p.DistanceWeight*(1-distance/p.MaximumDistanceKM)
	weights := p.TimeWeight + p.DistanceWeight
	if a.Magnitude != nil && b.Magnitude != nil {
		diff := math.Abs(*a.Magnitude - *b.Magnitude)
		if diff > p.MaximumMagnitudeDiff {
			return CorrelationMatch{}, false
		}
		match.MagnitudeDiff = &diff
		weighted += p.MagnitudeWeight * (1 - diff/p.MaximumMagnitudeDiff)
		weights += p.MagnitudeWeight
	}
	if a.DepthKM != nil && b.DepthKM != nil {
		diff := math.Abs(*a.DepthKM - *b.DepthKM)
		if diff > p.MaximumDepthDiffKM {
			return CorrelationMatch{}, false
		}
		match.DepthDiffKM = &diff
		weighted += p.DepthWeight * (1 - diff/p.MaximumDepthDiffKM)
		weights += p.DepthWeight
	}
	if weights == 0 {
		return CorrelationMatch{}, false
	}
	match.Score = weighted / weights
	return match, true
}

func ProductionCorrelationPolicy() CorrelationPolicy {
	return CorrelationPolicy{
		Version: "emsc-usgs-conservative-v1", MaximumTimeDelta: 30 * time.Second,
		MaximumDistanceKM: 25, MaximumMagnitudeDiff: 0.5, MaximumDepthDiffKM: 30,
		TimeWeight: 0.45, DistanceWeight: 0.35, MagnitudeWeight: 0.15, DepthWeight: 0.05,
		AcceptanceThreshold: 0.82, AmbiguityMargin: 0.08,
	}
}

func PreferCanonicalSource(currentProvider string, currentSolution SolutionClass,
	incomingProvider string, incomingSolution SolutionClass) bool {
	currentRank, incomingRank := solutionRank(currentSolution), solutionRank(incomingSolution)
	if incomingRank != currentRank {
		return incomingRank > currentRank
	}
	return providerPriority(incomingProvider) > providerPriority(currentProvider)
}

func providerPriority(provider string) int {
	switch provider {
	case "usgs":
		return 2
	case "emsc":
		return 1
	default:
		return 0
	}
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

func surfaceDistanceKM(latA, lonA, latB, lonB float64) float64 {
	const earthRadiusKM = 6371.0088
	toRadians := math.Pi / 180
	lat1, lat2 := latA*toRadians, latB*toRadians
	dLat, dLon := (latB-latA)*toRadians, (lonB-lonA)*toRadians
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusKM * 2 * math.Atan2(math.Sqrt(h), math.Sqrt(1-h))
}
