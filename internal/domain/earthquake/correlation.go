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
	IncidentID uuid.UUID
	Score      float64
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
		if score, ok := p.score(incoming, candidate.Event); ok {
			ranked = append(ranked, CorrelationMatch{IncidentID: candidate.IncidentID, Score: score})
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

func (p CorrelationPolicy) score(a, b Event) (float64, bool) {
	timeDelta := absDuration(a.OccurredAt.Sub(b.OccurredAt))
	if timeDelta > p.MaximumTimeDelta {
		return 0, false
	}
	distance := surfaceDistanceKM(a.Latitude, a.Longitude, b.Latitude, b.Longitude)
	if distance > p.MaximumDistanceKM {
		return 0, false
	}

	weighted := p.TimeWeight*(1-float64(timeDelta)/float64(p.MaximumTimeDelta)) +
		p.DistanceWeight*(1-distance/p.MaximumDistanceKM)
	weights := p.TimeWeight + p.DistanceWeight
	if a.Magnitude != nil && b.Magnitude != nil {
		diff := math.Abs(*a.Magnitude - *b.Magnitude)
		if diff > p.MaximumMagnitudeDiff {
			return 0, false
		}
		weighted += p.MagnitudeWeight * (1 - diff/p.MaximumMagnitudeDiff)
		weights += p.MagnitudeWeight
	}
	if a.DepthKM != nil && b.DepthKM != nil {
		diff := math.Abs(*a.DepthKM - *b.DepthKM)
		if diff > p.MaximumDepthDiffKM {
			return 0, false
		}
		weighted += p.DepthWeight * (1 - diff/p.MaximumDepthDiffKM)
		weights += p.DepthWeight
	}
	if weights == 0 {
		return 0, false
	}
	return weighted / weights, true
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
