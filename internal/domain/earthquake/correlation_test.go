package earthquake

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestResolveLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		solutions []SolutionClass
		want      Lifecycle
	}{
		{name: "websocket only", solutions: []SolutionClass{PreliminarySolution}, want: Preliminary},
		{name: "catalog confirms websocket", solutions: []SolutionClass{PreliminarySolution, ConfirmedSolution}, want: Confirmed},
		{name: "reviewed outranks catalog", solutions: []SolutionClass{ConfirmedSolution, ReviewedSolution}, want: Reviewed},
		{name: "one retraction does not override confirmation", solutions: []SolutionClass{RetractedSolution, ConfirmedSolution}, want: Confirmed},
		{name: "only retracted evidence", solutions: []SolutionClass{RetractedSolution}, want: Retracted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ResolveLifecycle(test.solutions); got != test.want {
				t.Fatalf("ResolveLifecycle()=%q, want %q", got, test.want)
			}
		})
	}
}

func TestStrongerSolutionDoesNotResurrectRetractionFromStaleEvidence(t *testing.T) {
	if got := StrongerSolution(RetractedSolution, ConfirmedSolution); got != RetractedSolution {
		t.Fatalf("StrongerSolution()=%q, want retracted", got)
	}
}

func TestCorrelationPolicy(t *testing.T) {
	policy := testCorrelationPolicy()
	now := time.Now().UTC()
	incoming := correlationEvent("emsc", now, 42, 74, 5.2, 12)
	closeID := uuid.New()
	farID := uuid.New()
	decision := policy.Correlate(incoming, []CorrelationCandidate{
		{IncidentID: farID, Event: correlationEvent("usgs", now.Add(-4*time.Minute), 44, 76, 5.2, 12)},
		{IncidentID: closeID, Event: correlationEvent("usgs", now.Add(-2*time.Second), 42.02, 74.01, 5.1, 14)},
	})
	if decision.Match == nil || decision.Match.IncidentID != closeID {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestCorrelationPolicyRejectsAmbiguousCandidates(t *testing.T) {
	policy := testCorrelationPolicy()
	policy.AmbiguityMargin = 0.2
	now := time.Now().UTC()
	incoming := correlationEvent("emsc", now, 42, 74, 5.2, 12)
	decision := policy.Correlate(incoming, []CorrelationCandidate{
		{IncidentID: uuid.New(), Event: correlationEvent("usgs", now.Add(-time.Second), 42.01, 74, 5.2, 12)},
		{IncidentID: uuid.New(), Event: correlationEvent("usgs", now.Add(-2*time.Second), 42.02, 74, 5.2, 12)},
	})
	if !decision.Ambiguous || decision.Match != nil {
		t.Fatalf("decision=%+v", decision)
	}
}

func testCorrelationPolicy() CorrelationPolicy {
	return CorrelationPolicy{
		Version: "test-v1", MaximumTimeDelta: 2 * time.Minute,
		MaximumDistanceKM: 100, MaximumMagnitudeDiff: 1.5, MaximumDepthDiffKM: 100,
		TimeWeight: 0.45, DistanceWeight: 0.35, MagnitudeWeight: 0.15, DepthWeight: 0.05,
		AcceptanceThreshold: 0.75, AmbiguityMargin: 0.1,
	}
}

func correlationEvent(provider string, occurred time.Time, lat, lon, magnitude, depth float64) Event {
	return Event{Provider: provider, OccurredAt: occurred, Latitude: lat, Longitude: lon, Magnitude: &magnitude, DepthKM: &depth}
}
