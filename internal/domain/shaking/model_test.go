package shaking

import (
	"math"
	"testing"
)

func TestEstimateAtMatchesPublishedHypocentralEquation(t *testing.T) {
	magnitude, depth := 6.0, 10.0
	estimate, err := EstimateAt(&magnitude, &depth, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantMean, wantSigma := allenEtAl2012Rhypo(magnitude, math.Hypot(50, depth))
	if math.Abs(estimate.MeanMMI-wantMean) > 1e-9 || math.Abs(estimate.SigmaMMI-wantSigma) > 1e-9 {
		t.Fatalf("estimate=%+v want mean=%f sigma=%f", estimate, wantMean, wantSigma)
	}
	if estimate.ModelVersion != ModelVersion || estimate.UpperMMI <= estimate.MeanMMI {
		t.Fatalf("invalid estimate metadata: %+v", estimate)
	}
}

func TestEstimateAtUsesAuditableDefaultDepth(t *testing.T) {
	magnitude := 5.5
	estimate, err := EstimateAt(&magnitude, nil, 20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.DepthKM != 10 || estimate.Assumptions["depth_defaulted"] != true {
		t.Fatalf("estimate=%+v", estimate)
	}
}

func TestCandidateRadiusIsDynamicAndConservative(t *testing.T) {
	low, high := 4.0, 7.0
	decisionBoundary := 1.5
	lowRadius := CandidateRadiusKM(&low, nil, decisionBoundary)
	highRadius := CandidateRadiusKM(&high, nil, decisionBoundary)
	if lowRadius <= 0 || highRadius <= lowRadius || highRadius == 1000 {
		t.Fatalf("low=%f high=%f", lowRadius, highRadius)
	}
	estimate, err := EstimateAt(&high, nil, highRadius, nil)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(estimate.UpperMMI-decisionBoundary) > 1e-6 {
		t.Fatalf("upper MMI at boundary=%f", estimate.UpperMMI)
	}
}

func TestFeltBishkekIncidentFallsInsideCategoryIICandidateRadius(t *testing.T) {
	magnitude, depth := 4.1, 14.057
	const distanceKM = 153.17
	radius := CandidateRadiusKM(&magnitude, &depth, 1.5)
	estimate, err := EstimateAt(&magnitude, &depth, distanceKM, nil)
	if err != nil {
		t.Fatal(err)
	}
	if radius <= distanceKM || estimate.UpperMMI < 1.5 || estimate.UpperMMI >= 2 {
		t.Fatalf("radius=%f estimate=%+v", radius, estimate)
	}
}
