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
	lowRadius := CandidateRadiusKM(&low, nil)
	highRadius := CandidateRadiusKM(&high, nil)
	if lowRadius <= 0 || highRadius <= lowRadius || highRadius == 1000 {
		t.Fatalf("low=%f high=%f", lowRadius, highRadius)
	}
	estimate, err := EstimateAt(&high, nil, highRadius, nil)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(estimate.UpperMMI-MinimumSupportedMMI) > 1e-6 {
		t.Fatalf("upper MMI at boundary=%f", estimate.UpperMMI)
	}
}
