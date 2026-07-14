package shaking

import (
	"errors"
	"math"
	"strings"
)

const (
	ModelName           = "Allen-Wald-Worden hypocentral IPE"
	ModelVersion        = "allen-et-al-2012-rhypo-v1"
	MinimumSupportedMMI = 2.0
	maximumSearchKM     = 20000.0
)

var ErrMissingMagnitude = errors.New("magnitude is required to estimate shaking intensity")

// Estimate is a point prediction in Modified Mercalli Intensity units. The
// bounds represent one total standard deviation around the model mean.
type Estimate struct {
	ModelName             string         `json:"model_name"`
	ModelVersion          string         `json:"model_version"`
	MeanMMI               float64        `json:"mean_mmi"`
	SigmaMMI              float64        `json:"sigma_mmi"`
	LowerMMI              float64        `json:"lower_mmi"`
	UpperMMI              float64        `json:"upper_mmi"`
	EpicentralDistanceKM  float64        `json:"epicentral_distance_km"`
	HypocentralDistanceKM float64        `json:"hypocentral_distance_km"`
	Magnitude             float64        `json:"magnitude"`
	DepthKM               float64        `json:"depth_km"`
	Assumptions           map[string]any `json:"assumptions"`
}

// EstimateAt applies the globally calibrated active-crust hypocentral-distance
// IPE from Allen, Wald, and Worden (2012). It is intended as a rapid preliminary
// estimate until an observed ShakeMap becomes available.
func EstimateAt(magnitude *float64, depthKM *float64, epicentralDistanceKM float64, magnitudeType *string) (Estimate, error) {
	if magnitude == nil || math.IsNaN(*magnitude) || math.IsInf(*magnitude, 0) {
		return Estimate{}, ErrMissingMagnitude
	}
	depth := 10.0
	depthDefaulted := true
	if depthKM != nil && !math.IsNaN(*depthKM) && !math.IsInf(*depthKM, 0) {
		depth = math.Max(0, *depthKM)
		depthDefaulted = false
	}
	epicentralDistanceKM = math.Max(0, epicentralDistanceKM)
	hypocentral := math.Hypot(epicentralDistanceKM, depth)
	mean, sigma := allenEtAl2012Rhypo(*magnitude, hypocentral)

	magnitudeIsMoment := magnitudeType != nil && strings.EqualFold(strings.TrimSpace(*magnitudeType), "mw")
	assumptions := map[string]any{
		"active_crust":          true,
		"average_site_response": true,
		"depth_defaulted":       depthDefaulted,
		"magnitude_is_mw":       magnitudeIsMoment,
		"one_sigma_bounds":      true,
		"extrapolated":          *magnitude < 5 || *magnitude > 7.9 || hypocentral > 300 || !magnitudeIsMoment,
	}
	if magnitudeType != nil && *magnitudeType != "" {
		assumptions["input_magnitude_type"] = *magnitudeType
	}
	return Estimate{
		ModelName: ModelName, ModelVersion: ModelVersion,
		MeanMMI: clampMMI(mean), SigmaMMI: sigma,
		LowerMMI: clampMMI(mean - sigma), UpperMMI: clampMMI(mean + sigma),
		EpicentralDistanceKM: epicentralDistanceKM, HypocentralDistanceKM: hypocentral,
		Magnitude: *magnitude, DepthKM: depth, Assumptions: assumptions,
	}, nil
}

// CandidateRadiusKM returns a conservative, magnitude-dependent PostGIS search
// radius. Exact intensity and the subscriber threshold are evaluated afterwards.
func CandidateRadiusKM(magnitude *float64, depthKM *float64) float64 {
	if magnitude == nil || math.IsNaN(*magnitude) || math.IsInf(*magnitude, 0) {
		return 0
	}
	depth := 10.0
	if depthKM != nil && !math.IsNaN(*depthKM) && !math.IsInf(*depthKM, 0) {
		depth = math.Max(0, *depthKM)
	}
	upperAt := func(epicentral float64) float64 {
		mean, sigma := allenEtAl2012Rhypo(*magnitude, math.Hypot(epicentral, depth))
		return mean + sigma
	}
	if upperAt(0) < MinimumSupportedMMI {
		return 0
	}
	if upperAt(maximumSearchKM) >= MinimumSupportedMMI {
		return maximumSearchKM
	}
	low, high := 0.0, maximumSearchKM
	for range 64 {
		mid := (low + high) / 2
		if upperAt(mid) >= MinimumSupportedMMI {
			low = mid
		} else {
			high = mid
		}
	}
	return high
}

func SurfaceDistanceKM(latitudeA, longitudeA, latitudeB, longitudeB float64) float64 {
	const earthRadiusKM = 6371.0088
	toRadians := math.Pi / 180
	latA, latB := latitudeA*toRadians, latitudeB*toRadians
	dLat := (latitudeB - latitudeA) * toRadians
	dLon := (longitudeB - longitudeA) * toRadians
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(latA)*math.Cos(latB)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKM * math.Asin(math.Min(1, math.Sqrt(a)))
}

func allenEtAl2012Rhypo(magnitude, hypocentralDistanceKM float64) (float64, float64) {
	const (
		c0 = 2.085
		c1 = 1.428
		c2 = -1.402
		c4 = 0.078
		m1 = -0.209
		m2 = 2.042
		s1 = 0.82
		s2 = 0.37
		s3 = 22.9
	)
	rm := m1 + m2*math.Exp(magnitude-5)
	distance := math.Max(hypocentralDistanceKM, 0.001)
	mean := c0 + c1*magnitude + c2*math.Log(math.Sqrt(distance*distance+rm*rm))
	if distance > 50 {
		mean += c4 * math.Log(distance/50)
	}
	scaledDistance := distance / s3
	sigma := s1 + s2/(1+scaledDistance*scaledDistance)
	return mean, sigma
}

func clampMMI(value float64) float64 { return math.Max(1, math.Min(10, value)) }
