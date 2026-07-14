package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestEMSCWebSocketMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewEMSCWebSocketMetrics(registry)

	if got := metricValue(t, registry, "earthquake_emsc_websocket_up"); got != 0 {
		t.Fatalf("initial connected gauge=%v", got)
	}
	metrics.SetConnected(true)
	metrics.MessageHandled()
	metrics.MessageHandled()
	if got := metricValue(t, registry, "earthquake_emsc_websocket_up"); got != 1 {
		t.Fatalf("connected gauge=%v", got)
	}
	if got := metricValue(t, registry, "earthquake_emsc_websocket_messages_total"); got != 2 {
		t.Fatalf("message counter=%v", got)
	}
	metrics.SetConnected(false)
	if got := metricValue(t, registry, "earthquake_emsc_websocket_up"); got != 0 {
		t.Fatalf("disconnected gauge=%v", got)
	}
}

func metricValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name || len(family.Metric) != 1 {
			continue
		}
		if family.Metric[0].Gauge != nil {
			return family.Metric[0].Gauge.GetValue()
		}
		if family.Metric[0].Counter != nil {
			return family.Metric[0].Counter.GetValue()
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}
