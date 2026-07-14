package observability

import "github.com/prometheus/client_golang/prometheus"

// EMSCWebSocketMetrics exposes the current standing-order connection state and
// the number of messages successfully handed to ingestion.
type EMSCWebSocketMetrics struct {
	connected prometheus.Gauge
	messages  prometheus.Counter
}

func NewEMSCWebSocketMetrics(reg prometheus.Registerer) *EMSCWebSocketMetrics {
	m := &EMSCWebSocketMetrics{
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "earthquake_emsc_websocket_up",
			Help: "Whether the EMSC standing-order WebSocket is currently connected.",
		}),
		messages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "earthquake_emsc_websocket_messages_total",
			Help: "EMSC standing-order WebSocket messages successfully handed to ingestion.",
		}),
	}
	reg.MustRegister(m.connected, m.messages)
	m.connected.Set(0)
	return m
}

func (m *EMSCWebSocketMetrics) SetConnected(connected bool) {
	if connected {
		m.connected.Set(1)
		return
	}
	m.connected.Set(0)
}

func (m *EMSCWebSocketMetrics) MessageHandled() {
	m.messages.Inc()
}
