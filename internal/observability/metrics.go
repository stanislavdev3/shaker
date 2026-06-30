package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	IngestionRuns          *prometheus.CounterVec
	IngestionDuration      *prometheus.HistogramVec
	IngestionEvents        *prometheus.CounterVec
	ProviderRequests       *prometheus.CounterVec
	ProviderDuration       *prometheus.HistogramVec
	ProviderLastSuccess    *prometheus.GaugeVec
	NotificationsCreated   *prometheus.CounterVec
	NotificationDeliveries *prometheus.CounterVec
	NotificationDuration   *prometheus.HistogramVec
	QueueSize              *prometheus.GaugeVec
	OldestPending          prometheus.Gauge
	HTTPRequests           *prometheus.CounterVec
	HTTPDuration           *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		IngestionRuns:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "earthquake_ingestion_runs_total", Help: "Ingestion runs."}, []string{"provider", "mode", "status"}),
		IngestionDuration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "earthquake_ingestion_duration_seconds", Help: "Ingestion duration."}, []string{"provider", "mode"}),
		IngestionEvents:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "earthquake_ingestion_events_total", Help: "Ingestion events."}, []string{"provider", "result"}),
		ProviderRequests:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "earthquake_provider_requests_total", Help: "Provider requests."}, []string{"provider", "status"}),
		ProviderDuration:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "earthquake_provider_request_duration_seconds", Help: "Provider request duration."}, []string{"provider"}),
		ProviderLastSuccess:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "earthquake_provider_last_success_timestamp_seconds", Help: "Last provider success."}, []string{"provider"}),
		NotificationsCreated:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "earthquake_notifications_created_total", Help: "Notifications created."}, []string{"trigger_type"}),
		NotificationDeliveries: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "earthquake_notification_deliveries_total", Help: "Notification deliveries."}, []string{"channel", "status"}),
		NotificationDuration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "earthquake_notification_delivery_duration_seconds", Help: "Notification delivery duration."}, []string{"channel"}),
		QueueSize:              prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "earthquake_notification_queue_size", Help: "Notification queue size."}, []string{"status"}),
		OldestPending:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "earthquake_notification_oldest_pending_age_seconds", Help: "Oldest pending age."}),
		HTTPRequests:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_server_requests_total", Help: "HTTP requests."}, []string{"method", "route", "status"}),
		HTTPDuration:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_server_request_duration_seconds", Help: "HTTP duration."}, []string{"method", "route"}),
	}
	reg.MustRegister(m.IngestionRuns, m.IngestionDuration, m.IngestionEvents, m.ProviderRequests, m.ProviderDuration, m.ProviderLastSuccess,
		m.NotificationsCreated, m.NotificationDeliveries, m.NotificationDuration, m.QueueSize, m.OldestPending, m.HTTPRequests, m.HTTPDuration)
	return m
}
