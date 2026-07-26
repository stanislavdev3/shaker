package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains application-level metrics shared by the service runtimes. A
// runtime registers the complete set once, but only updates the metrics that it
// owns.
type Metrics struct {
	ProviderPolls           *prometheus.CounterVec
	ProviderPollDuration    *prometheus.HistogramVec
	ProviderEvents          *prometheus.CounterVec
	ProviderLastSuccess     *prometheus.GaugeVec
	CoreObservations        *prometheus.CounterVec
	CoreProcessingDuration  *prometheus.HistogramVec
	CoreOutboxPublishes     *prometheus.CounterVec
	IncidentChanges         *prometheus.CounterVec
	IncidentChangeDuration  prometheus.Histogram
	NotificationDeliveries  *prometheus.CounterVec
	NotificationDuration    *prometheus.HistogramVec
	TelegramAlertDeliveries *prometheus.CounterVec
	HTTPRequests            *prometheus.CounterVec
	HTTPDuration            *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ProviderPolls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shaker_provider_polls_total",
			Help: "Provider polling attempts by mode and result.",
		}, []string{"provider", "mode", "result"}),
		ProviderPollDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "shaker_provider_poll_duration_seconds",
			Help:    "Provider polling duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"provider", "mode"}),
		ProviderEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shaker_provider_events_total",
			Help: "Provider events by mode and processing result.",
		}, []string{"provider", "mode", "result"}),
		ProviderLastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "shaker_provider_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful provider poll.",
		}, []string{"provider"}),
		CoreObservations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shaker_core_observations_total",
			Help: "Provider observations handled by core.",
		}, []string{"provider", "result"}),
		CoreProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "shaker_core_observation_processing_duration_seconds",
			Help:    "Core provider observation processing duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"provider"}),
		CoreOutboxPublishes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shaker_core_outbox_publishes_total",
			Help: "Core outbox publication attempts.",
		}, []string{"result"}),
		IncidentChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shaker_notification_incident_changes_total",
			Help: "Canonical incident changes handled by the notification service.",
		}, []string{"operation", "result"}),
		IncidentChangeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "shaker_notification_incident_change_processing_duration_seconds",
			Help:    "Notification incident change processing duration.",
			Buckets: prometheus.DefBuckets,
		}),
		NotificationDeliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shaker_notification_deliveries_total",
			Help: "Notification delivery attempts by channel and result.",
		}, []string{"channel", "result"}),
		NotificationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "shaker_notification_delivery_duration_seconds",
			Help:    "Notification delivery duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel"}),
		TelegramAlertDeliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shaker_notification_telegram_alert_deliveries_total",
			Help: "Telegram canonical alert projection attempts.",
		}, []string{"operation", "result"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_server_requests_total",
			Help: "HTTP requests.",
		}, []string{"method", "route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_server_request_duration_seconds",
			Help: "HTTP request duration.",
		}, []string{"method", "route"}),
	}
	reg.MustRegister(
		m.ProviderPolls,
		m.ProviderPollDuration,
		m.ProviderEvents,
		m.ProviderLastSuccess,
		m.CoreObservations,
		m.CoreProcessingDuration,
		m.CoreOutboxPublishes,
		m.IncidentChanges,
		m.IncidentChangeDuration,
		m.NotificationDeliveries,
		m.NotificationDuration,
		m.TelegramAlertDeliveries,
		m.HTTPRequests,
		m.HTTPDuration,
	)
	return m
}

func (m *Metrics) ObserveProviderPoll(provider, mode, result string, completedAt time.Time, duration time.Duration,
	fetched, invalid int,
) {
	m.ProviderPolls.WithLabelValues(provider, mode, result).Inc()
	m.ProviderPollDuration.WithLabelValues(provider, mode).Observe(duration.Seconds())
	if fetched > 0 {
		m.ProviderEvents.WithLabelValues(provider, mode, "fetched").Add(float64(fetched))
	}
	if invalid > 0 {
		m.ProviderEvents.WithLabelValues(provider, mode, "invalid").Add(float64(invalid))
	}
	if result == "success" {
		m.ProviderLastSuccess.WithLabelValues(provider).Set(float64(completedAt.Unix()))
	}
}

func (m *Metrics) ObserveProviderPublished(provider, mode string) {
	m.ProviderEvents.WithLabelValues(provider, mode, "published").Inc()
}

func (m *Metrics) ObserveCoreObservation(provider, result string, duration time.Duration) {
	m.CoreObservations.WithLabelValues(provider, result).Inc()
	m.CoreProcessingDuration.WithLabelValues(provider).Observe(duration.Seconds())
}

func (m *Metrics) ObserveCoreOutbox(result string) {
	m.CoreOutboxPublishes.WithLabelValues(result).Inc()
}

func (m *Metrics) ObserveIncidentChange(operation, result string, duration time.Duration) {
	m.IncidentChanges.WithLabelValues(operation, result).Inc()
	m.IncidentChangeDuration.Observe(duration.Seconds())
}

func (m *Metrics) ObserveNotificationDelivery(channel, result string, duration time.Duration) {
	m.NotificationDeliveries.WithLabelValues(channel, result).Inc()
	m.NotificationDuration.WithLabelValues(channel).Observe(duration.Seconds())
}

func (m *Metrics) ObserveTelegramAlert(operation, result string) {
	m.TelegramAlertDeliveries.WithLabelValues(operation, result).Inc()
}
