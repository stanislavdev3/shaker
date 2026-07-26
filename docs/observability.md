# Observability

Each runtime exposes Prometheus metrics on its configured `metrics_address`. Kubernetes
adds the `component`, `provider`, and `pod` target labels while discovering annotated
pods. Application metrics use bounded labels only; incident IDs, provider external IDs,
URLs, and error messages must never become metric labels.

The provisioned Grafana dashboards are maintained as code in
`deploy/monitoring/grafana`:

- **Shaker Overview** shows service availability, provider freshness, end-to-end
  throughput, errors, Kafka lag, API latency, and runtime memory.
- **Shaker Providers** compares provider availability, freshness, poll reliability,
  event outcomes, latency, and the EMSC WebSocket connection.
- **Shaker Pipeline** follows observations through core correlation and the
  transactional outbox, including consumer lag and inbox duplicates.
- **Shaker Notifications** covers incident-change consumption, delivery results,
  retries, dead deliveries, latency, and Telegram alert projection.
- **Shaker API** contains HTTP availability, traffic, error ratio, route latency, and
  runtime resource panels.
- **Shaker Kafka** uses `kafka_exporter` metrics for broker discovery, topic offsets,
  partitions, and consumer-group lag.

The legacy combined worker dashboard is intentionally removed because provider, core,
and notification processing no longer share a runtime. Its Loki panels also depended on
obsolete Docker Compose labels. Kubernetes log panels should be added only after the log
collector assigns stable `namespace`, `component`, `provider`, and `pod` labels; until
then the dashboards contain only queries backed by deployed Prometheus collectors.

The Kafka dashboard requires `kafka_exporter` to connect to the broker and expose port
9308 to Prometheus. It provides topic and consumer-group visibility, but not broker JVM
or request-handler internals. Add the Prometheus JMX exporter before introducing panels
or alerts for Kafka JVM memory, garbage collection, request latency, or controller
metrics.
