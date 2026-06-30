# Notifications

## Trigger and matching rules

`new_event` applies only to an event first observed by normal realtime polling after baseline completion and within the subscription's maximum event age. Baseline, backfill, and recovery never create this trigger.

`magnitude_threshold_crossed` applies when the previous magnitude was absent or below the subscription minimum and the new magnitude reaches or exceeds it. Remaining above the threshold does not retrigger.

`tsunami_activated` applies only to a transition from false or absent to true. `alert_level_increased` uses `none < green < yellow < orange < red`.

All configured magnitude, tsunami, alert, event-type, and geographic filters must match. Geographic matching uses PostGIS distance.

## Delivery and retries

Ingestion inserts deliveries in the same transaction as the earthquake version. Workers claim bounded batches with `FOR UPDATE SKIP LOCKED`, commit, perform network I/O, and persist results in a second short operation. Any 2xx response succeeds. Failures use exponential backoff with jitter, capped at one hour, and become `dead` after ten attempts by default. Processing locks older than five minutes are reclaimable.

Delivery is at least once. A receiver must persist and deduplicate `X-Earthquake-Delivery-ID`; duplicate HTTP requests are possible around failures. The database uniqueness key prevents duplicate jobs for one subscription, earthquake version, and trigger.

## Signature verification

The request includes a Unix timestamp and `sha256=<hex>` signature. Calculate HMAC-SHA256 over:

```text
timestamp + "." + raw_request_body
```

Use the webhook secret and compare signatures in constant time. Reject stale timestamps according to the receiver's replay policy. The server stores secrets encrypted with AES-GCM and never returns a stored secret after creation.

## SSRF protections

Production webhook URLs require HTTPS and cannot contain credentials. Every attempt resolves DNS and rejects loopback, private, link-local, multicast, unspecified, and metadata-service addresses. The transport dials only an IP from the validated resolution set, preventing DNS rebinding between validation and connection. Redirects are disabled; connection time, total request time, and response bytes are bounded.

Development can explicitly allow private addresses for local webhook testing. That option is rejected in non-development environments.
