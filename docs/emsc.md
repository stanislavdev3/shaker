# EMSC ingestion

## Runtime flow

EMSC integration is opt-in with `EMSC_ENABLED=true` and uses two official
SeismicPortal interfaces:

- the standing-order WebSocket at
  `wss://www.seismicportal.eu/standing_order/websocket` for low-latency
  `insert`, `update`, and `delete` messages;
- the FDSN Event API at
  `https://www.seismicportal.eu/fdsnws/event/1/query` for catalogue
  confirmation, overlapping recovery, and historical windows.

Both adapters normalize the EMSC `unid` as `(provider=emsc, external_id=unid)`.
This is the stable identity shared by standing-order and FDSN payloads. A
standing-order observation has channel `emsc_standing_order` and solution class
`preliminary`; an FDSN observation has channel `emsc_fdsn` and solution class
`confirmed`. Consequently, an FDSN update changes an existing preliminary
incident and its Telegram projection instead of creating another EMSC incident.

The WebSocket client enforces a frame-size limit, sends periodic pings, retries
database persistence before reconnecting, and reconnects with capped exponential
backoff and jitter. Invalid frames are rejected without stopping the stream. The
FDSN poll uses `updatedafter` with a configurable overlap, bounded response
reads, request timeouts, retry handling, and paginated historical queries. All
provider tests use committed fixtures and local `httptest` servers.

## Ordering and provenance

Every materially distinct payload is retained in `provider_observations` with
its channel, solution class, source timestamp, payload hash, and receive time.
The complete standing-order envelope is retained so the provider action is not
lost.

A newer preliminary WebSocket payload cannot replace parameters already selected
from a confirmed FDSN payload. It remains in the observation audit trail. A
confirmed FDSN payload can promote a preliminary incident even if its provider
timestamp is older, without replacing newer preliminary parameters in that
specific stale-update case. Provider deletion can transition an EMSC-only
incident to `retracted`.

## Configuration

```env
EMSC_ENABLED=true
EMSC_WEBSOCKET_URL=wss://www.seismicportal.eu/standing_order/websocket
EMSC_FDSN_URL=https://www.seismicportal.eu/fdsnws/event/1/query
EMSC_POLL_INTERVAL=30s
EMSC_HTTP_TIMEOUT=20s
EMSC_LOOKBACK_DURATION=2h
EMSC_PING_INTERVAL=15s
EMSC_MAX_RESPONSE_BYTES=26214400
EMSC_MAX_FRAME_BYTES=262144
```

The feature defaults to disabled so a release can be deployed before activating
the new external dependency. Enable it only on worker roles; API roles never
open provider connections.

## Current correlation boundary

EMSC standing-order and EMSC FDSN observations are merged by their authoritative
shared `unid`. Cross-provider EMSC-to-USGS heuristic association remains disabled
until replay fixtures calibrate the policy described in
[`event-correlation.md`](event-correlation.md). Until that work is activated,
enabling both providers can temporarily represent the same physical earthquake
as two canonical incidents. This favors a visible duplicate over an unsafe merge
of nearby earthquakes.

The next correlation increment should use authoritative EventID mappings where
available, then replay-calibrated heuristic matching for the remaining events.
