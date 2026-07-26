# EMSC ingestion

## Runtime flow

EMSC integration is enabled with `providers.emsc.enabled = true` and uses two official
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

```toml
[providers.emsc]
enabled = true
websocket_url = "wss://www.seismicportal.eu/standing_order/websocket"
fdsn_url = "https://www.seismicportal.eu/fdsnws/event/1/query"
state_file = "/var/lib/shaker/provider-state/emsc.json"
poll_interval = "30s"
http_timeout = "20s"
lookback = "2h"
ping_interval = "15s"
max_response_bytes = 26214400
max_frame_bytes = 262144
```

The feature defaults to disabled so a release can be deployed before activating
the new external dependency. Enable it only on the EMSC provider-worker; API roles never
open provider connections.

## Current correlation boundary

EMSC standing-order and EMSC FDSN observations are merged by their authoritative shared
`unid`. Previously unseen EMSC and USGS identities are also eligible for automatic
cross-provider association under the conservative, versioned
`multi-catalog-conservative-v2` policy described in
[`event-correlation.md`](event-correlation.md). Ambiguous and below-threshold candidates
remain separate incidents, with their ranked decision evidence stored on the
`new_incident` association. Accepted links store the score, component deltas, complete
ranked candidates, gates, and algorithm version on the immutable association history.

Authoritative EventID mappings remain preferable when available and should be added as
a higher-priority correlation mechanism in a future increment.
