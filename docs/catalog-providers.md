# Additional catalogue providers

## GEOFON

GEOFON ingestion uses the GFZ FDSN Event service at
`https://geofon.gfz.de/fdsnws/event/1/query`. Realtime polling requests the
bounded text representation with a bounded `starttime`/`endtime` window; recovery and backfill use bounded
`starttime`, `endtime`, `limit`, and `offset` pages. Each row becomes a confirmed
`geofon` observation on the `geofon_fdsn` channel. The complete named FDSN row is
retained as its raw payload.

The text representation has no separate revision timestamp. The adapter therefore
uses origin time as `source_updated_at`; equal timestamps are still revision-safe
because ingestion compares the canonical raw payload hash. Event links point to the
official GEOFON event page.

```toml
[providers.geofon]
enabled = true
fdsn_url = "https://geofon.gfz.de/fdsnws/event/1/query"
state_file = "/var/lib/shaker/provider-state/geofon.json"
poll_interval = "1m"
http_timeout = "20s"
lookback = "2h"
max_response_bytes = 26214400
```

## KNDC

KNDC ingestion uses the public JSON request made by the urgent bulletin page at
`https://kndc.kz/kndc/pagecontent/alarm-bulletin/getOriginList.php`. KNDC does not
publish a versioned machine-readable contract for this endpoint, so the integration
is deliberately isolated behind its provider adapter, uses bounded pages and responses,
and has fixture-based contract tests. A schema or availability change fails that
provider run without affecting other catalogue workers.

Observations are confirmed `kndc` records on the `kndc_alarm_bulletin` channel. `mb`
is preferred over `Mpv` when both are present. The `evdate` and `evtime` origin fields
are UTC and authoritative; observed KNDC `epochtime` values are consistently shifted
by six hours and are not used for normalization. The `lddate` update field is
interpreted as UTC+6. The original JSON row is retained unchanged as the raw payload.

```toml
[providers.kndc]
enabled = true
bulletin_url = "https://kndc.kz/kndc/pagecontent/alarm-bulletin"
state_file = "/var/lib/shaker/provider-state/kndc.json"
poll_interval = "5m"
http_timeout = "20s"
max_response_bytes = 4194304
```

## Correlation and rollout

Provider identities remain immutable and distinct. New GEOFON and KNDC records enter
the same bounded, audited cross-provider association policy as EMSC and USGS. Accepted
associations retain score components, candidates, algorithm version, and history;
ambiguous observations remain separate incidents. Within an equal solution class,
the stable canonical preference is KNDC, USGS, GEOFON, then EMSC. This gives the
regional KNDC catalogue precedence for its Central Asia coverage without discarding
any global-provider value.

Their first successful poll is a baseline and does not create a notification burst.
Deploy each one as its own provider-worker and monitor provider
ingestion runs, invalid counts, new-incident association evidence, and Telegram edits.
