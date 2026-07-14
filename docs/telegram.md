# Telegram bot

The Telegram bot creates earthquake alert subscriptions through a short chat flow:

1. The user sends `/start` and shares a Telegram location.
2. The bot stores that point with a fixed 1,000 km radius and asks for a minimum magnitude.
3. The user sends a number from 0 to 10, which activates the subscription.

After a location is received, the location-sharing keyboard is removed. It is shown
again only for first-time registration or an explicit `/location` command. Repeating
`/start` with an existing location reports the current setup without showing the button.

`/status` shows the active threshold, `/location` replaces the configured location,
and `/stop` disables alerts. Replacing a location pauses the subscription until a
new minimum magnitude is supplied, so an incomplete configuration cannot send alerts.

## Runtime

Set `TELEGRAM_BOT_TOKEN` to enable both inbound bot polling and Telegram delivery in
the `worker` role. Obtain the token from BotFather and keep it out of logs and source
control. `TELEGRAM_API_URL` exists for tests and compatible gateways; production should
normally use the default `https://api.telegram.org`.

Set `TELEGRAM_GLOBAL_CHANNEL` to a public channel username such as `@eqmonitor` to
publish a worldwide feed. On startup the worker resolves the username to a stable
numeric chat ID, verifies that the bot is a channel administrator with post and edit
permissions, and idempotently creates an unfiltered system subscription. A configured
channel that cannot be resolved or has insufficient permissions prevents the worker
from starting.

The bot uses bounded, context-aware `getUpdates` long polling. The last processed
update offset is stored in `provider_state`. A PostgreSQL advisory lock elects one bot
poller when multiple worker replicas are running. Notification delivery itself remains
distributed through the normal PostgreSQL queue.

Telegram chat IDs and precise user coordinates are stored in
`notification_subscriptions`. They are operationally sensitive personal data and must
be protected through the same database access and backup controls as other subscription
data.

## Matching and delivery

PostGIS applies the fixed 1,000 km radius and minimum magnitude filter during ingestion.
The exact distance from the configured point to the epicenter is added to the durable
delivery payload. Telegram messages include magnitude, distance, depth, place, event
time, and the provider details URL when those values are available.

An alert represents a canonical incident rather than a provider record. Its heading
shows the current lifecycle state:

- `🟡 Preliminary — details are being refined` for an EMSC WebSocket-only incident.
- `✅ Confirmed earthquake` after an EMSC FDSN or USGS catalogue observation is
  associated with the incident.
- `🔵 Reviewed earthquake` when an associated provider explicitly reports a reviewed
  solution.
- `❌ Retracted event` when the incident is determined not to be an earthquake.

The first matching preliminary incident creates one Telegram message. The service
stores the returned Telegram `message_id`; material changes and lifecycle transitions
replace the text of that message with `editMessageText`. Rapid provider revisions are
coalesced to the newest desired incident version. If the original message can no longer
be edited, the service sends one linked correction and stores its new `message_id`.

If a preliminary magnitude later falls below the subscription threshold or its
epicenter moves outside the configured radius, the already delivered message is
updated with the correction instead of silently disappearing. A later threshold
crossing for an incident that was not previously delivered creates the first message.

The existing baseline, backfill, recovery, retry, and at-least-once rules also apply to
Telegram. A material canonical revision that crosses the configured minimum magnitude
can create a threshold-crossing alert.

## Global channel

The global channel subscription has no magnitude, point, or radius filter. It accepts
normalized earthquake events worldwide while excluding non-earthquake provider event
types. Baseline and backfill do not publish historical posts. Recovery may update a
post that was previously created from a preliminary realtime observation.

The global channel uses the same canonical incident and Telegram message projection as
private alerts. The first matching version calls `sendMessage` and stores its returned
`message_id`; later material or lifecycle versions call `editMessageText`. Personal
Telegram projections are claimed before global channel projections so a global burst
does not take priority over location-based alerts. Telegram 429 responses use the
normal durable retry policy.
