# Telegram bot

The Telegram bot creates earthquake alert subscriptions through a short chat flow:

1. The user sends `/start` and shares a Telegram location.
2. The bot stores that point and asks for the notification language (`ru` or `en`).
3. The user chooses any supported minimum expected Modified Mercalli Intensity from II
   through VI at that point. VI is the highest configurable threshold; estimates above
   VI still appear in alerts and naturally match a VI subscription.
4. The subscription becomes active after all three settings are present.

After a location is received, the location-sharing keyboard is removed. It is shown
again only for first-time registration or an explicit `/location` command. Repeating
`/start` with an existing location reports the current setup without showing the button.

`/status` shows the active threshold, `/location` replaces the configured location,
`/language` changes the notification language, `/intensity` changes the MMI threshold,
and `/stop` disables alerts. Replacing a location pauses the subscription until a
language and intensity are selected, so an incomplete configuration cannot send alerts.

Existing magnitude-and-radius subscriptions remain active as legacy subscriptions until
the user explicitly selects `/intensity`. There is no scientifically valid automatic
mapping from an epicentral magnitude threshold to local MMI. Their language initially
remains unset so `/start` asks them to choose `ru` or `en` without changing the legacy
filter.

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

For each event, the service derives a conservative candidate radius from the event
magnitude, depth, and the lowest supported MMI. PostGIS uses that dynamic radius only
to bound the candidate query. The application then predicts MMI separately at every
candidate subscriber point and notifies when the prediction's one-sigma upper bound
reaches the subscriber threshold. A fixed user-facing radius is not used.

The preliminary model is the hypocentral-distance form of Allen, Wald, and Worden
(2012), versioned as `allen-et-al-2012-rhypo-v1`. It uses a 10 km auditable default when
provider depth is absent and assumes active crust with average site response. Values
outside the model's published magnitude or distance calibration are marked as
extrapolated. This estimate is not an observed ground-motion measurement.

Every candidate decision stores the event version, model name and version, magnitude,
effective depth, epicentral and hypocentral distance, mean MMI, total sigma, bounds,
threshold, assumptions, and decision. A later canonical earthquake version recomputes
the estimate and edits an existing Telegram alert.

Personal Telegram messages include magnitude, expected MMI and verbal severity, likely
one-sigma MMI range, distance, depth, place, event time, and provider links. Text and the
location callback button use the subscriber's `ru` or `en` language. The global channel
has no subscriber location, so it does not display a local intensity estimate.

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

If a preliminary revision later produces a lower local estimate, the already delivered
message is updated with the correction instead of silently disappearing. A later MMI
threshold crossing for an incident that was not previously delivered creates the first
message.

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
