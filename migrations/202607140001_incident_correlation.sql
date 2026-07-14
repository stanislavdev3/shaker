ALTER TABLE earthquakes
    ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'confirmed'
        CHECK (lifecycle IN ('preliminary', 'confirmed', 'reviewed', 'retracted')),
    ADD COLUMN canonical_provenance JSONB NOT NULL DEFAULT '{}';

ALTER TABLE earthquake_source_records
    ADD COLUMN latest_observation_channel TEXT NOT NULL DEFAULT 'legacy',
    ADD COLUMN solution_class TEXT NOT NULL DEFAULT 'confirmed'
        CHECK (solution_class IN ('preliminary', 'confirmed', 'reviewed', 'retracted'));

CREATE TABLE provider_observations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_record_id UUID NOT NULL REFERENCES earthquake_source_records(id),
    source_version BIGINT NOT NULL CHECK (source_version >= 1),
    channel TEXT NOT NULL,
    solution_class TEXT NOT NULL
        CHECK (solution_class IN ('preliminary', 'confirmed', 'reviewed', 'retracted')),
    source_updated_at TIMESTAMPTZ NOT NULL,
    payload_hash BYTEA NOT NULL,
    raw_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source_record_id, channel, solution_class, source_updated_at, payload_hash)
);

CREATE INDEX provider_observations_source_version_idx
    ON provider_observations (source_record_id, source_version DESC);
CREATE INDEX provider_observations_received_at_idx
    ON provider_observations (received_at DESC);
CREATE INDEX provider_observations_channel_idx
    ON provider_observations (channel, received_at DESC);

INSERT INTO provider_observations (
    source_record_id, source_version, channel, solution_class, source_updated_at,
    payload_hash, raw_payload, received_at
)
SELECT id, version, 'legacy', 'confirmed', source_updated_at, payload_hash,
       raw_payload, last_seen_at
FROM earthquake_source_records;

CREATE TABLE earthquake_source_associations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_record_id UUID NOT NULL REFERENCES earthquake_source_records(id),
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id),
    method TEXT NOT NULL CHECK (
        method IN ('new_incident', 'provider_identity', 'authoritative_mapping',
                   'heuristic', 'manual', 'legacy')
    ),
    confidence DOUBLE PRECISION CHECK (
        confidence IS NULL OR (confidence >= 0 AND confidence <= 1)
    ),
    algorithm_version TEXT,
    evidence JSONB NOT NULL DEFAULT '{}',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    associated_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    CHECK ((active AND ended_at IS NULL) OR (NOT active AND ended_at IS NOT NULL))
);

CREATE UNIQUE INDEX earthquake_source_associations_active_source_idx
    ON earthquake_source_associations (source_record_id)
    WHERE active;
CREATE INDEX earthquake_source_associations_earthquake_idx
    ON earthquake_source_associations (earthquake_id)
    WHERE active;

INSERT INTO earthquake_source_associations (
    source_record_id, earthquake_id, method, confidence, algorithm_version,
    evidence, active, associated_at
)
SELECT id, earthquake_id, 'legacy', 1, 'legacy-v1',
       jsonb_build_object('migration', '202607140001'), TRUE, created_at
FROM earthquake_source_records;

CREATE TABLE telegram_alert_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES notification_subscriptions(id),
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id),
    telegram_chat_id BIGINT NOT NULL,
    telegram_message_id BIGINT,
    desired_earthquake_version BIGINT NOT NULL CHECK (desired_earthquake_version >= 1),
    delivered_earthquake_version BIGINT CHECK (delivered_earthquake_version >= 1),
    desired_payload JSONB NOT NULL,
    lifecycle TEXT NOT NULL
        CHECK (lifecycle IN ('preliminary', 'confirmed', 'reviewed', 'retracted')),
    status TEXT NOT NULL CHECK (
        status IN ('pending_send', 'pending_edit', 'processing', 'active', 'retry', 'dead')
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (subscription_id, earthquake_id)
);

CREATE UNIQUE INDEX telegram_alert_messages_remote_message_idx
    ON telegram_alert_messages (telegram_chat_id, telegram_message_id)
    WHERE telegram_message_id IS NOT NULL;
CREATE INDEX telegram_alert_messages_claim_idx
    ON telegram_alert_messages (status, next_attempt_at);
