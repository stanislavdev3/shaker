-- atlas:txmode none
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE earthquakes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    preferred_source TEXT NOT NULL,
    preferred_external_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    depth_km DOUBLE PRECISION CHECK (depth_km IS NULL OR depth_km <> 'NaN'::float8),
    location GEOGRAPHY(POINT, 4326) NOT NULL,
    magnitude DOUBLE PRECISION CHECK (magnitude IS NULL OR magnitude <> 'NaN'::float8),
    magnitude_type TEXT, place TEXT, title TEXT, status TEXT, event_type TEXT, alert_level TEXT,
    tsunami BOOLEAN, significance INTEGER, felt_reports INTEGER, cdi DOUBLE PRECISION,
    mmi DOUBLE PRECISION, station_count INTEGER, azimuthal_gap DOUBLE PRECISION,
    minimum_distance DOUBLE PRECISION, rms DOUBLE PRECISION, source_url TEXT, detail_url TEXT,
    version BIGINT NOT NULL CHECK (version >= 1),
    first_seen_at TIMESTAMPTZ NOT NULL, last_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (preferred_source, preferred_external_id)
);
CREATE INDEX earthquakes_occurred_at_idx ON earthquakes (occurred_at DESC, id);
CREATE INDEX earthquakes_magnitude_idx ON earthquakes (magnitude);
CREATE INDEX earthquakes_source_updated_at_idx ON earthquakes (source_updated_at);
CREATE INDEX earthquakes_source_identity_idx ON earthquakes (preferred_source, preferred_external_id);
CREATE INDEX earthquakes_location_idx ON earthquakes USING GIST (location);
CREATE INDEX earthquakes_filters_idx ON earthquakes (event_type, status, alert_level, tsunami);

CREATE TABLE earthquake_source_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id),
    provider TEXT NOT NULL, external_id TEXT NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL, payload_hash BYTEA NOT NULL, raw_payload JSONB NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 1),
    first_seen_at TIMESTAMPTZ NOT NULL, last_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, external_id), UNIQUE (earthquake_id, provider, external_id)
);

CREATE TABLE earthquake_revisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id),
    source_record_id UUID NOT NULL REFERENCES earthquake_source_records(id),
    version BIGINT NOT NULL, source_updated_at TIMESTAMPTZ NOT NULL,
    changed_fields JSONB NOT NULL, raw_payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source_record_id, version)
);

CREATE TABLE ingestion_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), provider TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('baseline','realtime','backfill','recovery')),
    started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running','succeeded','failed')),
    fetched_count INTEGER NOT NULL DEFAULT 0, inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0, unchanged_count INTEGER NOT NULL DEFAULT 0,
    invalid_count INTEGER NOT NULL DEFAULT 0, error_message TEXT, metadata JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE provider_state (
    provider TEXT NOT NULL, state_key TEXT NOT NULL, state_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (provider, state_key)
);

CREATE TABLE notification_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','paused','disabled')),
    channel TEXT NOT NULL CHECK (channel = 'webhook'), webhook_url TEXT NOT NULL,
    encrypted_webhook_secret BYTEA NOT NULL,
    minimum_magnitude DOUBLE PRECISION, maximum_magnitude DOUBLE PRECISION,
    center_latitude DOUBLE PRECISION, center_longitude DOUBLE PRECISION, radius_km DOUBLE PRECISION,
    area GEOGRAPHY(POINT,4326), tsunami_only BOOLEAN NOT NULL DEFAULT FALSE,
    allowed_alert_levels TEXT[], allowed_event_types TEXT[],
    notify_on_new BOOLEAN NOT NULL DEFAULT TRUE,
    notify_on_threshold_crossing BOOLEAN NOT NULL DEFAULT TRUE,
    notify_on_tsunami_change BOOLEAN NOT NULL DEFAULT TRUE,
    notify_on_alert_increase BOOLEAN NOT NULL DEFAULT TRUE,
    maximum_event_age INTERVAL NOT NULL DEFAULT '2 hours',
    created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    CHECK ((center_latitude IS NULL AND center_longitude IS NULL AND radius_km IS NULL) OR
           (center_latitude BETWEEN -90 AND 90 AND center_longitude BETWEEN -180 AND 180 AND radius_km > 0)),
    CHECK (minimum_magnitude IS NULL OR maximum_magnitude IS NULL OR minimum_magnitude <= maximum_magnitude)
);

CREATE TABLE notification_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES notification_subscriptions(id),
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id), earthquake_version BIGINT NOT NULL,
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('new_event','magnitude_threshold_crossed','tsunami_activated','alert_level_increased')),
    status TEXT NOT NULL CHECK (status IN ('pending','processing','retry','sent','dead')),
    attempt_count INTEGER NOT NULL DEFAULT 0, next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ, locked_by TEXT, sent_at TIMESTAMPTZ,
    response_status INTEGER, last_error TEXT, payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (subscription_id, earthquake_id, earthquake_version, trigger_type)
);
CREATE INDEX notification_deliveries_claim_idx ON notification_deliveries(status, next_attempt_at);
