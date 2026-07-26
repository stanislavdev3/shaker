CREATE TABLE core_message_inbox (
    message_id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    message_offset BIGINT NOT NULL CHECK (message_offset >= 0),
    schema_name TEXT NOT NULL,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (topic, partition, message_offset)
);

CREATE INDEX core_message_inbox_provider_idx
    ON core_message_inbox (provider, external_id, processed_at DESC);

CREATE TABLE notification_message_inbox (
    message_id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    message_offset BIGINT NOT NULL CHECK (message_offset >= 0),
    schema_name TEXT NOT NULL,
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id),
    earthquake_version BIGINT NOT NULL CHECK (earthquake_version > 0),
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (topic, partition, message_offset)
);

CREATE INDEX notification_message_inbox_earthquake_idx
    ON notification_message_inbox (earthquake_id, earthquake_version DESC);

CREATE TABLE core_outbox_messages (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX core_outbox_messages_claim_idx
    ON core_outbox_messages (next_attempt_at, created_at)
    WHERE published_at IS NULL;

CREATE OR REPLACE FUNCTION reject_core_message_inbox_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'core_message_inbox is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER core_message_inbox_reject_update
BEFORE UPDATE ON core_message_inbox
FOR EACH ROW EXECUTE FUNCTION reject_core_message_inbox_mutation();

CREATE TRIGGER core_message_inbox_reject_delete
BEFORE DELETE ON core_message_inbox
FOR EACH ROW EXECUTE FUNCTION reject_core_message_inbox_mutation();

CREATE OR REPLACE FUNCTION reject_notification_message_inbox_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'notification_message_inbox is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notification_message_inbox_reject_update
BEFORE UPDATE ON notification_message_inbox
FOR EACH ROW EXECUTE FUNCTION reject_notification_message_inbox_mutation();

CREATE TRIGGER notification_message_inbox_reject_delete
BEFORE DELETE ON notification_message_inbox
FOR EACH ROW EXECUTE FUNCTION reject_notification_message_inbox_mutation();

CREATE OR REPLACE FUNCTION protect_core_outbox_message() RETURNS trigger AS $$
BEGIN
    IF OLD.id IS DISTINCT FROM NEW.id
       OR OLD.topic IS DISTINCT FROM NEW.topic
       OR OLD.message_key IS DISTINCT FROM NEW.message_key
       OR OLD.schema_name IS DISTINCT FROM NEW.schema_name
       OR OLD.payload IS DISTINCT FROM NEW.payload
       OR OLD.headers IS DISTINCT FROM NEW.headers
       OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
        RAISE EXCEPTION 'core outbox message content is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER core_outbox_messages_protect_content
BEFORE UPDATE ON core_outbox_messages
FOR EACH ROW EXECUTE FUNCTION protect_core_outbox_message();
