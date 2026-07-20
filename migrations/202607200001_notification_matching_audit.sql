CREATE TABLE notification_matching_audits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id),
    earthquake_version BIGINT NOT NULL CHECK (earthquake_version >= 1),
    mode TEXT NOT NULL,
    baseline_complete BOOLEAN NOT NULL,
    model_version TEXT NOT NULL,
    candidate_radius_km DOUBLE PRECISION NOT NULL CHECK (candidate_radius_km >= 0),
    selected_subscription_count INTEGER NOT NULL CHECK (selected_subscription_count >= 0),
    intensity_candidate_count INTEGER NOT NULL CHECK (intensity_candidate_count >= 0),
    intensity_evaluation_count INTEGER NOT NULL CHECK (intensity_evaluation_count >= 0),
    notify_decision_count INTEGER NOT NULL CHECK (notify_decision_count >= 0),
    below_threshold_count INTEGER NOT NULL CHECK (below_threshold_count >= 0),
    estimate_error_count INTEGER NOT NULL CHECK (estimate_error_count >= 0),
    trigger_count INTEGER NOT NULL CHECK (trigger_count >= 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX notification_matching_audits_earthquake_idx
    ON notification_matching_audits (earthquake_id, earthquake_version, created_at DESC);

CREATE OR REPLACE FUNCTION reject_notification_matching_audit_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'notification_matching_audits is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notification_matching_audits_reject_update
BEFORE UPDATE ON notification_matching_audits
FOR EACH ROW EXECUTE FUNCTION reject_notification_matching_audit_mutation();

CREATE TRIGGER notification_matching_audits_reject_delete
BEFORE DELETE ON notification_matching_audits
FOR EACH ROW EXECUTE FUNCTION reject_notification_matching_audit_mutation();
