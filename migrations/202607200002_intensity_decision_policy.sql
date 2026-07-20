ALTER TABLE notification_intensity_evaluations
    ADD COLUMN decision_boundary_mmi DOUBLE PRECISION,
    ADD COLUMN decision_policy_version TEXT;

UPDATE notification_intensity_evaluations
SET decision_boundary_mmi = threshold_mmi,
    decision_policy_version = 'mmi-integer-threshold-one-sigma-v1';

ALTER TABLE notification_intensity_evaluations
    ALTER COLUMN decision_boundary_mmi SET NOT NULL,
    ALTER COLUMN decision_policy_version SET NOT NULL;

ALTER TABLE notification_matching_audits
    ADD COLUMN decision_policy_version TEXT NOT NULL DEFAULT 'mmi-integer-threshold-one-sigma-v1',
    ADD COLUMN candidate_minimum_mmi DOUBLE PRECISION NOT NULL DEFAULT 2.0;

ALTER TABLE notification_matching_audits
    ALTER COLUMN decision_policy_version DROP DEFAULT,
    ALTER COLUMN candidate_minimum_mmi DROP DEFAULT;
