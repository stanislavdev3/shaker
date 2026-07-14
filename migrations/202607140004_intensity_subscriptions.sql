ALTER TABLE notification_subscriptions
    ADD COLUMN minimum_intensity DOUBLE PRECISION
        CHECK (minimum_intensity IS NULL OR minimum_intensity BETWEEN 2 AND 6),
    ADD COLUMN notification_language TEXT
        CHECK (notification_language IS NULL OR notification_language IN ('en', 'ru'));

UPDATE notification_subscriptions
SET notification_language = 'en'
WHERE subscription_kind = 'global_channel';

ALTER TABLE notification_subscriptions
    DROP CONSTRAINT notification_subscriptions_check,
    ADD CONSTRAINT notification_subscriptions_geography_check
        CHECK (
            (center_latitude IS NULL AND center_longitude IS NULL AND radius_km IS NULL)
            OR
            (
                center_latitude BETWEEN -90 AND 90
                AND center_longitude BETWEEN -180 AND 180
                AND (radius_km IS NULL OR radius_km > 0)
            )
        );

CREATE TABLE notification_intensity_evaluations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES notification_subscriptions(id),
    earthquake_id UUID NOT NULL REFERENCES earthquakes(id),
    earthquake_version BIGINT NOT NULL CHECK (earthquake_version >= 1),
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    mean_mmi DOUBLE PRECISION NOT NULL,
    sigma_mmi DOUBLE PRECISION NOT NULL CHECK (sigma_mmi >= 0),
    lower_mmi DOUBLE PRECISION NOT NULL,
    upper_mmi DOUBLE PRECISION NOT NULL,
    threshold_mmi DOUBLE PRECISION NOT NULL,
    epicentral_distance_km DOUBLE PRECISION NOT NULL CHECK (epicentral_distance_km >= 0),
    hypocentral_distance_km DOUBLE PRECISION NOT NULL CHECK (hypocentral_distance_km >= 0),
    magnitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('notify', 'below_threshold', 'refresh')),
    assumptions JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (subscription_id, earthquake_id, earthquake_version, model_version)
);

CREATE INDEX notification_intensity_evaluations_earthquake_idx
    ON notification_intensity_evaluations (earthquake_id, earthquake_version);
