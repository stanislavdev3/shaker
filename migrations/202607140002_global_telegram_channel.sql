ALTER TABLE notification_subscriptions
    ADD COLUMN subscription_kind TEXT NOT NULL DEFAULT 'user'
        CHECK (subscription_kind IN ('user', 'global_channel')),
    ADD COLUMN telegram_chat_username TEXT,
    ADD CONSTRAINT notification_subscriptions_global_channel_configuration_check
        CHECK (
            subscription_kind <> 'global_channel'
            OR (
                channel = 'telegram'
                AND telegram_chat_id IS NOT NULL
                AND center_latitude IS NULL
                AND center_longitude IS NULL
                AND radius_km IS NULL
                AND area IS NULL
                AND minimum_magnitude IS NULL
                AND maximum_magnitude IS NULL
            )
        );

CREATE UNIQUE INDEX notification_subscriptions_active_global_channel_idx
    ON notification_subscriptions (subscription_kind)
    WHERE subscription_kind = 'global_channel' AND status <> 'disabled';
