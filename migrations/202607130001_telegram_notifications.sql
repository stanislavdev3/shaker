ALTER TABLE notification_subscriptions
    DROP CONSTRAINT notification_subscriptions_channel_check,
    ALTER COLUMN webhook_url DROP NOT NULL,
    ALTER COLUMN encrypted_webhook_secret DROP NOT NULL,
    ADD COLUMN telegram_chat_id BIGINT,
    ADD CONSTRAINT notification_subscriptions_channel_check
        CHECK (channel IN ('webhook', 'telegram')),
    ADD CONSTRAINT notification_subscriptions_channel_configuration_check
        CHECK (
            (channel = 'webhook' AND webhook_url IS NOT NULL AND encrypted_webhook_secret IS NOT NULL AND telegram_chat_id IS NULL)
            OR
            (channel = 'telegram' AND webhook_url IS NULL AND encrypted_webhook_secret IS NULL AND telegram_chat_id IS NOT NULL)
        );

CREATE UNIQUE INDEX notification_subscriptions_telegram_chat_idx
    ON notification_subscriptions (telegram_chat_id)
    WHERE channel = 'telegram';
