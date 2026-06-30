CREATE OR REPLACE FUNCTION publish_earthquake_change() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        'earthquake_changes',
        json_build_object(
            'operation', lower(TG_OP),
            'earthquake_id', NEW.id,
            'version', NEW.version
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER earthquakes_publish_change
AFTER INSERT OR UPDATE OF version ON earthquakes
FOR EACH ROW EXECUTE FUNCTION publish_earthquake_change();

CREATE OR REPLACE FUNCTION publish_notification_delivery_change() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' OR OLD.status IS DISTINCT FROM NEW.status THEN
        PERFORM pg_notify(
            'notification_delivery_changes',
            json_build_object(
                'delivery_id', NEW.id,
                'status', NEW.status,
                'trigger_type', NEW.trigger_type
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notification_deliveries_publish_change
AFTER INSERT OR UPDATE OF status ON notification_deliveries
FOR EACH ROW EXECUTE FUNCTION publish_notification_delivery_change();
