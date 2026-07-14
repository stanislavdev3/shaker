CREATE TABLE admin_role_bindings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('viewer', 'operator', 'owner')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (email = lower(btrim(email)) AND email <> ''),
    UNIQUE (email)
);

CREATE INDEX admin_role_bindings_active_email_idx
    ON admin_role_bindings (email)
    WHERE active;

CREATE TABLE admin_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_subject TEXT NOT NULL,
    actor_email TEXT NOT NULL,
    actor_role TEXT NOT NULL CHECK (actor_role IN ('viewer', 'operator', 'owner')),
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    before_state JSONB,
    after_state JSONB,
    reason TEXT,
    request_id TEXT,
    source_ip INET,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX admin_audit_log_created_at_idx
    ON admin_audit_log (created_at DESC, id);
CREATE INDEX admin_audit_log_resource_idx
    ON admin_audit_log (resource_type, resource_id, created_at DESC);
CREATE INDEX admin_audit_log_actor_idx
    ON admin_audit_log (actor_email, created_at DESC);

CREATE OR REPLACE FUNCTION reject_admin_audit_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'admin_audit_log is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER admin_audit_log_reject_update
BEFORE UPDATE ON admin_audit_log
FOR EACH ROW EXECUTE FUNCTION reject_admin_audit_mutation();

CREATE TRIGGER admin_audit_log_reject_delete
BEFORE DELETE ON admin_audit_log
FOR EACH ROW EXECUTE FUNCTION reject_admin_audit_mutation();
