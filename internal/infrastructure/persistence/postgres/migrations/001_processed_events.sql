CREATE TABLE IF NOT EXISTS processed_events (
    id UUID PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_tenant_processed_at
    ON processed_events (tenant_id, processed_at DESC);
