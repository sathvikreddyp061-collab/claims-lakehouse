-- Vercel-facing mirror of the dbt gold marts. Intentionally narrow.

CREATE SCHEMA IF NOT EXISTS gold;

-- Member-360 surface: one row per active member with key benefit + utilization metrics
CREATE TABLE IF NOT EXISTS gold.member_360 (
    member_id            TEXT PRIMARY KEY,
    full_name_masked     TEXT NOT NULL,        -- SHA-256 hash (HIPAA)
    age_band             TEXT NOT NULL,        -- "0-17", "18-34", "35-54", "55-64", "65+"
    state                CHAR(2) NOT NULL,
    plan_type            TEXT NOT NULL,
    eligibility_status   TEXT NOT NULL,
    pcp_provider_id      TEXT,
    claims_last_90d      INTEGER NOT NULL DEFAULT 0,
    paid_amount_last_90d NUMERIC(12, 2) NOT NULL DEFAULT 0,
    last_claim_dos       DATE,
    risk_band            TEXT,                 -- "low" / "rising" / "high"
    refreshed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_member_360_state ON gold.member_360 (state);
CREATE INDEX IF NOT EXISTS idx_member_360_risk  ON gold.member_360 (risk_band);

-- Daily claims summary feeding the dashboard's "ops" tab
CREATE TABLE IF NOT EXISTS gold.claims_daily (
    dos_date           DATE NOT NULL,
    plan_type          TEXT NOT NULL,
    claims_count       INTEGER NOT NULL,
    paid_amount        NUMERIC(14, 2) NOT NULL,
    avg_paid_per_claim NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (dos_date, plan_type)
);

-- Pipeline freshness — Airflow writes here at the end of every run, dashboard
-- reads it to show the SLA badge.
CREATE TABLE IF NOT EXISTS gold.pipeline_freshness (
    dag_id             TEXT PRIMARY KEY,
    last_success_ts    TIMESTAMPTZ NOT NULL,
    last_duration_s    INTEGER NOT NULL,
    rows_processed     BIGINT NOT NULL
);
