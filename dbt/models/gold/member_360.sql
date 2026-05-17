-- Gold: one row per member with 90-day utilization + dollar exposure.
-- This is the surface the Vercel member-360 dashboard reads from.

with claims as (
    select * from {{ ref('claims_enriched') }}
),

cohort as (
    select
        subscriber_id                                       as member_id,
        count(*)                                            as claims_lifetime,
        count(*) filter (where service_date >= current_date - 90)  as claims_last_90d,
        sum(total_charge) filter (where service_date >= current_date - 90)  as paid_amount_last_90d,
        max(service_date)                                   as last_claim_dos,
        max(rendering_provider_npi)                         as pcp_provider_id,
        count(*) filter (where dollar_band = 'high_dollar') as high_dollar_claims_lifetime
    from claims
    group by 1
)

select
    member_id,
    {{ hipaa_mask('member_id') }}            as full_name_masked,
    'unknown'                                as age_band,    -- TODO: join Synthea patients seed
    'XX'                                     as state,       -- TODO: join Synthea patients seed
    'commercial'                             as plan_type,
    'active'                                 as eligibility_status,
    pcp_provider_id,
    coalesce(claims_last_90d, 0)             as claims_last_90d,
    coalesce(paid_amount_last_90d, 0)        as paid_amount_last_90d,
    last_claim_dos,
    case
        when high_dollar_claims_lifetime >= 3 then 'high'
        when claims_last_90d >= 6              then 'rising'
        else                                        'low'
    end                                      as risk_band,
    current_timestamp                        as refreshed_at
from cohort
