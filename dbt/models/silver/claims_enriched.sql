-- Silver — claims with derived fields used by every downstream mart.
-- HIPAA-relevant fields stay raw at silver (downstream marts apply the mask
-- via `{{ hipaa_mask() }}` macro).

with claims as (
    select * from {{ ref('stg_claims_837') }}
)

select
    claim_id,
    subscriber_id,
    receiver_id              as payer_id,
    service_date,
    date_trunc('month', service_date) as service_month,
    total_charge,
    rendering_provider_npi,
    primary_diagnosis_code,
    place_of_service,
    ingest_ts,
    -- Decision-grade columns
    case
        when total_charge >= 10000 then 'high_dollar'
        when total_charge >=  1000 then 'medium_dollar'
        else                              'standard'
    end as dollar_band,
    case
        when place_of_service in ('21','23') then 'inpatient_or_er'
        when place_of_service =  '22'        then 'outpatient'
        when place_of_service in ('11','22') then 'office_outpatient'
        else 'other'
    end as setting
from claims
